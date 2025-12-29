package gocql

import (
	"context"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
)

type ExecutableQuery interface {
	borrowForExecution()    // Used to ensure that the query stays alive for lifetime of a particular execution goroutine.
	releaseAfterExecution() // Used when a goroutine finishes its execution attempts, either with ok result or an error.
	execute(ctx context.Context, conn *Conn) *Iter
	attempt(keyspace string, end, start time.Time, iter *Iter, host *HostInfo)
	retryPolicy() RetryPolicy
	speculativeExecutionPolicy() SpeculativeExecutionPolicy
	GetRoutingKey() ([]byte, error)
	GetRoutingKeyYb() ([]byte, error)
	Keyspace() string
	KeyspaceAndTableYb() (string, string)
	Table() string
	IsIdempotent() bool

	withContext(context.Context) ExecutableQuery

	RetryableQuery
}

type queryExecutor struct {
	pool   *policyConnPool
	policy HostSelectionPolicy
}

func (q *queryExecutor) attemptQuery(ctx context.Context, qry ExecutableQuery, conn *Conn) *Iter {
	start := time.Now()
	iter := qry.execute(ctx, conn)
	end := time.Now()

	qry.attempt(q.pool.keyspace, end, start, iter, conn.host)

	return iter
}

func (q *queryExecutor) speculate(ctx context.Context, qry ExecutableQuery, sp SpeculativeExecutionPolicy,
	hostIter NextHost, results chan *Iter) *Iter {
	ticker := time.NewTicker(sp.Delay())
	defer ticker.Stop()

	for i := 0; i < sp.Attempts(); i++ {
		select {
		case <-ticker.C:
			qry.borrowForExecution() // ensure liveness in case of executing Query to prevent races with Query.Release().
			go q.run(ctx, qry, hostIter, results)
		case <-ctx.Done():
			return &Iter{err: ctx.Err()}
		case iter := <-results:
			return iter
		}
	}

	return nil
}

func (q *queryExecutor) executeQuery(qry ExecutableQuery) (*Iter, error) {
	hostIter := q.policy.Pick(qry)

	// check if the query is not marked as idempotent, if
	// it is, we force the policy to NonSpeculative
	sp := qry.speculativeExecutionPolicy()
	if !qry.IsIdempotent() || sp.Attempts() == 0 {
		log.Trace().Msgf("Calling do() method")
		return q.do(qry.Context(), qry, hostIter), nil
	}

	// When speculative execution is enabled, we could be accessing the host iterator from multiple goroutines below.
	// To ensure we don't call it concurrently, we wrap the returned NextHost function here to synchronize access to it.
	var mu sync.Mutex
	origHostIter := hostIter
	hostIter = func() SelectedHost {
		mu.Lock()
		defer mu.Unlock()
		return origHostIter()
	}

	ctx, cancel := context.WithCancel(qry.Context())
	defer cancel()

	results := make(chan *Iter, 1)

	// Launch the main execution
	qry.borrowForExecution() // ensure liveness in case of executing Query to prevent races with Query.Release().
	go q.run(ctx, qry, hostIter, results)

	// The speculative executions are launched _in addition_ to the main
	// execution, on a timer. So Speculation{2} would make 3 executions running
	// in total.
	if iter := q.speculate(ctx, qry, sp, hostIter, results); iter != nil {
		return iter, nil
	}

	select {
	case iter := <-results:
		return iter, nil
	case <-ctx.Done():
		return &Iter{err: ctx.Err()}, nil
	}
}

func (q *queryExecutor) do(ctx context.Context, qry ExecutableQuery, hostIter NextHost) *Iter {
	log.Debug().Msgf("Iterating on a host-list for query %s...", qry)
	selectedHost := hostIter()
	rt := qry.retryPolicy()

	var lastErr error
	var iter *Iter
	for selectedHost != nil {
		host := selectedHost.Info()
		if host == nil || !host.IsUp() {
			selectedHost = hostIter()
                        if host == nil {
                                log.Debug().Msg("Found a nil host, checking the next host")
                        } else {
                                log.Debug().Msgf("Host %s is not up, checking the next host", host.connectAddress)
                        }
			continue
		}

		pool, ok := q.pool.getPool(host)
		if !ok {
			selectedHost = hostIter()
			log.Debug().Msgf("Did not get a pool for host %s, checking the next host", host.connectAddress)
			continue
		}

		conn := pool.Pick()
		if conn == nil {
			selectedHost = hostIter()
			log.Debug().Msgf("Did not get a conn from pool of host %s, checking the next host", host.connectAddress)
			continue
		}

		iter = q.attemptQuery(ctx, qry, conn)
		iter.host = selectedHost.Info()
		// Update host
		switch iter.err {
		case context.Canceled, context.DeadlineExceeded, ErrNotFound:
			// those errors represents logical errors, they should not count
			// toward removing a node from the pool
			selectedHost.Mark(nil)
			return iter
		default:
                        log.Debug().Msgf("Marking host %s as dead due to %v", selectedHost.Info().connectAddress, iter.err)
			selectedHost.Mark(iter.err)
		}

		// Exit if the query was successful
		// or no retry policy defined or retry attempts were reached
		if iter.err == nil || rt == nil || !rt.Attempt(qry) {
			return iter
		}
		lastErr = iter.err

		// If query is unsuccessful, check the error with RetryPolicy to retry
		switch rt.GetRetryType(iter.err) {
		case Retry:
			// retry on the same host
                        log.Debug().Msgf("Retrying the query on the same host %s", host.connectAddress)
			continue
		case Rethrow, Ignore:
			return iter
		case RetryNextHost:
			// retry on the next host
                        log.Debug().Msgf("Retrying the query on the next host")
			selectedHost = hostIter()
			continue
		default:
			// Undefined? Return nil and error, this will panic in the requester
			return &Iter{err: ErrUnknownRetryType}
		}
	}

	if lastErr != nil {
		return &Iter{err: lastErr}
	}

	return &Iter{err: ErrNoConnections}
}

func (q *queryExecutor) run(ctx context.Context, qry ExecutableQuery, hostIter NextHost, results chan<- *Iter) {
	select {
	case results <- q.do(ctx, qry, hostIter):
	case <-ctx.Done():
	}
	qry.releaseAfterExecution()
}
