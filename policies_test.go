// Copyright (c) 2015 The gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package gocql

import (
	"errors"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/hailocab/go-hostpool"
	inf "gopkg.in/inf.v0"
)

func OnPage(link string, t *testing.T) string {
	res, err := http.Get(link)
	if err != nil {
		t.Fatal(err)
	}
	content, err := ioutil.ReadAll(res.Body)
	res.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	return string(content)
}

func localReadAndWrite(s string, t *testing.T) (int, int) {

	value := strings.Index(s, "handler_latency_yb_client_read_local")
	line := s[value+70:]
	l := strings.Split(line, ",")
	read, err := strconv.Atoi(l[0])
	if err != nil {
		t.Fatal(err)
	}

	value1 := strings.Index(s, "handler_latency_yb_client_write_local")
	line1 := s[value1+71:]
	l1 := strings.Split(line1, ",")
	write, err := strconv.Atoi(l1[0])
	if err != nil {
		t.Fatal(err)
	}
	return read, write
}

//Test of PartitionAwarePolicy, to see if it routes query to the correct host

// With PartitionAwarePolicy, all calls should be local ideally but there is no
// 100% guarantee
// because as soon as the test table has been created and the partition metadata
// has been
// loaded, the cluster's load-balancer may still be rebalancing the leaders.
// It may happen that the Test Fail sometimes do to above reason but should pass Majority of the times
func TestHostRouting(t *testing.T) {
	//change the ip address according to the cluster
	cluster := NewCluster("127.0.0.1")
	cluster.PoolConfig.HostSelectionPolicy = YBPartitionAwareHostPolicy(RoundRobinHostPolicy())
	cluster.Timeout = 5 * time.Second

	session, err := cluster.CreateSession()
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()

	hosts, _, err := session.hostSource.GetHosts()
	if err != nil {
		t.Fatal(err)
	}
	var (
		Beforeread  int = 0
		Beforewrite int = 0
		Totalread   int = 0
		Totalwrite  int = 0
	)
	for i := 0; i < len(hosts); i++ {
		s := "http://" + hosts[i].connectAddress.String() + ":9000/metrics"
		content := OnPage(s, t)
		read, write := localReadAndWrite(content, t)
		Beforeread += read
		Beforewrite += write
	}

	NUM_KEYS := 100

	//create keyspace
	var createStmtk = "create keyspace IF NOT EXISTS example"
	if err := session.Query(createStmtk).Exec(); err != nil {
		t.Fatal(err)
	}

	//Datatype Text, Composite Partition Key
	// Create test table.
	var createStmt1 = "create table IF NOT EXISTS example.testtext2 (h1 text, h2 text, c int, primary key ((h1,h2)));"
	if err := session.Query(createStmt1).Exec(); err != nil {
		t.Fatal(err)
	}

	//Datatype: float and double, Composite Partition Key
	// Create test table.
	var createStmt = "create table IF NOT EXISTS example.testfloat2 (h1 float, h2 double, c int, primary key ((h1, h2)));"
	if err := session.Query(createStmt).Exec(); err != nil {
		t.Fatal(err)
	}

	for i := 1; i <= NUM_KEYS; i++ {
		err := session.Query("insert into example.testtext2 (h1, h2, c) values (?, ?, ?)", strconv.Itoa(i), strconv.Itoa(i), i).Exec()
		if err != nil {
			t.Fatal(err)
		}
	}

	for i := 1; i <= NUM_KEYS; i++ {
		err := session.Query("update example.testtext2 set c = ? where h2 = ? and h1 = ?", i*2, strconv.Itoa(i), strconv.Itoa(i)).Exec()
		if err != nil {
			t.Fatal(err)
		}
	}

	for i := 1; i <= NUM_KEYS; i++ {
		stmt := session.Query(`select c from example.testtext2 where h2 = ? and h1 = ?`, strconv.Itoa(i), strconv.Itoa(i)).Iter()
		if stmt == nil {
			t.Fatal(stmt)
		}
		var c int
		stmt.Scan(&c)
		assertEqual(t, "c value", 2*i, c)
	}

	for i := 1; i <= NUM_KEYS; i++ {
		err := session.Query(" delete from example.testtext2 where h2 = ? and h1 = ?", strconv.Itoa(i), strconv.Itoa(i)).Exec()
		if err != nil {
			t.Fatal(err)
		}
	}

	for i := 1; i <= NUM_KEYS; i++ {
		err := session.Query("insert into example.testfloat2 (h1, h2, c) values (?, ?, ?)", float32(i), float64(i)+1.070701, i).Exec()
		if err != nil {
			t.Fatal(err)
		}
	}

	for i := 1; i <= NUM_KEYS; i++ {
		err := session.Query("update example.testfloat2 set c = ? where h1 = ? and h2 = ?", i*2, float32(i), float64(i)+1.070701).Exec()
		if err != nil {
			t.Fatal(err)
		}
	}

	for i := 1; i <= NUM_KEYS; i++ {
		stmt := session.Query(`select c from example.testfloat2 where h1 = ? and h2 = ?`, float32(i), float64(i)+1.070701).Iter()
		if stmt == nil {
			t.Fatal(stmt)
		}
		var c int
		stmt.Scan(&c)
		assertEqual(t, "c value", 2*i, c)
	}

	for i := 1; i <= NUM_KEYS; i++ {
		err := session.Query(" delete from example.testfloat2 where h1 = ? and h2 = ?", float32(i), float64(i)+1.070701).Exec()
		if err != nil {
			t.Fatal(err)
		}
	}

	for i := 0; i < len(hosts); i++ {
		s := "http://" + hosts[i].connectAddress.String() + ":9000/metrics"
		content := OnPage(s, t)
		read, write := localReadAndWrite(content, t)
		Totalread += read
		Totalwrite += write
	}

	Totalread -= Beforeread
	Totalwrite -= Beforewrite

	if Totalread < (NUM_KEYS*7*2)/10 && Totalwrite < (NUM_KEYS*6*7)/10 {
		t.Fatalf("Less number of local read or write are happening, local read = %d, local write = %d ", Totalread, Totalwrite)
	}

}

func TestHostRoutingIndex(t *testing.T) {
	//change the ip address according to the cluster
	cluster := NewCluster("127.0.0.1")
	cluster.PoolConfig.HostSelectionPolicy = YBPartitionAwareHostPolicy(RoundRobinHostPolicy())
	cluster.Timeout = 5 * time.Second

	session, err := cluster.CreateSession()
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()

	hosts, _, err := session.hostSource.GetHosts()
	if err != nil {
		t.Fatal(err)
	}
	var (
		Beforeread  int = 0
		Beforewrite int = 0
		Totalread   int = 0
		Totalwrite  int = 0
	)
	for i := 0; i < len(hosts); i++ {
		s := "http://" + hosts[i].connectAddress.String() + ":9000/metrics"
		content := OnPage(s, t)
		read, write := localReadAndWrite(content, t)
		Beforeread += read
		Beforewrite += write
	}

	NUM_KEYS := 100

	//create keyspace
	var createStmtk = "create keyspace IF NOT EXISTS example"
	if err := session.Query(createStmtk).Exec(); err != nil {
		t.Fatal(err)
	}

	// Create test table.
	var createStmt1 = "create table IF NOT EXISTS example.test_lb_idx (h1 int, h2 text, c int,j jsonb, primary key ((h1,h2))) with transactions = { 'enabled' : true };"
	if err := session.Query(createStmt1).Exec(); err != nil {
		t.Fatal(err)
	}

	var createindex1 = "create index test_lb_idx_1 on example.test_lb_idx (h1) include (c);"
	if err := session.Query(createindex1).Exec(); err != nil {
		t.Fatal(err)
	}

	var createindex2 = "create index test_lb_idx_2 on example.test_lb_idx (c);"
	if err := session.Query(createindex2).Exec(); err != nil {
		t.Fatal(err)
	}

	var createindex3 = "create index test_lb_idx_3 on example.test_lb_idx (j->>'a');"
	if err := session.Query(createindex3).Exec(); err != nil {
		t.Fatal(err)
	}

	for i := 1; i <= NUM_KEYS; i++ {
		err := session.Query("insert into example.test_lb_idx (h1, h2, c, j) values (?, ?, ?, ?)", i, strconv.Itoa(i), i, strconv.Itoa(i)).Exec()
		if err != nil {
			t.Fatal(err)
		}
	}

	for i := 1; i <= NUM_KEYS; i++ {
		stmt := session.Query(`select c from example.test_lb_idx where h1 = ?;`, i).Iter()
		if stmt == nil {
			t.Fatal(stmt)
		}
		var c int
		stmt.Scan(&c)
		assertEqual(t, "c value", i, c)
	}

	for i := 1; i <= NUM_KEYS; i++ {
		stmt := session.Query(`select h1, h2 from example.test_lb_idx where c = ?;`, i).Iter()
		if stmt == nil {
			t.Fatal(stmt)
		}
	}

	for i := 1; i <= NUM_KEYS; i++ {
		stmt := session.Query(`select c from example.test_lb_idx where j = ?;`, strconv.Itoa(i)).Iter()
		if stmt == nil {
			t.Fatal(stmt)
		}
		var c int
		stmt.Scan(&c)
		assertEqual(t, "c value", i, c)
	}

	for i := 0; i < len(hosts); i++ {
		s := "http://" + hosts[i].connectAddress.String() + ":9000/metrics"
		content := OnPage(s, t)
		read, write := localReadAndWrite(content, t)
		Totalread += read
		Totalwrite += write
	}

	Totalread -= Beforeread
	Totalwrite -= Beforewrite

	if Totalread < (NUM_KEYS*7*3)/10 && Totalwrite < (NUM_KEYS*7)/10 {
		t.Fatalf("Less number of local read or write are happening, local read = %d, local write = %d ", Totalread, Totalwrite)
	}
}

func TestHostRoutingBatch(t *testing.T) {
	//change the ip address according to the cluster
	cluster := NewCluster("127.0.0.1")
	cluster.PoolConfig.HostSelectionPolicy = YBPartitionAwareHostPolicy(RoundRobinHostPolicy())
	cluster.Timeout = 5 * time.Second

	session, err := cluster.CreateSession()
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()

	hosts, _, err := session.hostSource.GetHosts()
	if err != nil {
		t.Fatal(err)
	}
	var (
		Beforeread  int = 0
		Beforewrite int = 0
		Totalread   int = 0
		Totalwrite  int = 0
	)
	for i := 0; i < len(hosts); i++ {
		s := "http://" + hosts[i].connectAddress.String() + ":9000/metrics"
		content := OnPage(s, t)
		read, write := localReadAndWrite(content, t)
		Beforeread += read
		Beforewrite += write
	}

	NUM_KEYS := 100

	//create keyspace
	var createStmtk = "create keyspace IF NOT EXISTS example"
	if err := session.Query(createStmtk).Exec(); err != nil {
		t.Fatal(err)
	}

	// Create test table.
	var createStmt1 = "create table example.test_lb (h int, r text, c int, primary key ((h), r));"
	if err := session.Query(createStmt1).Exec(); err != nil {
		t.Fatal(err)
	}

	stmt := "insert into example.test_lb (h, r, c) values (?, ?, ?);"

	for i := 1; i <= NUM_KEYS; i++ {
		batch := session.NewBatch(LoggedBatch)
		for j := 1; j <= 5; j++ {
			batch.Query(stmt, i, strconv.Itoa(j), i)
		}
		err := session.ExecuteBatch(batch)
		if err != nil {
			t.Fatal(err)
		}
	}

	for i := 1; i <= NUM_KEYS; i++ {
		stmt := session.Query(`select c from example.test_lb where h = ? and r = '1';`, i).Iter()
		if stmt == nil {
			t.Fatal(stmt)
		}
		var c int
		stmt.Scan(&c)
		assertEqual(t, "c value", i, c)
	}

	for i := 0; i < len(hosts); i++ {
		s := "http://" + hosts[i].connectAddress.String() + ":9000/metrics"
		content := OnPage(s, t)
		read, write := localReadAndWrite(content, t)
		Totalread += read
		Totalwrite += write
	}

	Totalread -= Beforeread
	Totalwrite -= Beforewrite

	if Totalread < (NUM_KEYS*7)/10 && Totalwrite < (NUM_KEYS*7)/10 {
		t.Fatalf("Less number of local read or write are happening, local read = %d, local write = %d ", Totalread, Totalwrite)
	}
}

// The cluster should be created with the following tags:
//--tserver_flags="cql_nodelist_refresh_interval_secs=8" --master_flags="tserver_unresponsive_timeout_ms=10000"
func TestCreateDropTable(t *testing.T) {
	//change the ip address according to the cluster
	cluster := NewCluster("127.0.0.1")
	cluster.PoolConfig.HostSelectionPolicy = YBPartitionAwareHostPolicy(RoundRobinHostPolicy())
	cluster.Timeout = 5 * time.Second

	session, err := cluster.CreateSession()
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()

	MAX_WAIT_SECONDS := 10

	//Create Keyspace
	var createStmtk = "create keyspace IF NOT EXISTS example"
	if err := session.Query(createStmtk).Exec(); err != nil {
		t.Fatal(err)
	}

	// Create test table. Verify that the PartitionMetadata gets notified of the table creation
	// and loads the metadata.
	var createStmt1 = "create table example.test_partition1 (k int primary key);"
	if err := session.Query(createStmt1).Exec(); err != nil {
		t.Fatal(err)
	}

	found := false

	for i := 0; i < MAX_WAIT_SECONDS; i++ {
		partitionMap := getTableSplitMetadata("example", "test_partition1")
		if len(partitionMap.getHosts(0)) > 0 {
			found = true
			break
		}
		time.Sleep(1 * time.Second)
	}
	assertEqual(t, "", true, found)

	// Drop test table. Verify that the PartitionMetadata gets notified of the table drop
	// and clears the the metadata.
	var dropStmt1 = "Drop table example.test_partition1;"
	if err := session.Query(dropStmt1).Exec(); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < MAX_WAIT_SECONDS; i++ {
		partitionMap := getTableSplitMetadata("example", "test_partition1")
		if len(partitionMap.getHosts(0)) == 0 {
			found = false
			break
		}
		time.Sleep(1 * time.Second)
	}
	assertEqual(t, "", false, found)
}

func check(q *Query, i int64, t *testing.T) {
	key, err := q.GetRoutingKeyYb()
	if err != nil || len(key) == 0 {
		t.Fatal(err)
	}
	Key := GetKey(key)
	if Key != i {
		t.Fatalf("expected %d key value got %d", i, Key)
	}
}

func createTables(t *testing.T, s *Session) {

	createStmt := "CREATE Keyspace IF NOT EXISTS example;"
	if err := s.Query(createStmt).Exec(); err != nil {
		t.Fatal(err)
	}

	createStmt = "create table IF NOT EXISTS example.test_int_2 (h1 int, h2 text, h3 int, c int, primary key ((h1,h2),h3));"
	if err := s.Query(createStmt).Exec(); err != nil {
		t.Fatal(err)
	}

	createStmt = "create table IF NOT EXISTS example.test_decimal_2 (h1 decimal, h2 text, h3 int, c int, primary key ((h1,h2),h3));"
	if err := s.Query(createStmt).Exec(); err != nil {
		t.Fatal(err)
	}

	createStmt = "create table IF NOT EXISTS example.test_time_1 (h1 timestamp, h2 text, h3 int, c int, primary key (h1,h2,h3));"
	if err := s.Query(createStmt).Exec(); err != nil {
		t.Fatal(err)
	}

	createStmt = "create table IF NOT EXISTS example.test_date_2 (h1 date, h2 text, h3 int, c int, primary key ((h1,h2),h3));"
	if err := s.Query(createStmt).Exec(); err != nil {
		t.Fatal(err)
	}

	createStmt = "create table IF NOT EXISTS example.test_uuid_2 (h1 uuid, h2 text, h3 int, c int, primary key ((h1,h2),h3));"
	if err := s.Query(createStmt).Exec(); err != nil {
		t.Fatal(err)
	}

	createStmt = "create table IF NOT EXISTS example.test_inet_2 (h1 inet, h2 text, h3 int, c int, primary key ((h1,h2),h3));"
	if err := s.Query(createStmt).Exec(); err != nil {
		t.Fatal(err)
	}

	createStmt = "create table IF NOT EXISTS example.test_bool_2 (h1 boolean, h2 text, h3 int, c int, primary key ((h1,h2),h3));"
	if err := s.Query(createStmt).Exec(); err != nil {
		t.Fatal(err)
	}

	createStmt = "create table IF NOT EXISTS example.test_tinyint_2 (h1 tinyint, h2 text, h3 int, c int, primary key ((h1,h2),h3));"
	if err := s.Query(createStmt).Exec(); err != nil {
		t.Fatal(err)
	}

	createStmt = "create table IF NOT EXISTS example.test_smallint_2 (h1 smallint, h2 text, h3 int, c int, primary key ((h1,h2),h3));"
	if err := s.Query(createStmt).Exec(); err != nil {
		t.Fatal(err)
	}

}

//Test of Getkey function written for PartitionAwarePolicy
func TestGetKey(t *testing.T) {
	//change the ip address according to the cluster
	cluster := NewCluster("127.0.0.1")
	cluster.PoolConfig.HostSelectionPolicy = YBPartitionAwareHostPolicy(RoundRobinHostPolicy())
	cluster.Timeout = 5 * time.Second

	session, err := cluster.CreateSession()
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()

	createTables(t, session)

	//Hash value of these queries are taken from java-driver and are being matched with the hash that gocql gives for the queries.

	//int composite partition key
	qry1 := session.Query("select c from example.test_int_2 where h1 = ? and h2= ?", 100, "100")
	check(qry1, 46090, t)

	//decimal composite partition key
	qry2 := session.Query("select c from example.test_decimal_2 where h1 = ? and h2 = ?", inf.NewDec(int64(90), 0), strconv.Itoa(90))
	check(qry2, 57745, t)

	//timestamp composite partition key
	t1, err := time.Parse("2006-01-02 15:04:05.000", "2011-02-03 12:12:12.555")
	if err != nil {
		t.Fatal(err)
	}
	qry3 := session.Query("select c from example.test_time_1 where h1 = ?", t1)
	check(qry3, 37279, t)

	//date composite partition key
	t2, err := time.Parse("2006-01-02", "2011-02-03")
	if err != nil {
		t.Fatal(err)
	}
	qry4 := session.Query("select c from example.test_date_2 where h1 = ? and h2 = ?", t2, "o")
	check(qry4, 32877, t)

	//uuid composite partition key
	qry5 := session.Query("select c from example.test_uuid_2 where h1 = ? and h2 = ?", "123e4567-e89b-12d3-a456-426614174000", "o")
	check(qry5, 38835, t)

	//inet composite partition key
	qry6 := session.Query("select c from example.test_inet_2 where h1 = ? and h2 = ?", "127.0.0.1", "o")
	check(qry6, 16064, t)

	//bool composite partition key
	qry7 := session.Query("select c from example.test_bool_2 where h1 = ? and h2 = ?", false, "o")
	check(qry7, 57256, t)

	//tinyint composite partition key
	qry8 := session.Query("select c from example.test_tinyint_2 where h1 = ? and h2 = ?", 1, "o")
	check(qry8, 48500, t)

	//Smallint composite partition key
	qry9 := session.Query("select c from example.test_smallint_2 where h1 = ? and h2 = ?", 2, "o")
	check(qry9, 26643, t)

}

// Tests of the round-robin host selection policy implementation
func TestRoundRobbin(t *testing.T) {
	policy := RoundRobinHostPolicy()

	hosts := [...]*HostInfo{
		{hostId: "0", connectAddress: net.IPv4(0, 0, 0, 1)},
		{hostId: "1", connectAddress: net.IPv4(0, 0, 0, 2)},
	}

	for _, host := range hosts {
		policy.AddHost(host)
	}

	got := make(map[string]bool)
	it := policy.Pick(nil)
	for h := it(); h != nil; h = it() {
		id := h.Info().hostId
		if got[id] {
			t.Fatalf("got duplicate host: %v", id)
		}
		got[id] = true
	}
	if len(got) != len(hosts) {
		t.Fatalf("expected %d hosts got %d", len(hosts), len(got))
	}
}

// Tests of the token-aware host selection policy implementation with a
// round-robin host selection policy fallback.
func TestHostPolicy_TokenAware_SimpleStrategy(t *testing.T) {
	const keyspace = "myKeyspace"
	policy := TokenAwareHostPolicy(RoundRobinHostPolicy())
	policyInternal := policy.(*tokenAwareHostPolicy)
	policyInternal.getKeyspaceName = func() string { return keyspace }
	policyInternal.getKeyspaceMetadata = func(ks string) (*KeyspaceMetadata, error) {
		return nil, errors.New("not initalized")
	}

	query := &Query{routingInfo: &queryRoutingInfo{}}
	query.getKeyspace = func() string { return keyspace }

	iter := policy.Pick(nil)
	if iter == nil {
		t.Fatal("host iterator was nil")
	}
	actual := iter()
	if actual != nil {
		t.Fatalf("expected nil from iterator, but was %v", actual)
	}

	// set the hosts
	hosts := [...]*HostInfo{
		{hostId: "0", connectAddress: net.IPv4(10, 0, 0, 1), tokens: []string{"00"}},
		{hostId: "1", connectAddress: net.IPv4(10, 0, 0, 2), tokens: []string{"25"}},
		{hostId: "2", connectAddress: net.IPv4(10, 0, 0, 3), tokens: []string{"50"}},
		{hostId: "3", connectAddress: net.IPv4(10, 0, 0, 4), tokens: []string{"75"}},
	}
	for _, host := range &hosts {
		policy.AddHost(host)
	}

	policy.SetPartitioner("OrderedPartitioner")

	policyInternal.getKeyspaceMetadata = func(keyspaceName string) (*KeyspaceMetadata, error) {
		if keyspaceName != keyspace {
			return nil, fmt.Errorf("unknown keyspace: %s", keyspaceName)
		}
		return &KeyspaceMetadata{
			Name:          keyspace,
			StrategyClass: "SimpleStrategy",
			StrategyOptions: map[string]interface{}{
				"class":              "SimpleStrategy",
				"replication_factor": 2,
			},
		}, nil
	}
	policy.KeyspaceChanged(KeyspaceUpdateEvent{Keyspace: keyspace})

	// The SimpleStrategy above should generate the following replicas.
	// It's handy to have as reference here.
	assertDeepEqual(t, "replicas", map[string]tokenRingReplicas{
		"myKeyspace": {
			{orderedToken("00"), []*HostInfo{hosts[0], hosts[1]}},
			{orderedToken("25"), []*HostInfo{hosts[1], hosts[2]}},
			{orderedToken("50"), []*HostInfo{hosts[2], hosts[3]}},
			{orderedToken("75"), []*HostInfo{hosts[3], hosts[0]}},
		},
	}, policyInternal.getMetadataReadOnly().replicas)

	// now the token ring is configured
	query.RoutingKey([]byte("20"))
	iter = policy.Pick(query)
	// first token-aware hosts
	expectHosts(t, "hosts[0]", iter, "1")
	expectHosts(t, "hosts[1]", iter, "2")
	// then rest of the hosts
	expectHosts(t, "rest", iter, "0", "3")
	expectNoMoreHosts(t, iter)
}

// Tests of the host pool host selection policy implementation
func TestHostPolicy_HostPool(t *testing.T) {
	policy := HostPoolHostPolicy(hostpool.New(nil))

	hosts := []*HostInfo{
		{hostId: "0", connectAddress: net.IPv4(10, 0, 0, 0)},
		{hostId: "1", connectAddress: net.IPv4(10, 0, 0, 1)},
	}

	// Using set host to control the ordering of the hosts as calling "AddHost" iterates the map
	// which will result in an unpredictable ordering
	policy.(*hostPoolHostPolicy).SetHosts(hosts)

	// the first host selected is actually at [1], but this is ok for RR
	// interleaved iteration should always increment the host
	iter := policy.Pick(nil)
	actualA := iter()
	if actualA.Info().HostID() != "0" {
		t.Errorf("Expected hosts[0] but was hosts[%s]", actualA.Info().HostID())
	}
	actualA.Mark(nil)

	actualB := iter()
	if actualB.Info().HostID() != "1" {
		t.Errorf("Expected hosts[1] but was hosts[%s]", actualB.Info().HostID())
	}
	actualB.Mark(fmt.Errorf("error"))

	actualC := iter()
	if actualC.Info().HostID() != "0" {
		t.Errorf("Expected hosts[0] but was hosts[%s]", actualC.Info().HostID())
	}
	actualC.Mark(nil)

	actualD := iter()
	if actualD.Info().HostID() != "0" {
		t.Errorf("Expected hosts[0] but was hosts[%s]", actualD.Info().HostID())
	}
	actualD.Mark(nil)
}

func TestHostPolicy_RoundRobin_NilHostInfo(t *testing.T) {
	policy := RoundRobinHostPolicy()

	host := &HostInfo{hostId: "host-1"}
	policy.AddHost(host)

	iter := policy.Pick(nil)
	next := iter()
	if next == nil {
		t.Fatal("got nil host")
	} else if v := next.Info(); v == nil {
		t.Fatal("got nil HostInfo")
	} else if v.HostID() != host.HostID() {
		t.Fatalf("expected host %v got %v", host, v)
	}

	next = iter()
	if next != nil {
		t.Errorf("expected to get nil host got %+v", next)
		if next.Info() == nil {
			t.Fatalf("HostInfo is nil")
		}
	}
}

func TestHostPolicy_TokenAware_NilHostInfo(t *testing.T) {
	policy := TokenAwareHostPolicy(RoundRobinHostPolicy())
	policyInternal := policy.(*tokenAwareHostPolicy)
	policyInternal.getKeyspaceName = func() string { return "myKeyspace" }
	policyInternal.getKeyspaceMetadata = func(ks string) (*KeyspaceMetadata, error) {
		return nil, errors.New("not initialized")
	}

	hosts := [...]*HostInfo{
		{connectAddress: net.IPv4(10, 0, 0, 0), tokens: []string{"00"}},
		{connectAddress: net.IPv4(10, 0, 0, 1), tokens: []string{"25"}},
		{connectAddress: net.IPv4(10, 0, 0, 2), tokens: []string{"50"}},
		{connectAddress: net.IPv4(10, 0, 0, 3), tokens: []string{"75"}},
	}
	for _, host := range hosts {
		policy.AddHost(host)
	}
	policy.SetPartitioner("OrderedPartitioner")

	query := &Query{routingInfo: &queryRoutingInfo{}}
	query.getKeyspace = func() string { return "myKeyspace" }
	query.RoutingKey([]byte("20"))

	iter := policy.Pick(query)
	next := iter()
	if next == nil {
		t.Fatal("got nil host")
	} else if v := next.Info(); v == nil {
		t.Fatal("got nil HostInfo")
	} else if !v.ConnectAddress().Equal(hosts[1].ConnectAddress()) {
		t.Fatalf("expected peer 1 got %v", v.ConnectAddress())
	}

	// Empty the hosts to trigger the panic when using the fallback.
	for _, host := range hosts {
		policy.RemoveHost(host)
	}

	next = iter()
	if next != nil {
		t.Errorf("expected to get nil host got %+v", next)
		if next.Info() == nil {
			t.Fatalf("HostInfo is nil")
		}
	}
}

func TestCOWList_Add(t *testing.T) {
	var cow cowHostList

	toAdd := [...]net.IP{net.IPv4(10, 0, 0, 1), net.IPv4(10, 0, 0, 2), net.IPv4(10, 0, 0, 3)}

	for _, addr := range toAdd {
		if !cow.add(&HostInfo{connectAddress: addr}) {
			t.Fatal("did not add peer which was not in the set")
		}
	}

	hosts := cow.get()
	if len(hosts) != len(toAdd) {
		t.Fatalf("expected to have %d hosts got %d", len(toAdd), len(hosts))
	}

	set := make(map[string]bool)
	for _, host := range hosts {
		set[string(host.ConnectAddress())] = true
	}

	for _, addr := range toAdd {
		if !set[string(addr)] {
			t.Errorf("addr was not in the host list: %q", addr)
		}
	}
}

// TestSimpleRetryPolicy makes sure that we only allow 1 + numRetries attempts
func TestSimpleRetryPolicy(t *testing.T) {
	q := &Query{routingInfo: &queryRoutingInfo{}}

	// this should allow a total of 3 tries.
	rt := &SimpleRetryPolicy{NumRetries: 2}

	cases := []struct {
		attempts int
		allow    bool
	}{
		{0, true},
		{1, true},
		{2, true},
		{3, false},
		{4, false},
		{5, false},
	}

	for _, c := range cases {
		q.metrics = preFilledQueryMetrics(map[string]*hostMetrics{"127.0.0.1": {Attempts: c.attempts}})
		if c.allow && !rt.Attempt(q) {
			t.Fatalf("should allow retry after %d attempts", c.attempts)
		}
		if !c.allow && rt.Attempt(q) {
			t.Fatalf("should not allow retry after %d attempts", c.attempts)
		}
	}
}

func TestExponentialBackoffPolicy(t *testing.T) {
	// test with defaults
	sut := &ExponentialBackoffRetryPolicy{NumRetries: 2}

	cases := []struct {
		attempts int
		delay    time.Duration
	}{

		{1, 100 * time.Millisecond},
		{2, (2) * 100 * time.Millisecond},
		{3, (2 * 2) * 100 * time.Millisecond},
		{4, (2 * 2 * 2) * 100 * time.Millisecond},
	}
	for _, c := range cases {
		// test 100 times for each case
		for i := 0; i < 100; i++ {
			d := sut.napTime(c.attempts)
			if d < c.delay-(100*time.Millisecond)/2 {
				t.Fatalf("Delay %d less than jitter min of %d", d, c.delay-100*time.Millisecond/2)
			}
			if d > c.delay+(100*time.Millisecond)/2 {
				t.Fatalf("Delay %d greater than jitter max of %d", d, c.delay+100*time.Millisecond/2)
			}
		}
	}
}

func TestDowngradingConsistencyRetryPolicy(t *testing.T) {

	q := &Query{cons: LocalQuorum, routingInfo: &queryRoutingInfo{}}

	rewt0 := &RequestErrWriteTimeout{
		Received:  0,
		WriteType: "SIMPLE",
	}

	rewt1 := &RequestErrWriteTimeout{
		Received:  1,
		WriteType: "BATCH",
	}

	rewt2 := &RequestErrWriteTimeout{
		WriteType: "UNLOGGED_BATCH",
	}

	rert := &RequestErrReadTimeout{}

	reu0 := &RequestErrUnavailable{
		Alive: 0,
	}

	reu1 := &RequestErrUnavailable{
		Alive: 1,
	}

	// this should allow a total of 3 tries.
	consistencyLevels := []Consistency{Three, Two, One}
	rt := &DowngradingConsistencyRetryPolicy{ConsistencyLevelsToTry: consistencyLevels}
	cases := []struct {
		attempts  int
		allow     bool
		err       error
		retryType RetryType
	}{
		{0, true, rewt0, Rethrow},
		{3, true, rewt1, Ignore},
		{1, true, rewt2, Retry},
		{2, true, rert, Retry},
		{4, false, reu0, Rethrow},
		{16, false, reu1, Retry},
	}

	for _, c := range cases {
		q.metrics = preFilledQueryMetrics(map[string]*hostMetrics{"127.0.0.1": {Attempts: c.attempts}})
		if c.retryType != rt.GetRetryType(c.err) {
			t.Fatalf("retry type should be %v", c.retryType)
		}
		if c.allow && !rt.Attempt(q) {
			t.Fatalf("should allow retry after %d attempts", c.attempts)
		}
		if !c.allow && rt.Attempt(q) {
			t.Fatalf("should not allow retry after %d attempts", c.attempts)
		}
	}
}

// expectHosts makes sure that the next len(hostIDs) returned from iter is a permutation of hostIDs.
func expectHosts(t *testing.T, msg string, iter NextHost, hostIDs ...string) {
	t.Helper()

	expectedHostIDs := make(map[string]struct{}, len(hostIDs))
	for i := range hostIDs {
		expectedHostIDs[hostIDs[i]] = struct{}{}
	}

	expectedStr := func() string {
		keys := make([]string, 0, len(expectedHostIDs))
		for k := range expectedHostIDs {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		return strings.Join(keys, ", ")
	}

	for len(expectedHostIDs) > 0 {
		host := iter()
		if host == nil || host.Info() == nil {
			t.Fatalf("%s: expected hostID one of {%s}, but got nil", msg, expectedStr())
		}
		hostID := host.Info().HostID()
		if _, ok := expectedHostIDs[hostID]; !ok {
			t.Fatalf("%s: expected host ID one of {%s}, but got %s", msg, expectedStr(), hostID)
		}
		delete(expectedHostIDs, hostID)
	}
}

func expectNoMoreHosts(t *testing.T, iter NextHost) {
	t.Helper()
	host := iter()
	if host == nil {
		// success
		return
	}
	info := host.Info()
	if info == nil {
		t.Fatalf("expected no more hosts, but got host with nil Info()")
		return
	}
	t.Fatalf("expected no more hosts, but got %s", info.HostID())
}

func TestHostPolicy_DCAwareRR(t *testing.T) {
	p := DCAwareRoundRobinPolicy("local")

	hosts := [...]*HostInfo{
		{hostId: "0", connectAddress: net.ParseIP("10.0.0.1"), dataCenter: "local"},
		{hostId: "1", connectAddress: net.ParseIP("10.0.0.2"), dataCenter: "local"},
		{hostId: "2", connectAddress: net.ParseIP("10.0.0.3"), dataCenter: "remote"},
		{hostId: "3", connectAddress: net.ParseIP("10.0.0.4"), dataCenter: "remote"},
	}

	for _, host := range hosts {
		p.AddHost(host)
	}

	got := make(map[string]bool, len(hosts))
	var dcs []string

	it := p.Pick(nil)
	for h := it(); h != nil; h = it() {
		id := h.Info().hostId
		dc := h.Info().dataCenter

		if got[id] {
			t.Fatalf("got duplicate host %s", id)
		}
		got[id] = true
		dcs = append(dcs, dc)
	}

	if len(got) != len(hosts) {
		t.Fatalf("expected %d hosts got %d", len(hosts), len(got))
	}

	var remote bool
	for _, dc := range dcs {
		if dc == "local" {
			if remote {
				t.Fatalf("got local dc after remote: %v", dcs)
			}
		} else {
			remote = true
		}
	}

}

// Tests of the token-aware host selection policy implementation with a
// DC aware round-robin host selection policy fallback
// with {"class": "NetworkTopologyStrategy", "a": 1, "b": 1, "c": 1} replication.
func TestHostPolicy_TokenAware(t *testing.T) {
	const keyspace = "myKeyspace"
	policy := TokenAwareHostPolicy(DCAwareRoundRobinPolicy("local"))
	policyInternal := policy.(*tokenAwareHostPolicy)
	policyInternal.getKeyspaceName = func() string { return keyspace }
	policyInternal.getKeyspaceMetadata = func(ks string) (*KeyspaceMetadata, error) {
		return nil, errors.New("not initialized")
	}

	query := &Query{routingInfo: &queryRoutingInfo{}}
	query.getKeyspace = func() string { return keyspace }

	iter := policy.Pick(nil)
	if iter == nil {
		t.Fatal("host iterator was nil")
	}
	actual := iter()
	if actual != nil {
		t.Fatalf("expected nil from iterator, but was %v", actual)
	}

	// set the hosts
	hosts := [...]*HostInfo{
		{hostId: "0", connectAddress: net.IPv4(10, 0, 0, 1), tokens: []string{"05"}, dataCenter: "remote1"},
		{hostId: "1", connectAddress: net.IPv4(10, 0, 0, 2), tokens: []string{"10"}, dataCenter: "local"},
		{hostId: "2", connectAddress: net.IPv4(10, 0, 0, 3), tokens: []string{"15"}, dataCenter: "remote2"},
		{hostId: "3", connectAddress: net.IPv4(10, 0, 0, 4), tokens: []string{"20"}, dataCenter: "remote1"},
		{hostId: "4", connectAddress: net.IPv4(10, 0, 0, 5), tokens: []string{"25"}, dataCenter: "local"},
		{hostId: "5", connectAddress: net.IPv4(10, 0, 0, 6), tokens: []string{"30"}, dataCenter: "remote2"},
		{hostId: "6", connectAddress: net.IPv4(10, 0, 0, 7), tokens: []string{"35"}, dataCenter: "remote1"},
		{hostId: "7", connectAddress: net.IPv4(10, 0, 0, 8), tokens: []string{"40"}, dataCenter: "local"},
		{hostId: "8", connectAddress: net.IPv4(10, 0, 0, 9), tokens: []string{"45"}, dataCenter: "remote2"},
		{hostId: "9", connectAddress: net.IPv4(10, 0, 0, 10), tokens: []string{"50"}, dataCenter: "remote1"},
		{hostId: "10", connectAddress: net.IPv4(10, 0, 0, 11), tokens: []string{"55"}, dataCenter: "local"},
		{hostId: "11", connectAddress: net.IPv4(10, 0, 0, 12), tokens: []string{"60"}, dataCenter: "remote2"},
	}
	for _, host := range hosts {
		policy.AddHost(host)
	}

	// the token ring is not setup without the partitioner, but the fallback
	// should work
	if actual := policy.Pick(nil)(); actual == nil {
		t.Fatal("expected to get host from fallback got nil")
	}

	query.RoutingKey([]byte("30"))
	if actual := policy.Pick(query)(); actual == nil {
		t.Fatal("expected to get host from fallback got nil")
	}

	policy.SetPartitioner("OrderedPartitioner")

	policyInternal.getKeyspaceMetadata = func(keyspaceName string) (*KeyspaceMetadata, error) {
		if keyspaceName != keyspace {
			return nil, fmt.Errorf("unknown keyspace: %s", keyspaceName)
		}
		return &KeyspaceMetadata{
			Name:          keyspace,
			StrategyClass: "NetworkTopologyStrategy",
			StrategyOptions: map[string]interface{}{
				"class":   "NetworkTopologyStrategy",
				"local":   1,
				"remote1": 1,
				"remote2": 1,
			},
		}, nil
	}
	policy.KeyspaceChanged(KeyspaceUpdateEvent{Keyspace: "myKeyspace"})

	// The NetworkTopologyStrategy above should generate the following replicas.
	// It's handy to have as reference here.
	assertDeepEqual(t, "replicas", map[string]tokenRingReplicas{
		"myKeyspace": {
			{orderedToken("05"), []*HostInfo{hosts[0], hosts[1], hosts[2]}},
			{orderedToken("10"), []*HostInfo{hosts[1], hosts[2], hosts[3]}},
			{orderedToken("15"), []*HostInfo{hosts[2], hosts[3], hosts[4]}},
			{orderedToken("20"), []*HostInfo{hosts[3], hosts[4], hosts[5]}},
			{orderedToken("25"), []*HostInfo{hosts[4], hosts[5], hosts[6]}},
			{orderedToken("30"), []*HostInfo{hosts[5], hosts[6], hosts[7]}},
			{orderedToken("35"), []*HostInfo{hosts[6], hosts[7], hosts[8]}},
			{orderedToken("40"), []*HostInfo{hosts[7], hosts[8], hosts[9]}},
			{orderedToken("45"), []*HostInfo{hosts[8], hosts[9], hosts[10]}},
			{orderedToken("50"), []*HostInfo{hosts[9], hosts[10], hosts[11]}},
			{orderedToken("55"), []*HostInfo{hosts[10], hosts[11], hosts[0]}},
			{orderedToken("60"), []*HostInfo{hosts[11], hosts[0], hosts[1]}},
		},
	}, policyInternal.getMetadataReadOnly().replicas)

	// now the token ring is configured
	query.RoutingKey([]byte("23"))
	iter = policy.Pick(query)
	// first should be host with matching token from the local DC
	expectHosts(t, "matching token from local DC", iter, "4")
	// next are in non-deterministic order
	expectHosts(t, "rest", iter, "0", "1", "2", "3", "5", "6", "7", "8", "9", "10", "11")
	expectNoMoreHosts(t, iter)
}

// Tests of the token-aware host selection policy implementation with a
// DC aware round-robin host selection policy fallback
// with {"class": "NetworkTopologyStrategy", "a": 2, "b": 2, "c": 2} replication.
func TestHostPolicy_TokenAware_NetworkStrategy(t *testing.T) {
	const keyspace = "myKeyspace"
	policy := TokenAwareHostPolicy(DCAwareRoundRobinPolicy("local"), NonLocalReplicasFallback())
	policyInternal := policy.(*tokenAwareHostPolicy)
	policyInternal.getKeyspaceName = func() string { return keyspace }
	policyInternal.getKeyspaceMetadata = func(ks string) (*KeyspaceMetadata, error) {
		return nil, errors.New("not initialized")
	}

	query := &Query{routingInfo: &queryRoutingInfo{}}
	query.getKeyspace = func() string { return keyspace }

	iter := policy.Pick(nil)
	if iter == nil {
		t.Fatal("host iterator was nil")
	}
	actual := iter()
	if actual != nil {
		t.Fatalf("expected nil from iterator, but was %v", actual)
	}

	// set the hosts
	hosts := [...]*HostInfo{
		{hostId: "0", connectAddress: net.IPv4(10, 0, 0, 1), tokens: []string{"05"}, dataCenter: "remote1"},
		{hostId: "1", connectAddress: net.IPv4(10, 0, 0, 2), tokens: []string{"10"}, dataCenter: "local"},
		{hostId: "2", connectAddress: net.IPv4(10, 0, 0, 3), tokens: []string{"15"}, dataCenter: "remote2"},
		{hostId: "3", connectAddress: net.IPv4(10, 0, 0, 4), tokens: []string{"20"}, dataCenter: "remote1"}, // 1
		{hostId: "4", connectAddress: net.IPv4(10, 0, 0, 5), tokens: []string{"25"}, dataCenter: "local"},   // 2
		{hostId: "5", connectAddress: net.IPv4(10, 0, 0, 6), tokens: []string{"30"}, dataCenter: "remote2"}, // 3
		{hostId: "6", connectAddress: net.IPv4(10, 0, 0, 7), tokens: []string{"35"}, dataCenter: "remote1"}, // 4
		{hostId: "7", connectAddress: net.IPv4(10, 0, 0, 8), tokens: []string{"40"}, dataCenter: "local"},   // 5
		{hostId: "8", connectAddress: net.IPv4(10, 0, 0, 9), tokens: []string{"45"}, dataCenter: "remote2"}, // 6
		{hostId: "9", connectAddress: net.IPv4(10, 0, 0, 10), tokens: []string{"50"}, dataCenter: "remote1"},
		{hostId: "10", connectAddress: net.IPv4(10, 0, 0, 11), tokens: []string{"55"}, dataCenter: "local"},
		{hostId: "11", connectAddress: net.IPv4(10, 0, 0, 12), tokens: []string{"60"}, dataCenter: "remote2"},
	}
	for _, host := range hosts {
		policy.AddHost(host)
	}

	policy.SetPartitioner("OrderedPartitioner")

	policyInternal.getKeyspaceMetadata = func(keyspaceName string) (*KeyspaceMetadata, error) {
		if keyspaceName != keyspace {
			return nil, fmt.Errorf("unknown keyspace: %s", keyspaceName)
		}
		return &KeyspaceMetadata{
			Name:          keyspace,
			StrategyClass: "NetworkTopologyStrategy",
			StrategyOptions: map[string]interface{}{
				"class":   "NetworkTopologyStrategy",
				"local":   2,
				"remote1": 2,
				"remote2": 2,
			},
		}, nil
	}
	policy.KeyspaceChanged(KeyspaceUpdateEvent{Keyspace: keyspace})

	// The NetworkTopologyStrategy above should generate the following replicas.
	// It's handy to have as reference here.
	assertDeepEqual(t, "replicas", map[string]tokenRingReplicas{
		keyspace: {
			{orderedToken("05"), []*HostInfo{hosts[0], hosts[1], hosts[2], hosts[3], hosts[4], hosts[5]}},
			{orderedToken("10"), []*HostInfo{hosts[1], hosts[2], hosts[3], hosts[4], hosts[5], hosts[6]}},
			{orderedToken("15"), []*HostInfo{hosts[2], hosts[3], hosts[4], hosts[5], hosts[6], hosts[7]}},
			{orderedToken("20"), []*HostInfo{hosts[3], hosts[4], hosts[5], hosts[6], hosts[7], hosts[8]}},
			{orderedToken("25"), []*HostInfo{hosts[4], hosts[5], hosts[6], hosts[7], hosts[8], hosts[9]}},
			{orderedToken("30"), []*HostInfo{hosts[5], hosts[6], hosts[7], hosts[8], hosts[9], hosts[10]}},
			{orderedToken("35"), []*HostInfo{hosts[6], hosts[7], hosts[8], hosts[9], hosts[10], hosts[11]}},
			{orderedToken("40"), []*HostInfo{hosts[7], hosts[8], hosts[9], hosts[10], hosts[11], hosts[0]}},
			{orderedToken("45"), []*HostInfo{hosts[8], hosts[9], hosts[10], hosts[11], hosts[0], hosts[1]}},
			{orderedToken("50"), []*HostInfo{hosts[9], hosts[10], hosts[11], hosts[0], hosts[1], hosts[2]}},
			{orderedToken("55"), []*HostInfo{hosts[10], hosts[11], hosts[0], hosts[1], hosts[2], hosts[3]}},
			{orderedToken("60"), []*HostInfo{hosts[11], hosts[0], hosts[1], hosts[2], hosts[3], hosts[4]}},
		},
	}, policyInternal.getMetadataReadOnly().replicas)

	// now the token ring is configured
	query.RoutingKey([]byte("18"))
	iter = policy.Pick(query)
	// first should be hosts with matching token from the local DC
	expectHosts(t, "matching token from local DC", iter, "4", "7")
	// rest should be hosts with matching token from remote DCs
	expectHosts(t, "matching token from remote DCs", iter, "3", "5", "6", "8")
	// followed by other hosts
	expectHosts(t, "rest", iter, "0", "1", "2", "9", "10", "11")
	expectNoMoreHosts(t, iter)
}

func TestHostPolicy_RackAwareRR(t *testing.T) {
	p := RackAwareRoundRobinPolicy("local", "b")

	hosts := [...]*HostInfo{
		{hostId: "0", connectAddress: net.ParseIP("10.0.0.1"), dataCenter: "local", rack: "a"},
		{hostId: "1", connectAddress: net.ParseIP("10.0.0.2"), dataCenter: "local", rack: "a"},
		{hostId: "2", connectAddress: net.ParseIP("10.0.0.3"), dataCenter: "local", rack: "b"},
		{hostId: "3", connectAddress: net.ParseIP("10.0.0.4"), dataCenter: "local", rack: "b"},
		{hostId: "4", connectAddress: net.ParseIP("10.0.0.5"), dataCenter: "remote", rack: "a"},
		{hostId: "5", connectAddress: net.ParseIP("10.0.0.6"), dataCenter: "remote", rack: "a"},
		{hostId: "6", connectAddress: net.ParseIP("10.0.0.7"), dataCenter: "remote", rack: "b"},
		{hostId: "7", connectAddress: net.ParseIP("10.0.0.8"), dataCenter: "remote", rack: "b"},
	}

	for _, host := range hosts {
		p.AddHost(host)
	}

	it := p.Pick(nil)

	// Must start with rack-local hosts
	expectHosts(t, "rack-local hosts", it, "3", "2")
	// Then dc-local hosts
	expectHosts(t, "dc-local hosts", it, "0", "1")
	// Then the remote hosts
	expectHosts(t, "remote hosts", it, "4", "5", "6", "7")
	expectNoMoreHosts(t, it)
}

// Tests of the token-aware host selection policy implementation with a
// DC & Rack aware round-robin host selection policy fallback
func TestHostPolicy_TokenAware_RackAware(t *testing.T) {
	const keyspace = "myKeyspace"
	policy := TokenAwareHostPolicy(RackAwareRoundRobinPolicy("local", "b"))
	policyWithFallback := TokenAwareHostPolicy(RackAwareRoundRobinPolicy("local", "b"), NonLocalReplicasFallback())

	policyInternal := policy.(*tokenAwareHostPolicy)
	policyInternal.getKeyspaceName = func() string { return keyspace }
	policyInternal.getKeyspaceMetadata = func(ks string) (*KeyspaceMetadata, error) {
		return nil, errors.New("not initialized")
	}

	policyWithFallbackInternal := policyWithFallback.(*tokenAwareHostPolicy)
	policyWithFallbackInternal.getKeyspaceName = policyInternal.getKeyspaceName
	policyWithFallbackInternal.getKeyspaceMetadata = policyInternal.getKeyspaceMetadata

	query := &Query{routingInfo: &queryRoutingInfo{}}
	query.getKeyspace = func() string { return keyspace }

	iter := policy.Pick(nil)
	if iter == nil {
		t.Fatal("host iterator was nil")
	}
	actual := iter()
	if actual != nil {
		t.Fatalf("expected nil from iterator, but was %v", actual)
	}

	// set the hosts
	hosts := [...]*HostInfo{
		{hostId: "0", connectAddress: net.IPv4(10, 0, 0, 1), tokens: []string{"05"}, dataCenter: "remote", rack: "a"},
		{hostId: "1", connectAddress: net.IPv4(10, 0, 0, 2), tokens: []string{"10"}, dataCenter: "remote", rack: "b"},
		{hostId: "2", connectAddress: net.IPv4(10, 0, 0, 3), tokens: []string{"15"}, dataCenter: "local", rack: "a"},
		{hostId: "3", connectAddress: net.IPv4(10, 0, 0, 4), tokens: []string{"20"}, dataCenter: "local", rack: "b"},
		{hostId: "4", connectAddress: net.IPv4(10, 0, 0, 5), tokens: []string{"25"}, dataCenter: "remote", rack: "a"},
		{hostId: "5", connectAddress: net.IPv4(10, 0, 0, 6), tokens: []string{"30"}, dataCenter: "remote", rack: "b"},
		{hostId: "6", connectAddress: net.IPv4(10, 0, 0, 7), tokens: []string{"35"}, dataCenter: "local", rack: "a"},
		{hostId: "7", connectAddress: net.IPv4(10, 0, 0, 8), tokens: []string{"40"}, dataCenter: "local", rack: "b"},
		{hostId: "8", connectAddress: net.IPv4(10, 0, 0, 9), tokens: []string{"45"}, dataCenter: "remote", rack: "a"},
		{hostId: "9", connectAddress: net.IPv4(10, 0, 0, 10), tokens: []string{"50"}, dataCenter: "remote", rack: "b"},
		{hostId: "10", connectAddress: net.IPv4(10, 0, 0, 11), tokens: []string{"55"}, dataCenter: "local", rack: "a"},
		{hostId: "11", connectAddress: net.IPv4(10, 0, 0, 12), tokens: []string{"60"}, dataCenter: "local", rack: "b"},
	}
	for _, host := range hosts {
		policy.AddHost(host)
		policyWithFallback.AddHost(host)
	}

	// the token ring is not setup without the partitioner, but the fallback
	// should work
	if actual := policy.Pick(nil)(); actual == nil {
		t.Fatal("expected to get host from fallback got nil")
	}

	query.RoutingKey([]byte("30"))
	if actual := policy.Pick(query)(); actual == nil {
		t.Fatal("expected to get host from fallback got nil")
	}

	policy.SetPartitioner("OrderedPartitioner")
	policyWithFallback.SetPartitioner("OrderedPartitioner")

	policyInternal.getKeyspaceMetadata = func(keyspaceName string) (*KeyspaceMetadata, error) {
		if keyspaceName != keyspace {
			return nil, fmt.Errorf("unknown keyspace: %s", keyspaceName)
		}
		return &KeyspaceMetadata{
			Name:          keyspace,
			StrategyClass: "NetworkTopologyStrategy",
			StrategyOptions: map[string]interface{}{
				"class":  "NetworkTopologyStrategy",
				"local":  2,
				"remote": 2,
			},
		}, nil
	}
	policyWithFallbackInternal.getKeyspaceMetadata = policyInternal.getKeyspaceMetadata
	policy.KeyspaceChanged(KeyspaceUpdateEvent{Keyspace: "myKeyspace"})
	policyWithFallback.KeyspaceChanged(KeyspaceUpdateEvent{Keyspace: "myKeyspace"})

	// The NetworkTopologyStrategy above should generate the following replicas.
	// It's handy to have as reference here.
	assertDeepEqual(t, "replicas", map[string]tokenRingReplicas{
		"myKeyspace": {
			{orderedToken("05"), []*HostInfo{hosts[0], hosts[1], hosts[2], hosts[3]}},
			{orderedToken("10"), []*HostInfo{hosts[1], hosts[2], hosts[3], hosts[4]}},
			{orderedToken("15"), []*HostInfo{hosts[2], hosts[3], hosts[4], hosts[5]}},
			{orderedToken("20"), []*HostInfo{hosts[3], hosts[4], hosts[5], hosts[6]}},
			{orderedToken("25"), []*HostInfo{hosts[4], hosts[5], hosts[6], hosts[7]}},
			{orderedToken("30"), []*HostInfo{hosts[5], hosts[6], hosts[7], hosts[8]}},
			{orderedToken("35"), []*HostInfo{hosts[6], hosts[7], hosts[8], hosts[9]}},
			{orderedToken("40"), []*HostInfo{hosts[7], hosts[8], hosts[9], hosts[10]}},
			{orderedToken("45"), []*HostInfo{hosts[8], hosts[9], hosts[10], hosts[11]}},
			{orderedToken("50"), []*HostInfo{hosts[9], hosts[10], hosts[11], hosts[0]}},
			{orderedToken("55"), []*HostInfo{hosts[10], hosts[11], hosts[0], hosts[1]}},
			{orderedToken("60"), []*HostInfo{hosts[11], hosts[0], hosts[1], hosts[2]}},
		},
	}, policyInternal.getMetadataReadOnly().replicas)

	query.RoutingKey([]byte("23"))

	// now the token ring is configured
	// Test the policy with fallback
	iter = policyWithFallback.Pick(query)

	// first should be host with matching token from the local DC & rack
	expectHosts(t, "matching token from local DC and local rack", iter, "7")
	// next should be host with matching token from local DC and other rack
	expectHosts(t, "matching token from local DC and non-local rack", iter, "6")
	// next should be hosts with matching token from other DC, in any order
	expectHosts(t, "matching token from non-local DC", iter, "4", "5")
	// then the local DC & rack that didn't match the token
	expectHosts(t, "non-matching token from local DC and local rack", iter, "3", "11")
	// then the local DC & other rack that didn't match the token
	expectHosts(t, "non-matching token from local DC and non-local rack", iter, "2", "10")
	// finally, the other DC that didn't match the token
	expectHosts(t, "non-matching token from non-local DC", iter, "0", "1", "8", "9")
	expectNoMoreHosts(t, iter)

	// Test the policy without fallback
	iter = policy.Pick(query)

	// first should be host with matching token from the local DC & Rack
	expectHosts(t, "matching token from local DC and local rack", iter, "7")
	// next should be the other two hosts from local DC & rack
	expectHosts(t, "non-matching token local DC and local rack", iter, "3", "11")
	// then the three hosts from the local DC but other rack
	expectHosts(t, "local DC, non-local rack", iter, "2", "6", "10")
	// then the 6 hosts from the other DC
	expectHosts(t, "non-local DC", iter, "0", "1", "4", "5", "8", "9")
	expectNoMoreHosts(t, iter)
}
