package gocql

import (
	"encoding/hex"
	"net"
	"sort"
	"strconv"
	"sync"
)

type QualifiedTableName struct {
	keyspacename string
	tablename    string
}

func (q *QualifiedTableName) NewQualifiedTableName(keyspacename string, tablename string) {
	q.keyspacename = keyspacename
	q.tablename = tablename
}

func getKeyspaceName(QualifiedTableName *QualifiedTableName) string {
	return QualifiedTableName.keyspacename
}

func getTableName(QualifiedTableName *QualifiedTableName) string {
	return QualifiedTableName.tablename
}

type PartitionMetadata struct {
	startkey int64
	endkey   int64
	hosts    []*HostInfo
}

func (p *PartitionMetadata) NewPartitionMetadata(s_key int64, e_key int64, h []*HostInfo) {
	p.startkey = s_key
	p.endkey = e_key
	p.hosts = h
}

func getStartKey(PartitionMetadata *PartitionMetadata) int64 {
	return PartitionMetadata.startkey
}

func getEndKey(PartitionMetadata *PartitionMetadata) int64 {
	return PartitionMetadata.endkey
}

func getHosts(PartitionMetadata PartitionMetadata) []*HostInfo {
	return PartitionMetadata.hosts
}

type TableSplitMetadata struct {
	partitionMap map[int64]PartitionMetadata
}

func (r *TableSplitMetadata) Floor(num int64) int64 {
	keys := make([]int64, 0, len(r.partitionMap))

	for k := range r.partitionMap {
		keys = append(keys, k)
	}

	var u int
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
	var i int
	for i = 0; i < len(keys); i++ {
		if keys[i] > num {
			u = i - 1
			break
		}
	}
	if i == len(keys) {
		return keys[len(keys)-1]
	} else {
		return keys[u]
	}
}

func (r *TableSplitMetadata) NewTableSplitMetadata() {
	r.partitionMap = make(map[int64]PartitionMetadata)
}

func (r *TableSplitMetadata) getPartitionMetadata(key int64) PartitionMetadata {
	var p PartitionMetadata
	p = r.partitionMap[key]
	return p
}

func (r *TableSplitMetadata) getHosts(key int64) []*HostInfo {
	p := r.getPartitionMetadata(key)
	return getHosts(p)
}

func (r *TableSplitMetadata) getPartitionMap() map[int64]PartitionMetadata {
	return r.partitionMap
}

func mapkey(m map[*net.IP]string, value string) (key []*net.IP, ok bool) {
	for k, v := range m {
		if v == value {
			key = append(key, k)
			ok = true
		}
	}
	return
}

type tablesplitsStruc struct {
	mu          sync.Mutex
	tableSplits map[QualifiedTableName]TableSplitMetadata
}

var tablesplits tablesplitsStruc

func (t *tablesplitsStruc) Write(table map[QualifiedTableName]TableSplitMetadata) {
	t.mu.Lock()
	t.tableSplits = table
	t.mu.Unlock()
}

func (t *tablesplitsStruc) Read() (table map[QualifiedTableName]TableSplitMetadata) {
	t.mu.Lock()
	table = t.tableSplits
	t.mu.Unlock()
	return
}

func getTableSplitMetadata(keyspacename string, tablename string) TableSplitMetadata {
	var p QualifiedTableName
	p.NewQualifiedTableName(keyspacename, tablename)
	table := tablesplits.Read()
	return table[p]
}

func (r *ringDescriber) getClusterPartitionInfo() error {

	Iter := r.session.control.query("SELECT * FROM system.partitions;")

	var tableSplits1 = make(map[QualifiedTableName]TableSplitMetadata)
	if Iter == nil {
		return errNoControl
	}

	var (
		keyspace_name     string
		table_name        string
		start_key         string
		end_key           string
		id                UUID
		replica_addresses map[*net.IP]string
	)

	rows := Iter.Scanner()

	for rows.Next() {
		rows.Scan(&keyspace_name, &table_name, &start_key, &end_key, &id, &replica_addresses)
		var tableId QualifiedTableName
		tableId.NewQualifiedTableName(keyspace_name, table_name)

		var tableSplitMetadata TableSplitMetadata
		tableSplitMetadata.NewTableSplitMetadata()
		tableSplitMetadata = tableSplits1[tableId]

		if tableSplitMetadata.partitionMap == nil {
			tableSplitMetadata.NewTableSplitMetadata()
			tableSplits1[tableId] = tableSplitMetadata
		}

		var hosts []*HostInfo
		ip, _ := mapkey(replica_addresses, "LEADER")
		for i := 0; i < len(ip); i++ {
			host, _ := r.getHostInfoFromIp(*ip[i])
			if host != nil {
				hosts = append(hosts, host)
			}
		}

		ip, _ = mapkey(replica_addresses, "FOLLOWER")
		for i := 0; i < len(ip); i++ {
			host, _ := r.getHostInfoFromIp(*ip[i])
			if host != nil {
				hosts = append(hosts, host)
			}
		}

		ip, _ = mapkey(replica_addresses, "READ_REPLICA")
		for i := 0; i < len(ip); i++ {
			host, _ := r.getHostInfoFromIp(*ip[i])
			if host != nil {
				hosts = append(hosts, host)
			}
		}

		hstart := hex.EncodeToString([]byte(start_key))
		hend := hex.EncodeToString([]byte(end_key))
		var (
			s int64
			e int64
		)
		if start_key != "" {
			s, _ = strconv.ParseInt(hstart, 16, 64)
		} else {
			s = 0
		}
		if end_key != "" {
			e, _ = strconv.ParseInt(hend, 16, 64)
		} else {
			e = 0
		}

		p := tableSplitMetadata.getPartitionMap()
		var h PartitionMetadata
		h.NewPartitionMetadata(s, e, hosts)
		p[s] = h
	}

	tablesplits.Write(tableSplits1)
	return nil
}
