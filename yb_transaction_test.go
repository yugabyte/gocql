package gocql

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

//Test of Transactions supported by YCQL
func Test_YB_Trsansaction(t *testing.T) {
	//change the ip address according to the cluster
	cluster := NewCluster("127.0.0.1")
	cluster.Timeout = 5 * time.Second

	session, err := cluster.CreateSession()
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()

	//create keyspace "example"
	createStmt := "CREATE Keyspace IF NOT EXISTS example"
	if err := session.Query(createStmt).Exec(); err != nil {
		t.Fatal(err)
	}

	// Create test table "test1".
	var createStmtt1 = "CREATE TABLE example.test1 (test1_id bigint PRIMARY KEY, count bigint) WITH default_time_to_live = 0 AND transactions = {'enabled': 'true'}"
	if err := session.Query(createStmtt1).Exec(); err != nil {
		t.Fatal(err)
	}

	// Create test table "test2".
	var createStmtt2 = "CREATE TABLE example.test2 (test2_id text PRIMARY KEY, count bigint) WITH default_time_to_live = 0 AND transactions = {'enabled': 'true'}"
	if err := session.Query(createStmtt2).Exec(); err != nil {
		t.Fatal(err)
	}

	insertValues := make([]interface{}, 0)
	insertValues = append(insertValues, 1, 1, "1", 1)

	// Insert a row in table "test1" and "test2" as a single transaction
	s := fmt.Sprintf(`START TRANSACTION;
	INSERT INTO %[1]s.test1(test1_id, count) values(?,?);
	INSERT INTO %[1]s.test2(test2_id, count) values(?,?);;
	COMMIT;`, "example")
	if err = session.Query(s).Bind(insertValues...).Exec(); err != nil {
		t.Fatal(err)
	}

	values := make([]interface{}, 0)
	values = append(values, 100, 1, 3, 100, "1", 3)

	queryBuilder := "BEGIN TRANSACTION UPDATE example.test1 USING TTL ? SET count = count + 1 WHERE test1_id = ? IF count + 1 <= ? ELSE ERROR; UPDATE example.test2 USING TTL ? SET count = count + 1 WHERE test2_id = ? IF count + 1 <= ? ELSE ERROR; END TRANSACTION;"
	if err = session.Query(queryBuilder).Bind(values...).Exec(); err != nil {
		t.Fatal(err)
	}

	time.Sleep(5 * time.Second)
	selectQry1 := session.Query("select count from example.test1 where test1_id = ?", 1)
	var c int
	selectQry1.Scan(&c)
	assertEqual(t, "count value", 2, c)

	selectQry2 := session.Query("select count from example.test2 wherez ?", '1')
	selectQry2.Scan(&c)
	assertEqual(t, "count value", 2, c)

	queryBuilder = "START TRANSACTION; UPDATE example.test1 USING TTL ? SET count = count + 1 WHERE test1_id = ? IF count + 1 <= ? ELSE ERROR; UPDATE example.test2 USING TTL ? SET count = count + 1 WHERE test2_id = ? IF count + 1 <= ? ELSE ERROR; COMMIT;"
	if err = session.Query(queryBuilder).Bind(values...).Exec(); err != nil {
		t.Fatal(err)
	}

	time.Sleep(5 * time.Second)
	selectQry1 = session.Query("select count from example.test1 where test1_id = ?", 1)
	selectQry1.Scan(&c)
	assertEqual(t, "count value", 3, c)

	selectQry2 = session.Query("select count from example.test2 where test2_id = ?", '1')
	selectQry2.Scan(&c)
	assertEqual(t, "count value", 3, c)

	queryBuilder = "START TRANSACTION; UPDATE example.test1 USING TTL ? SET count = count + 1 WHERE test1_id = ? IF count + 1 <= ? ELSE ERROR; UPDATE example.test2 USING TTL ? SET count = count + 1 WHERE test2_id = ? IF count + 1 <= ? ELSE ERROR; COMMIT;"
	if err = session.Query(queryBuilder).Bind(values...).Exec(); err != nil {
		errorStr := strings.Split(err.Error(), ";")
		assertEqual(t, "Error", "code=2200", errorStr[0])
		assertEqual(t, "Error", " message=Execution Error. Condition on table test1 was not satisfied.", strings.Split(errorStr[1], "\n")[0])
	}

	time.Sleep(5 * time.Second)
	selectQry1 = session.Query("select count from example.test1 where test1_id = ?", 1)
	selectQry1.Scan(&c)
	assertEqual(t, "count value", 3, c)

	selectQry2 = session.Query("select count from example.test2 where test2_id = ?", '1')
	selectQry2.Scan(&c)
	assertEqual(t, "count value", 3, c)
}
