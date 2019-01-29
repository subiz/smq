package smq

import (
	"fmt"
	"github.com/gocql/gocql"
	cache "github.com/hashicorp/golang-lru"
	"github.com/subiz/errors"
	"time"
)

const (
	tblOffsets = "offsets"
	tblQueues  = "queues"
)

type SMQ interface {
	Enqueue(partition int, queue string, value []byte) (int64, error)

	// Commit updates consumer offset for a queue
	Commit(partition int, queue string, index int64) error

	// the latter paramteter is index of the last message
	Fetch(partition int, queue string) ([][]byte, int64, error)
}

const GROUP_SIZE = 1000

// QueueDB manages subscription for webhook
// CREATE KEYSPACE smqtest WITH REPLICATION = {'class':'SimpleStrategy','replication_factor':1};
// CREATE TABLE queues(queue ASCII,partition INT,group BIGINT,offset BIGINT,data BLOB, PRIMARY KEY ((queue, partition, group), offset)) WITH CLUSTERING ORDER BY (offset ASC);
// CREATE TABLE offsets(queue ASCII, partition INT,consumer_offset BIGINT,producer_offset BIGINT,PRIMARY KEY ((queue, partition)));
//
type QueueDB struct {
	session *gocql.Session
	l       *cache.Cache
}

func connect(seeds []string, keyspace string) (*gocql.Session, error) {
	cluster := gocql.NewCluster(seeds...)
	cluster.Timeout = 10 * time.Second
	cluster.Keyspace = "system_schema"
	var defaultSession *gocql.Session
	var err error
	for {
		if defaultSession, err = cluster.CreateSession(); err == nil {
			break
		}
		fmt.Println("cassandra", err, ". Retring after 5sec...")
		time.Sleep(5 * time.Second)
	}

	fmt.Println("CONNECTED TO ", seeds)
	defer defaultSession.Close()

	cluster.Keyspace = keyspace
	return cluster.CreateSession()
}

// Config initialize db connector and connect to cassandra cluster
func NewQueueDB(seeds []string, ks string) (*QueueDB, error) {
	var err error
	me := &QueueDB{}
	me.session, err = connect(seeds, ks)
	if err != nil {
		return nil, err
	}
	me.l, err = cache.New(128000)
	if err != nil {
		return nil, err
	}
	return me, err
}

func (me *QueueDB) Fetch(partition int, queue string) ([][]byte, int64, error) {
	initoffset, maxoffset, err := me.readIndex(partition, queue)
	if err != nil {
		return nil, -1, err
	}

	group := initoffset / GROUP_SIZE
	query := `SELECT offset, data FROM ` + tblQueues +
		` WHERE partition=? AND queue=? AND group=? AND offset>? ORDER BY offset ASC LIMIT ?`

	valueArr := make([][]byte, 0)
	value := make([]byte, 0)
	var lastoffset int64
	iter := me.session.Query(query, partition, queue, group, initoffset, GROUP_SIZE).Iter()
	for iter.Scan(&lastoffset, &value) {
		valueArr = append(valueArr, value)
		value = make([]byte, 0)
	}
	if err := iter.Close(); err != nil {
		return nil, 0, errors.Wrap(err, 500, errors.E_database_error)
	}

	// out of current group (not group out of message)
	if len(valueArr) < GROUP_SIZE && lastoffset < maxoffset {
		iter := me.session.Query(query, partition, queue, group+1, lastoffset,
			GROUP_SIZE-len(valueArr)).Iter()
		for iter.Scan(&lastoffset, &value) {
			valueArr = append(valueArr, value)
			value = make([]byte, 0)
		}
		if err := iter.Close(); err != nil {
			return nil, 0, errors.Wrap(err, 500, errors.E_database_error)
		}
	}

	return valueArr, lastoffset, nil
}

func (me *QueueDB) readIndex(partition int, queue string) (csm, pro int64, err error) {
	csmi, csmok := me.l.Get(fmt.Sprintf("consumer-%d-%s", partition, queue))
	proi, prook := me.l.Get(fmt.Sprintf("producer-%d-%s", partition, queue))
	if csmok && prook {
		return csmi.(int64), proi.(int64), nil
	}

	query := "SELECT consumer_offset, producer_offset FROM " + tblOffsets +
		" WHERE partition=? AND queue=?"

	err = me.session.Query(query, partition, queue).Scan(&csm, &pro)
	if err != nil && err.Error() == gocql.ErrNotFound.Error() {
		me.l.Add(fmt.Sprintf("consumer-%d-%s", partition, queue), int64(0))
		me.l.Add(fmt.Sprintf("producer-%d-%s", partition, queue), int64(0))
		return 0, 0, nil
	}
	if err != nil {
		return -1, -1, errors.Wrap(err, 500, errors.E_database_error)
	}
	me.l.Add(fmt.Sprintf("consumer-%d-%s", partition, queue), csm)
	me.l.Add(fmt.Sprintf("producer-%d-%s", partition, queue), pro)
	return csm, pro, nil
}

func (me *QueueDB) updateProducerIndex(partition int, queue string, offset int64) error {
	err := me.session.Query("INSERT INTO "+tblOffsets+
		" (partition, queue, producer_offset) VALUES(?,?,?)", partition, queue, offset).Exec()
	if err != nil {
		return errors.Wrap(err, 500, errors.E_database_error)
	}
	me.l.Add(fmt.Sprintf("producer-%d-%s", partition, queue), offset)
	return nil
}

func (me *QueueDB) updateConsumerIndex(partition int, queue string, offset int64) error {
	err := me.session.Query("INSERT INTO "+tblOffsets+
		" (partition, queue, consumer_offset) VALUES(?,?,?)", partition, queue, offset).Exec()
	if err != nil {
		return errors.Wrap(err, 500, errors.E_database_error)
	}

	me.l.Add(fmt.Sprintf("consumer-%d-%s", partition, queue), offset)
	return nil
}

func (me *QueueDB) Enqueue(partition int, queue string, value []byte) (int64, error) {
	_, offset, err := me.readIndex(partition, queue)
	if err != nil {
		return -1, err
	}

	offset++
	if err := me.updateProducerIndex(partition, queue, offset); err != nil {
		return -1, err
	}

	query := "INSERT INTO " + tblQueues + "(partition, queue, group, offset, data) " +
		"VALUES(?,?,?,?,?)"
	group := offset / GROUP_SIZE
	err = me.session.Query(query, partition, queue, group, offset, value).Exec()
	if err != nil {
		return -1, errors.Wrap(err, 500, errors.E_database_error)
	}

	return offset, nil
}

func (me *QueueDB) Commit(partition int, queue string, offset int64) error {
	csm, pro, err := me.readIndex(partition, queue)
	if err != nil {
		return err
	}
	if offset < csm {
		return nil
	}
	if pro < offset {
		offset = pro
	}
	return me.updateConsumerIndex(partition, queue, offset)
}
