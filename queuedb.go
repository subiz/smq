// This package lets you store queues to cassandra
// before using this package, you must setup the cassandra keyspace using this cqlsh script
//   create keyspace KEYSPACE_NAME with replication={'class':'SimpleStrategy','replication_factor':1};
//   create table queues(queue ascii,segment bigint,offset bigint,data blob,primary key((queue, segment),offset)) with clustering order by (offset asc);
//   create table offsets(queue ascii,consumer_offset bigint,producer_offset bigint,primary key (queue));
//
package smq

import (
	"fmt"
	"time"

	"github.com/gocql/gocql"
	"github.com/subiz/errors"
)

const (
	// table offsets is used to store consumer offset and producer offset of queues
	tblOffsets = "offsets"

	// table queues is used to store messages inside queues
	// messages are divided into segments, each segment contains at most SEGMENT_SIZE messages.
	// we assign messages to segments by dividing message's offset to SEGMENT_SIZE.
	// for example, if SEGMENT_SIZE is 1000, segment 0 would contains message
	// offsets [1 ... 999], segment 1 would contains message offsets [1000 ... 1999],
	// and so on.
	// segment keeps cassandra partition small and one consumed, whole segment will be
	// deleted leaving much less tombstone than deleting individual message.
	tblQueues = "queues"
)

// SEGMENT_SIZE is maxinum number of messages inside a segment
const SEGMENT_SIZE = 1000

type Message struct {
	value  []byte
	offset int64
}

// Queue is used to persist (load) queue messages from (to) cassandra database
// This struct is not thread-safe, you must handle concurrency yourself
type Queue struct {
	// hold connection to casasndra cluster
	session *gocql.Session
}

// connect creates a new session to cassandra, this function will keep retry
// until a session is established sucessfully
// Parameters:
// seeds: contains list of cassandra "host:port"s, used to initially
//   connect to a cassandra cluster, the rest of the hosts will be automatically
//   discovered.
// keyspace: the cassandra keyspace to connect to
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

	defer defaultSession.Close()

	cluster.Keyspace = keyspace
	return cluster.CreateSession()
}

// NewQueue creates a ready-to-user Queue object
// Parameters:
// seeds: contains list of cassandra "host:port"s, used to initially
//   connect to a cassandra cluster, the rest of the hosts will be automatically
//   discovered.
// keyspace: the cassandra keyspace to connect to, keyspace must already had two
// tables offsets, and queues. See class comments
func NewQueue(seeds []string, ks string) (*Queue, error) {
	var err error
	me := &Queue{}
	me.session, err = connect(seeds, ks)
	if err != nil {
		return nil, err
	}
	return me, err
}

// Fetch loads next messages from the last committed offset
// this method returns maximum SEGMENT_SIZE messages and offset of the
// last message (or -1 for empty queue, or latest committed offset when
// no messages found)
// limit must less than SEGMENT_SIZE
func (me *Queue) Fetch(queue string, limit int) ([][]byte, int64, error) {
	if limit < 0 || SEGMENT_SIZE < limit { // make sure limit inside
		limit = SEGMENT_SIZE
	}

	initOffset, _, err := me.ReadIndex(queue) // last commited offset
	if err != nil {
		return nil, -1, err
	}

	valueArr := make([][]byte, 0)
	var lastOffset = initOffset

	segment := int64(-1)
	if initOffset < 0 {
		initOffset = -1
	} else {
		segment = initOffset / SEGMENT_SIZE // initial segment
		// move to the next segment if we have reached the end of a segment
		if (lastOffset+1)%SEGMENT_SIZE == 0 {
			segment++
		}
	}

	query := `SELECT offset, data FROM ` + tblQueues +
		` WHERE queue=? AND segment=? AND offset>? ORDER BY offset ASC LIMIT ?`

	value := make([]byte, 0)
	iter := me.session.Query(query, queue, segment, lastOffset, limit).Iter()
	for iter.Scan(&lastOffset, &value) {
		valueArr = append(valueArr, value)
		value = make([]byte, 0)
	}
	if err := iter.Close(); err != nil {
		return nil, -1, errors.Wrap(err, 500, errors.E_database_error)
	}

	// reading next segment if the current segment is out of message
	// (reached end of the current segment
	if len(valueArr) < limit && (lastOffset+1)%SEGMENT_SIZE == 0 {
		iter := me.session.Query(query, queue, segment+1, lastOffset,
			limit-len(valueArr)).Iter()
		for iter.Scan(&lastOffset, &value) {
			valueArr = append(valueArr, value)
			value = make([]byte, 0)
		}
		if err := iter.Close(); err != nil {
			return nil, -1, errors.Wrap(err, 500, errors.E_database_error)
		}
	}

	return valueArr, lastOffset, nil
}

// ReadIndex reads queue consumer offset and producer offset
// SIDE EFFECTS:
// + this function also updates queue cache to latest value
func (me *Queue) ReadIndex(queue string) (csm, pro int64, err error) {
	query := "SELECT consumer_offset, producer_offset FROM " + tblOffsets +
		" WHERE queue=?"
	err = me.session.Query(query, queue).Scan(&csm, &pro)
	if err != nil && err.Error() == gocql.ErrNotFound.Error() {
		return -1, -1, nil
	}
	if err != nil {
		return -1, -1, errors.Wrap(err, 500, errors.E_database_error)
	}
	return csm, pro, nil
}

// Enqueue pushs new messages to queue, it returns messages offset
// TODO: should convert to batch to protect data integrity
func (me *Queue) Enqueue(queue string, value []byte) (int64, error) {
	_, offset, err := me.ReadIndex(queue)
	if err != nil {
		return -1, err
	}

	offset++
	err = me.session.Query("INSERT INTO "+tblOffsets+
		" (queue, producer_offset) VALUES(?,?)", queue, offset).Exec()
	if err != nil {
		return -1, errors.Wrap(err, 500, errors.E_database_error)
	}

	query := "INSERT INTO " + tblQueues + "(queue, segment, offset, data) " +
		"VALUES(?,?,?,?)"
	segment := offset / SEGMENT_SIZE
	err = me.session.Query(query, queue, segment, offset, value).Exec()
	if err != nil {
		return -1, errors.Wrap(err, 500, errors.E_database_error)
	}

	return offset, nil
}

// Commit increases current consumer offse
// its now safe for Queue to delete lesser offset messages
// this function ignore if user try to commit old messages
func (me *Queue) Commit(queue string, offset int64) error {
	csm, _, err := me.ReadIndex(queue)
	if err != nil {
		return err
	}
	if offset < csm {
		return nil
	}

	err = me.session.Query("INSERT INTO "+tblOffsets+
		" (queue, consumer_offset) VALUES(?,?)", queue, offset).Exec()
	if err != nil {
		return errors.Wrap(err, 500, errors.E_database_error)
	}

	me.c.Add("consumer-"+queue, offset)
	return nil
}
