package smq

import (
	"github.com/subiz/smq/core"
	"github.com/subiz/smq/db"
	"time"
)

// SMQ subiz message queue
type SMQ interface {
	Enqueue(partition, queue, value string) int64
	Commit(partition, queue string, jobid int64, state string)
	Peek(partition, queue string, njob int) (found bool, index int64, state string, jobs []*core.Job)
	QueueIter(partition string) <-chan string
	List(partition, queue string, start int64, n int) []*core.Job
	DeleteQueue(partition, queue string)
}

// NewSMQ create new subiz mq
func NewSMQ(seeds []string, prefix string, nrep int, jobttl time.Duration) SMQ {
	db := &db.QueueDB{}
	db.Config(seeds, prefix, nrep, jobttl)
	mq := &core.MQ{}
	mq.Config(db)
	return mq
}
