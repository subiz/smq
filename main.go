package smq

import (
	"bitbucket.org/subiz/smq/core"
	"bitbucket.org/subiz/smq/db"
	"time"
)

// SMQ subiz message queue
type SMQ interface {
	Enqueue(partition, queue, value string) string
	Commit(partition, queue, jobid, state string)
	Peek(partition, queue string, njob int) (found bool, index, state string, jobs []*core.Job)
	QueueIter(partition string) <-chan string
	List(partition, queue, start string, n int) []*core.Job
}

// NewSMQ create new subiz mq
func NewSMQ(seeds []string, prefix string, nrep int, jobttl time.Duration) SMQ {
	db := &db.QueueDB{}
	db.Config(seeds, prefix, nrep, jobttl)
	mq := &core.MQ{}
	mq.Config(db)
	return mq
}
