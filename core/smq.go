package core

import (
	"bitbucket.org/subiz/gocommon"
	_ "runtime"
)

type Job struct {
	ID    int64
	Value string
}

// why not use kafka? kafka doesn't support unlimited queue
type smdb interface {
	UpsertJobIndex(partition, queue string, jobid int64, state string)
	UpsertJob(partition, queue string, jobid int64, value string)
	ListJobs(partition, queue string, start int64, n int) []*Job
	DeleteJobs(partition, queue string)

	// IterQueue iterate over all queues in partition
	IterQueue(partition string) <-chan string
	DeleteIndex(partition, queue string)
	ReadIndex(partition, queue string) (found bool, index int64, state string, lastjobid int64)
	SetLastJobID(partition, queue string, lastjobid int64)
}

// MQ Message queue
type MQ struct {
	lm *common.MutexMap
	db smdb
}

func (me *MQ) Config(db smdb) {
	me.lm = common.NewMutexMap()
	me.db = db
}

// Enqueue add new job to specific queue
func (me *MQ) Enqueue(par, queue, value string) int64 {
	defer me.lm.Lock(par + queue).Unlock()

	// new id = last jobid + 1
	_, _, _, lastid64 := me.db.ReadIndex(par, queue)
	lastid64++
	jobid := lastid64

	me.db.SetLastJobID(par, queue, jobid)
	me.db.UpsertJob(par, queue, jobid, value)
	return jobid
}

// Commit job
func (me *MQ) Commit(partition, queue string, jobid int64, state string) {
	defer me.lm.Lock(partition + queue).Unlock()
	_, index, _, _ := me.db.ReadIndex(partition, queue)
	if jobid >= index || jobid == -1 {
		me.db.UpsertJobIndex(partition, queue, jobid, state)
	}
}

// Peek list N jobs in queue start from current job in queue
func (me *MQ) Peek(partition, queue string, n int) (found bool, index int64, state string, jobs []*Job) {
	defer me.lm.Lock(partition + queue).Unlock()
	var lastjobid int64
	found, index, state, lastjobid = me.db.ReadIndex(partition, queue)
	//runtime.Breakpoint()
	if state == "" && index == lastjobid {
		me.db.DeleteIndex(partition, queue)
	}
	if !found {
		return
	}

	jobs = me.db.ListJobs(partition, queue, index, n)
	return
}

func (me *MQ) QueueIter(partition string) <-chan string {
	return me.db.IterQueue(partition)
}

func (me *MQ) List(partition, queue string, start int64, n int) []*Job {
	return me.db.ListJobs(partition, queue, start, n)
}

func (me *MQ) DeleteQueue(partition, queue string) {
	me.db.DeleteIndex(partition, queue)
	me.db.DeleteJobs(partition, queue)
}
