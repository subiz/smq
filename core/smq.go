package core

import (
	"bitbucket.org/subiz/gocommon"
	"bitbucket.org/subiz/id"
)

type Job struct {
	JobID, Value string
}

type smdb interface {
	UpsertJobIndex(partition, queue, jobid, state string)
	UpsertJob(partition, queue, jobid, value string)
	ListJobs(partition, queue, start string, n int) []*Job
	IterQueue(partition string) <-chan string
	DeleteIndex(partition, queue string)
	ReadIndex(partition, queue string) (found bool, index, state, lastjobid string)
	SetLastJobID(partition, queue, lastjobid string)
}

// MQ Message queue
type MQ struct {
	lm *common.MutexMap
	db smdb
}

func (me *MQ) Config(db smdb) {
	me.lm = &common.MutexMap{}
	me.db = db
	me.lm.Init()
}

// Enqueue add new job to specific queue
func (me *MQ) Enqueue(partition, queue, value string) string {
	me.lm.Lock(partition + queue)
	defer me.lm.Unlock(partition + queue)
	jobid := ID.NewSmqJobID()
	me.db.SetLastJobID(partition, queue, jobid)
	me.db.UpsertJob(partition, queue, jobid, value)
	return jobid
}

// Commit job
func (me *MQ) Commit(partition, queue, jobid, state string) {
	me.lm.Lock(partition + queue)
	defer me.lm.Unlock(partition + queue)
	me.db.UpsertJobIndex(partition, queue, jobid, state)
}

// Peek list N jobs in queue start from current job in queue
func (me *MQ) Peek(partition, queue string, n int) (found bool, index, state string, jobs []*Job) {
	defer me.lm.Lock(partition + queue).Unlock()
	var lastjobid string
	found, index, state, lastjobid = me.db.ReadIndex(partition, queue)
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

func (me *MQ) List(partition, queue, start string, n int) []*Job {
	return me.db.ListJobs(partition, queue, start, n)
}
