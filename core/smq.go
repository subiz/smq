package core

import (
	"bitbucket.org/subiz/gocommon"
	"strconv"
)

type Job struct {
	ID, Value string
}

// why not use kafka? kafka doesn't support unlimited queue
type smdb interface {
	UpsertJobIndex(partition, queue, jobid, state string)
	UpsertJob(partition, queue, jobid, value string)
	ListJobs(partition, queue, start string, n int) []*Job

	// IterQueue iterate over all queues in partition
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
func (me *MQ) Enqueue(par, queue, value string) string {
	defer me.lm.Lock(par + queue).Unlock()

	// new id = last jobid + 1
	_, _, _, lastid := me.db.ReadIndex(par, queue)
	lastid64, _ := strconv.ParseInt(lastid, 0, 64)
	lastid64++
	jobid := strconv.FormatInt(lastid64, 10)

	me.db.SetLastJobID(par, queue, jobid)
	me.db.UpsertJob(par, queue, jobid, value)
	return jobid
}

// Commit job
func (me *MQ) Commit(partition, queue, jobid, state string) {
	me.lm.Lock(partition + queue).Unlock()
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
