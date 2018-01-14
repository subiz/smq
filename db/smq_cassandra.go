package db

import (
	"github.com/gocql/gocql"
	"bitbucket.org/subiz/gocommon"
	"fmt"
	"time"
	core "bitbucket.org/subiz/smq/core"
	"bitbucket.org/subiz/header/lang"
)

const (
	keyspace = "smq"
	tblIndices = "indices"
	tableJobs = "jobs"
)

// SubDB manages subscription for webhook
type QueueDB struct {
	session *gocql.Session
	keyspace string
	jobttl int
}

// Config initialize db connector and connect to cassandra cluster
func (me *QueueDB) Config(seeds []string, keyspaceprefix string, repfactor int, jobttl time.Duration) {
	me.keyspace = keyspaceprefix + keyspace
	me.createKeyspace(seeds, repfactor)
	me.createTables(seeds)
	me.jobttl = int(jobttl.Seconds())
}

func (me *QueueDB) createKeyspace(seeds []string, repfactor int) {
	cluster := gocql.NewCluster(seeds...)
	cluster.Timeout = 10 * time.Second
	cluster.Keyspace = "system"
	var defsession, err = cluster.CreateSession()
	defer defsession.Close()
	common.DieIf(err, lang.T_database_error, "failed to connect to cluster: %v", seeds)
	err = defsession.Query(fmt.Sprintf(`
		CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {
			'class': 'SimpleStrategy',
			'replication_factor': %d
		}`, me.keyspace, repfactor)).Exec()
	common.DieIf(err, lang.T_database_error, "failed to create keyspace %s", me.keyspace)
}

func (me *QueueDB) createTables(seeds []string) {
	cluster := gocql.NewCluster(seeds...)
	cluster.Timeout = 10 * time.Second
	cluster.Keyspace = me.keyspace
	var err error
	me.session, err = cluster.CreateSession()
	common.DieIf(err, lang.T_database_error, "failed to connect to cluster: %v", seeds)

	err = me.session.Query(fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
		par ASCII,
		queue ASCII,
		current_job BIGINT,
		last_job BIGINT,
		state ASCII,
		PRIMARY KEY (par, queue)
	) WITH CLUSTERING ORDER BY (queue ASC)`, tblIndices)).Exec()
	common.DieIf(err, lang.T_database_error, "failed to create table %s", tblIndices)

	err = me.session.Query(fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
		par ASCII,
		queue ASCII,
		job_id BIGINT,
		value TEXT,
		PRIMARY KEY ((par, queue), job_id)
	) WITH CLUSTERING ORDER BY (job_id ASC)`, tableJobs)).Exec()
	common.DieIf(err, lang.T_database_error, "failed to create table %s", tableJobs)
}

func (me *QueueDB) UpsertJobIndex(partition, queue string, jobid int64, state string) {
	query := "UPDATE " + tblIndices + " SET current_job=?, state=? WHERE par=? AND queue=?"
	err := me.session.Query(query, jobid, state, partition, queue).Exec()
	common.DieIf(err, lang.T_database_error, "unable to update jobindex %s, %s, %s, %s", partition, queue, jobid, state)
}

func (me *QueueDB) UpsertJob(partition, queue string, jobid int64, value string) {
	query := "INSERT INTO " + tableJobs + "(par, queue, job_id, value) VALUES(?,?,?,?)"
	err := me.session.Query(query, partition, queue, jobid, value).Exec()
	common.DieIf(err, lang.T_database_error, "unable to create job, %s, %s, %s, %s", partition, queue, jobid, value)
}

func (me *QueueDB) ListJobs(partition, queue string, start int64, n int) []*core.Job {
	jobs := make([]*core.Job, 0)
	if n == 0 {
		return jobs
	}
	query := `SELECT job_id, value FROM ` + tableJobs + ` WHERE par=? AND queue=? AND job_id>=? LIMIT ?`
	var jobid int64
	var value string
	iter := me.session.Query(query, partition, queue, start, n).Iter()
	for iter.Scan(&jobid, &value) {
		jobs = append(jobs, &core.Job{
			ID: jobid,
			Value: value,
		})
	}
	err := iter.Close()
	if err != nil && err.Error() != gocql.ErrNotFound.Error() {
		common.DieIf(err, lang.T_database_error, "unable to list jobs %s, %s, %s, %d", partition, queue, start, n)
	}
	return jobs
}

// IterQueue iterate over all queues in partition
func (me *QueueDB) IterQueue(partition string) <-chan string {
	outchan := make(chan string)
	go func() {
		defer close(outchan)
		query := `SELECT queue FROM ` + tblIndices + ` WHERE par=?`
		var queue string
		iter := me.session.Query(query, partition).Iter()
		for iter.Scan(&queue) {
			outchan <- queue
		}
		err := iter.Close()
		if err != nil && err.Error() != gocql.ErrNotFound.Error() {
			common.DieIf(err, lang.T_database_error, "unable to scan partition %s", partition)
		}
	}()
	return outchan
}

// DeleteIndex deletes index
func (me *QueueDB) DeleteIndex(partition, queue string) {
	query := "DELETE FROM " + tblIndices + " WHERE par=? AND queue=?"
	err := me.session.Query(query, partition, queue).Exec()
	common.DieIf(err, lang.T_database_error, "unable to delete index %s, %s", partition, queue)
}

func (me *QueueDB) DeleteJobs(partition, queue string) {
	query := "DELETE FROM " + tblJobs + " WHERE par=? AND queue=?"
	err := me.session.Query(query, partition, queue).Exec()
	common.DieIf(err, lang.T_database_error, "unable to delete jobs %s, %s", partition, queue)
}

func (me *QueueDB) ReadIndex(partition, queue string) (found bool, index int64, state string, lastjobid int64) {
	query := "SELECT current_job, last_job, state FROM " + tblIndices + " WHERE par=? AND queue=?"
	err := me.session.Query(query, partition, queue).Scan(&index, &lastjobid, &state)
	if err != nil {
		if err.Error() != gocql.ErrNotFound.Error() {
			common.DieIf(err, lang.T_database_error, "unable to read index %s, %s", partition, queue)
		}
		found = false
	} else {
		found = true
	}
	return
}

func (me *QueueDB) SetLastJobID(partition, queue string, lastjobid int64) {
	query := "UPDATE " + tblIndices + " SET last_job=? WHERE par=? AND queue=?"
	err := me.session.Query(query, lastjobid, partition, queue).Exec()
	common.DieIf(err, lang.T_database_error, "unable to update jobindex %s, %s, %v", partition, queue, lastjobid)
}
