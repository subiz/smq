package smq_test

import (
	"testing"
	. "bitbucket.org/subiz/smq"
	"bitbucket.org/subiz/gocommon"
	"time"
	"bitbucket.org/subiz/id"
	"strconv"
	"strings"
	"sync"
)

var queue SMQ

func skipQueueTest(t *testing.T) {
	t.Skip()
}

func tearupQueueTest(t *testing.T) {
	cassandraHost := common.StartCassandraDev("")
	queue = NewSMQ([]string{cassandraHost}, "smq", 1, 1 * time.Hour)
}

func TestQueue(t *testing.T) {
	//skipQueueTest(t)
	tearupQueueTest(t)
	par, q := "par" + ID.New(), "queue" + ID.New()
	j1 := queue.Enqueue(par, q, "1")
	j2 := queue.Enqueue(par, q, "2")
	j3 := queue.Enqueue(par, q, "3")
	found, jobid, state, jobs := queue.Peek(par, q, 2)
	if !found || 0 != jobid || state != "" || len(jobs) != 2 ||
		jobs[0].ID != j1 || jobs[0].Value != "1" ||
		jobs[1].ID != j2 || jobs[1].Value != "2" {
		t.Errorf("wrong %s", state)
	}

	jobs = queue.List(par, q, 0, 10)
	if len(jobs) != 3 ||
		jobs[0].ID != j1 || jobs[0].Value != "1" ||
		jobs[1].ID != j2 || jobs[1].Value != "2" ||
		jobs[2].ID != j3 || jobs[2].Value != "3" {
		t.Errorf("wrong")
	}
	queue.Commit(par, q, j2, "good")
	found, jobid, state, jobs = queue.Peek(par, q, 2)
	if !found || jobid != j2 || state != "good" || len(jobs) != 2 ||
		jobs[0].ID != j2 || jobs[0].Value != "2" {
		t.Errorf("wrong, %v", jobs[0])
	}
}

func TestQueueIter(t *testing.T) {
	//skipQueueTest(t)
	tearupQueueTest(t)
	par := "par" + ID.New()
	sum := 0
	for q := 0; q < 100; q++ {
		sum += q
		queue.Enqueue(par, "queue=" + strconv.Itoa(q) + "=" + ID.New(), strconv.Itoa(q))
	}
	queuechan := queue.QueueIter(par)
	for item := range queuechan {
		itemSplit := strings.Split(item, "=")
		if len(itemSplit) != 3 {
			t.Error("mut be 3")
		}
		q, err := strconv.Atoi(itemSplit[1])
		if err != nil {
			t.Errorf("%v", err)
		}
		sum -= q
	}
	if sum != 0 {
		t.Errorf("must be 0, got %d", sum)
	}
}

func TestEnqueue(t *testing.T) {
	skipQueueTest(t)
	tearupQueueTest(t)
	par := "par" + ID.New()
	var wg sync.WaitGroup
	for i := 0; i< 6; i++ {
		wg.Add(1)
		go func() {
			for q := 0; q < 1000; q++ {
				queue.Enqueue(par, "queue=" + strconv.Itoa(q) + ID.New(), strconv.Itoa(q))
			}
			wg.Done()
		}()
	}
	wg.Wait()
}
