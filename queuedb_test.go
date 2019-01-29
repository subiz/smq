package smq

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

// must create db first
func TestQueue(t *testing.T) {
	var NPAR = 5
	db, err := NewQueueDB([]string{"dev.subiz.net:9042"}, "smqtest")
	if err != nil {
		panic(err)
	}

	queue := fmt.Sprintf("%d", time.Now().UnixNano())

	wg := &sync.WaitGroup{}
	for par := 0; par < NPAR; par++ {
		wg.Add(1)
		go func(par string) {
			defer wg.Done()
			var lastoffset int64
			for j := 0; j < 1500; j++ {
				var err error
				lastoffset, err = db.Enqueue(par+queue, []byte(fmt.Sprintf("%d", j)))
				if err != nil {
					panic(err)
				}
			}

			if lastoffset != 1500 {
				t.Errorf("%s: should be 1000, got %d", par, lastoffset)
			}
		}(fmt.Sprintf("%d", par))
	}

	wg.Wait()
	for i := 0; i < NPAR; i++ {
		par := fmt.Sprintf("%d", i)
		values, offset, err := db.Fetch(par + queue)
		if err != nil {
			panic(err)
		}
		for i, v := range values {
			if string(v) != fmt.Sprintf("%d", i) {
				t.Fatalf("expect %d, got %s", i, v)
			}
		}

		// without commit
		values, offset, err = db.Fetch(par + queue)
		if err != nil {
			panic(err)
		}
		for i, v := range values {
			if string(v) != fmt.Sprintf("%d", i) {
				t.Fatalf("expect %d, got %s", i, v)
			}
		}
		if err := db.Commit(par+queue, offset); err != nil {
			t.Fatal(err)
		}

		lastlen := len(values)
		// without commit
		values, offset, err = db.Fetch(par + queue)
		if err != nil {
			panic(err)
		}
		if offset != 1500 {
			t.Fatalf("expect 1500, got %d", offset)
		}
		for i, v := range values {
			if string(v) != fmt.Sprintf("%d", i+lastlen) {
				t.Fatalf("expect %d, got %s", i+lastlen, v)
			}
		}
	}
}
