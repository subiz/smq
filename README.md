# Simple message queue build on top of cassandra

# [![GoDoc](https://godoc.org/github.com/subiz/smq?status.svg)](http://godoc.org/github.com/subiz/smq)

Upside: You can store unlimit number of queues without performance degrading
Downside: slow, no build-in pull or push mechanic

# Example
### Creating a new queue
```go
	db, err := NewQueueDB([]string{"dev.subiz.net:9042"}, "smqtest")
```

### Send message to queue
```go
	// send "hello", "world" to queue "myqueue"
	offset, _ := db.Enqueue("myqueue", []byte{"hello"})
	fmt.Println(offset) // 1

	offset, _ = db.Enqueue("myqueue", []byte{"world"})
	fmt.Println(offset) // 2

	// read from queue
	values, offset, err := db.Fetch("myqueue")
	fmt.Println(values, offset) // [][]byte{"hello", "world"}, 2

	// commit
	db.Commit("myqueue", 2)
```
