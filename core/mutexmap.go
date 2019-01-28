package core

import (
	"fmt"
	"github.com/orcaman/concurrent-map"
	"sync"
)

type QueueMap struct {
	globalLock sync.Mutex
	qMap       map[string]chan interface{}
}

func (me *QueueMap) GetQueue(queue string) chan interface{} {
	me.globalLock.Lock()
	if me.qMap == nil {
		me.qMap = make(map[string]chan interface{})
	}
	c, ok := me.qMap[queue]
	if !ok {
		c = make(chan interface{})
		me.qMap[queue] = c
	}
	me.globalLock.Unlock()
	return c
}

// MutexMap store map of mutex
type MutexMap struct {
	gmutex   *sync.Mutex
	mutexMap cmap.ConcurrentMap
}

// QuickUnlock unlock lock
type QuickUnlock interface {
	Unlock()
}

type quickunlock struct {
	key string
	mm  *MutexMap
}

func (me *quickunlock) Unlock() {
	me.mm.Unlock(me.key)
}

// Init intialize new map
func NewMutexMap() *MutexMap {
	return &MutexMap{
		gmutex:   &sync.Mutex{},
		mutexMap: cmap.New(),
	}
}

// Lock locks key, this function don't panic
func (me *MutexMap) Lock(key string) QuickUnlock {
	var mutex *sync.Mutex
	buff, ok := me.mutexMap.Get(key)
	me.gmutex.Lock()
	if !ok {
		mutex = &sync.Mutex{}
		me.mutexMap.Set(key, mutex)
	} else {
		mutex, ok = buff.(*sync.Mutex)
		if !ok {
			mutex = &sync.Mutex{}
			me.mutexMap.Set(key, mutex)
		}
	}
	me.gmutex.Unlock()
	mutex.Lock()
	return &quickunlock{key, me}
}

// Unlock unlocks key, this function don't panic
func (me *MutexMap) Unlock(key string) {
	buff, ok := me.mutexMap.Get(key)
	if !ok {
		fmt.Println("2df25a trying to unlock unexisted lock")
		return
	}
	mutex, ok := buff.(*sync.Mutex)
	if !ok {
		fmt.Println("2fgaaR invalid mutex")
		return
	}
	mutex.Unlock()
}
