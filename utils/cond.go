package utils

import (
	"sync"
	"time"
)

type Cond struct {
	L      sync.Locker
	signal chan bool
}

func (c *Cond) Signal() {
	select {
	case c.signal <- true:
	default:
	}
}

func (c *Cond) Broadcast() {
	for {
		select {
		case c.signal <- true:
		default:
			return
		}
	}
}

func (c *Cond) Wait() {
	c.L.Unlock()
	defer c.L.Lock()
	<-c.signal
}

var timerPool = sync.Pool{
	New: func() interface{} {
		return time.NewTimer(time.Second)
	},
}

func (c *Cond) WaitWithTimeout(d time.Duration) bool {
	c.L.Unlock()
	t := timerPool.Get().(*time.Timer)
	t.Reset(d)
	defer func() {
		t.Stop()
		timerPool.Put(t)
	}()
	defer c.L.Lock()
	select {
	case <-c.signal:
		return false
	case <-t.C:
		return true
	}
}

func NewCond(lock sync.Locker) *Cond {
	return &Cond{lock, make(chan bool, 1)}
}
