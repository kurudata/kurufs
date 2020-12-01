package chunk

import (
	"strconv"
	"sync"
	"testing"
	"time"
)

func TestSigleFlight(t *testing.T) {
	g := &Group{}
	gp := &sync.WaitGroup{}
	for i := 0; i < 100000; i++ {
		gp.Add(1)
		go func(k int) {
			p, _ := g.Do(strconv.Itoa(k/1000), func() (*Page, error) {
				time.Sleep(time.Microsecond * 1000)
				return NewOffPage(100), nil
			})
			p.Release()
			gp.Done()
		}(i)
	}
	gp.Wait()
}
