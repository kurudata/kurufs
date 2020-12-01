package chunk

import "sync"

type prefetcher struct {
	sync.Mutex
	pending chan string
	busy    map[string]bool
	op      func(key string)
}

func newPrefetcher(parallel int, fetch func(string)) *prefetcher {
	p := &prefetcher{
		pending: make(chan string, 10),
		busy:    make(map[string]bool),
		op:      fetch,
	}
	for i := 0; i < parallel; i++ {
		go p.do()
	}
	return p
}

func (p *prefetcher) do() {
	for key := range p.pending {
		p.Lock()
		if _, ok := p.busy[key]; !ok {
			p.busy[key] = true
			p.Unlock()

			p.op(key)

			p.Lock()
			delete(p.busy, key)
		}
		p.Unlock()
	}
}

func (p *prefetcher) fetch(key string) {
	select {
	case p.pending <- key:
	default:
	}
}
