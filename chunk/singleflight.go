/*
Copyright 2012 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package chunk

import "sync"

// call is an in-flight or completed Do call
type call struct {
	wg  sync.WaitGroup
	val *Page
	ref int
	err error
}

// Group represents a class of work and forms a namespace in which
// units of work can be executed with duplicate suppression.
type Group struct {
	mu sync.Mutex       // protects m
	m  map[string]*call // lazily initialized
}

// Do executes and returns the results of the given function, making
// sure that only one execution is in-flight for a given key at a
// time. If a duplicate comes in, the duplicate caller waits for the
// original to complete and receives the same results.
func (g *Group) Do(key string, fn func() (*Page, error)) (*Page, error) {
	g.mu.Lock()
	if g.m == nil {
		g.m = make(map[string]*call)
	}
	if c, ok := g.m[key]; ok {
		c.ref++
		g.mu.Unlock()
		c.wg.Wait()
		c.val.Acquire()
		g.mu.Lock()
		c.ref--
		if c.ref == 0 {
			c.val.Release()
		}
		g.mu.Unlock()
		return c.val, c.err
	}
	c := new(call)
	c.wg.Add(1)
	c.ref++
	g.m[key] = c
	g.mu.Unlock()

	c.val, c.err = fn()
	c.val.Acquire()
	c.wg.Done()

	g.mu.Lock()
	c.ref--
	if c.ref == 0 {
		c.val.Release()
	}
	delete(g.m, key)
	g.mu.Unlock()

	return c.val, c.err
}
