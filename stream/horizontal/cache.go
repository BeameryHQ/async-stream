package horizontal

import (
	"github.com/BeameryHQ/async-stream/stream"
	"sync"
)

type flowCache struct {
	mu *sync.RWMutex
	// currently it doesn't preserve the order might be a problem
	// for apps that expect that
	items map[string]*stream.FlowEvent
}

func newFlowCache() *flowCache {
	return &flowCache{
		mu:    &sync.RWMutex{},
		items: map[string]*stream.FlowEvent{},
	}
}

func (c *flowCache) put(key string, event *stream.FlowEvent) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.items[key] = event
}

func (c *flowCache) del(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.items, key)
}

func (c *flowCache) get(key string) *stream.FlowEvent {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.items[key]
}

// this is a sync iterate which is good for cases where need to re-process the
// whole cache again, it's important the flow is consumed and locked for writing
// and ok for reading
func (c *flowCache) iterate() chan *stream.FlowEvent {
	ch := make(chan *stream.FlowEvent)
	go func() {
		c.mu.RLock()
		defer c.mu.RUnlock()
		defer close(ch)

		for _, v := range c.items {
			ch <- v
		}
	}()

	return ch
}