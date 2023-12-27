package gomemq

import "sync/atomic"

type Context struct {
	ch      chan any
	trigger int64
	// tracks how many times done() was called (lowercase done)
	cnt    int64
	cancel int32
}

func (c *Context) Done() chan any {
	return c.ch
}

// Cancels message or grouped message. Multiple calls to this function wont cause errors
func (c *Context) Cancel() {
	atomic.SwapInt32(&c.cancel, 1)
	close(c.ch)
}

func (c *Context) done() {
	v := atomic.AddInt64(&c.cnt, 1)
	if v >= c.trigger && !c.canceled() {
		c.ch <- struct{}{}
	}
}

func (c *Context) canceled() bool {
	return atomic.LoadInt32(&c.cancel) == 1
}
