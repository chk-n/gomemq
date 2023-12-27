package gomemq

import (
	"sync/atomic"
	"time"
)

type Context struct {
	// ack channel, informs publisher all subscribers received message
	ackCh chan any
	// required ackCnt before broadcast to ackCh
	ackTrgr int64
	// tracks # of calls to ack()
	ackCnt int64
	// ack timeout channel
	ackToCh chan any
	// done channel, informs publishers all subscribers processed message
	doneCh chan any
	// required doneCnt before broadcasted to doneCh
	doneTrgr int64
	// tracks # of calls to done()
	doneCnt int64
	// done timeout channel
	doneToCh chan any
	// internal channel for done to know when all subscribers acknowledged message
	doneAckCh chan any
	// keeps track if message was cancelled, cancel == 0 => false, cancel >=1 => true
	cancel int32
}

func NewContext(trigger int64) *Context {
	return &Context{
		ackCh:     make(chan any),
		ackTrgr:   trigger,
		ackToCh:   make(chan any),
		doneCh:    make(chan any),
		doneTrgr:  trigger,
		doneToCh:  make(chan any),
		doneAckCh: make(chan any),
	}
}

func (c *Context) Ack() <-chan any {
	return c.ackCh
}

// Infoms you when messages were not acknowledged by subscribers in time.
//
// NOTE: Ensure to call this early on as timer does NOT start when message published
func (c *Context) WithAckTimeout(d time.Duration) <-chan any {
	go func() {
		select {
		case <-time.After(d):
			c.ackToCh <- struct{}{}
		}
	}()
	return c.ackToCh
}

func (c *Context) Done() <-chan any {
	return c.doneCh
}

// Informs you when messages were not processed by subscribers in time
func (c *Context) WithDoneTimeout(d time.Duration) <-chan any {
	go func() {
		select {
		case <-c.doneAckCh:
			select {
			case <-time.After(d):
				c.doneToCh <- struct{}{}
			}
		}
	}()

	return c.doneToCh
}

// Cancels message or grouped message. Multiple calls to this function wont cause errors
func (c *Context) Cancel() {
	atomic.SwapInt32(&c.cancel, 1)
	close(c.ackCh)
	close(c.doneCh)
	close(c.ackToCh)
	close(c.doneToCh)
	close(c.doneAckCh)
}

func (c *Context) done() {
	v := atomic.AddInt64(&c.doneCnt, 1)
	if v == c.doneTrgr && !c.canceled() {
		c.doneCh <- struct{}{}
	}
}

func (c *Context) ack() {
	v := atomic.AddInt64(&c.ackCnt, 1)
	if v == c.ackTrgr && !c.canceled() {
		c.ackCh <- struct{}{}
		c.doneAckCh <- struct{}{}
	}
}

func (c *Context) canceled() bool {
	return atomic.LoadInt32(&c.cancel) == 1
}
