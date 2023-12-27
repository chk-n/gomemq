package gomemq

import (
	"math"
	"sync"
	"sync/atomic"
)

type topicAll struct {
	cfg ConfigTopic
	// used for duplicate check
	subs   map[string]any
	hs     []MessageHandler
	hsLock sync.RWMutex
	rb     *RingBuffer[message]
	// dead letter queue contains failed messages
	dlq     []dlq
	dlqLock sync.RWMutex

	// Interface to define custom retry policy
	retry retrier

	// ids to callback to
	cback        *FastMap[string, *Context]
	cbackTrigger *FastMap[string, atomic.Int64]
}

func newTopicAll(retry retrier, cfg ConfigTopic) *topicAll {
	rb, _ := NewRingBuffer[message](1_024)
	return &topicAll{
		cfg:          cfg,
		rb:           rb,
		dlq:          []dlq{},
		retry:        retry,
		cback:        NewFM[*Context](),
		cbackTrigger: NewFM[atomic.Int64](),
	}
}

// start topic manager
func (t *topicAll) manage() {
	sem := make(chan interface{}, t.cfg.MaxConcurrentMessages)
	for {
		msgs := t.rb.PopN(math.MaxInt64)
		if len(msgs) == 0 {
			// runtime.Gosched()
			continue
		}

		for i := 0; i < len(msgs); i++ {
			if t.cfg.MaxConcurrentMessages == 1 {
				t.notify(msgs[i])
				continue
			}
			sem <- struct{}{}
			i := i
			go func() {
				t.notify(msgs[i])
				<-sem
			}()
		}
	}
}

func (t *topicAll) Subscribe(handler MessageHandler) {
	if handler == nil {
		return
	}

	// BUG not thread safe!
	// check duplicates
	ptr := getHandlerPointer(handler)
	if _, ok := t.subs[ptr]; ok {
		return
	}

	t.hsLock.Lock()
	defer t.hsLock.Unlock()

	t.hs = append(t.hs, handler)
}

func (t *topicAll) Publish(msg []byte) {
	m := makeCopy[byte](msg)

	id, err := generateUid()
	if err != nil {
		panic(err)
	}
	t.rb.Put(message{
		id:   id,
		data: m,
	})
}

// Publish with receiving an ackknowledgement
func (t *topicAll) PublishDone(msg []byte) *Context {
	m := makeCopy[byte](msg)

	id, err := generateUid()
	if err != nil {
		panic(err)
	}
	c := &Context{
		ch:  make(chan any),
		cnt: 1,
	}
	t.setCtx(id, c)

	t.rb.Put(message{
		id:   id,
		data: m,
	})
	return c
}

// Publishes multiple messages to subscribers
func (t *topicAll) PublishBatch(msgs [][]byte) {
	id, err := generateUid()
	if err != nil {
		panic(err)
	}
	t.putN(id, msgs)
}

// Publish with receiving an ackknowldgement when all msgs in batch processed by subscriber without error
func (t *topicAll) PublishBatchDone(msgs [][]byte) *Context {
	id, err := generateUid()
	if err != nil {
		panic(err)
	}
	c := &Context{
		ch:  make(chan any),
		cnt: int64(len(msgs)),
	}
	t.setCtx(id, c)

	t.putN(id, msgs)
	return c
}

// Makes copy of data and stores messages in buffer
func (t *topicAll) putN(id string, msgs [][]byte) {
	msgsCopy := makeCopy[[]byte](msgs)
	ms := make([]message, 0, len(msgs))
	for _, m := range msgsCopy {
		ms = append(ms, message{
			id:   id,
			data: m,
		})
	}
	t.rb.PutN(ms)
}

func (t *topicAll) notify(msg message) {
	sem := make(chan interface{}, t.cfg.MaxConcurrentSubscribers)
	for _, h := range t.hs {
		h := h
		// If canceled return
		if t.isCanceled(msg) {
			return
		}

		sem <- struct{}{}
		go func() {
			if err := t.retry.DoTimeout(t.cfg.MessageTimeout, func() error {
				// BUG data race occurs when reading msg (although its never edited by any other thread)

				// If canceled return
				if t.isCanceled(msg) {
					return nil
				}
				if err := h(msg.data); err != nil {
					return err
				}
				// Update context that message was delivered successfully
				if c, ok := t.cback.Get(msg.id); ok {
					c.done()
					// BUG memory leak in cback (need to add way to clean map up)
				}
				return nil
			}); err != nil {
				d := dlq{
					HandlerPtr: getHandlerPointer(h),
					Msg:        msg.data,
					Err:        err,
				}
				t.dlqLock.Lock()
				t.dlq = append(t.dlq, d)
				t.dlqLock.Unlock()
			}
			<-sem
		}()
	}
}

// checks whether a message was cancelled
func (t *topicAll) isCanceled(msg message) bool {
	if c, ok := t.cback.Get(msg.id); ok {
		if c.canceled() {
			return true
		}
	}
	return false
}

// safely set ctx
func (t *topicAll) setCtx(id string, ctx *Context) {
	t.cback.Set(id, ctx)
}
