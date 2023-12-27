package gomemq

import (
	"math"
	"sync"
	"sync/atomic"
)

type topicAll[T any] struct {
	cfg ConfigTopic
	// used for duplicate check
	subs map[string]any
	// contains all message handlers (i.e. subscribers)
	hs     []MessageHandler[T]
	hsLock sync.RWMutex
	rb     *RingBuffer[message[T]]
	// dead letter queue contains failed messages
	dlq     []dlq[T]
	dlqLock sync.RWMutex

	// Interface to define custom retry policy
	retry retrier

	// tracks message id with context so we can notify publisher when subscirber processed message successfully
	cback        *FastMap[string, *Context]
	cbackTrigger *FastMap[string, atomic.Int64]
}

func newTopicAll[T any](retry retrier, cfg ConfigTopic) *topicAll[T] {
	rb, _ := NewRingBuffer[message[T]](1_024)
	return &topicAll[T]{
		cfg:          cfg,
		rb:           rb,
		dlq:          []dlq[T]{},
		retry:        retry,
		cback:        NewFM[*Context](),
		cbackTrigger: NewFM[atomic.Int64](),
	}
}

// start topic manager
func (t *topicAll[T]) manage() {
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

func (t *topicAll[T]) Subscribe(handler MessageHandler[T]) {
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

// TODO add way to unsubscribe

func (t *topicAll[T]) Publish(msg T) {
	//m := makeCopy[T](msg)

	id, err := generateUid()
	if err != nil {
		panic(err)
	}
	t.rb.Put(message[T]{
		id:   id,
		data: msg,
	})
}

// Publish with receiving an ackknowledgement
func (t *topicAll[T]) PublishDone(msg T) *Context {
	// m := makeCopy[byte](msg)

	id, err := generateUid()
	if err != nil {
		panic(err)
	}
	c := &Context{
		ch:      make(chan any),
		trigger: 1,
	}
	t.cback.Set(id, c)

	t.rb.Put(message[T]{
		id:   id,
		data: msg,
	})
	return c
}

// Publishes multiple messages to subscribers
func (t *topicAll[T]) PublishBatch(msgs []T) {
	id, err := generateUid()
	if err != nil {
		panic(err)
	}
	t.putN(id, msgs)
}

// Publish with receiving an ackknowldgement when all msgs in batch processed by subscriber without error
func (t *topicAll[T]) PublishBatchDone(msgs []T) *Context {
	id, err := generateUid()
	if err != nil {
		panic(err)
	}
	c := &Context{
		ch:      make(chan any),
		trigger: int64(len(msgs)),
	}
	t.cback.Set(id, c)

	t.putN(id, msgs)
	return c
}

// Makes copy of data and stores messages in buffer
func (t *topicAll[T]) putN(id string, msgs []T) {
	msgsCopy := makeCopy[T](msgs)
	ms := make([]message[T], 0, len(msgs))
	for _, m := range msgsCopy {
		ms = append(ms, message[T]{
			id:   id,
			data: m,
		})
	}
	t.rb.PutN(ms)
}

// Copy of message passed to notify
func (t *topicAll[T]) notify(msg message[T]) {
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
				d := dlq[T]{
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
func (t *topicAll[T]) isCanceled(msg message[T]) bool {
	if c, ok := t.cback.Get(msg.id); ok {
		if c.canceled() {
			return true
		}
	}
	return false
}
