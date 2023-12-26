package gomemq

import (
	"math"
	"sync"
	"sync/atomic"
)

type topicAll struct {
	cfg    ConfigTopic
	subs   map[string]interface{}
	hs     []MessageHandler
	hsLock sync.RWMutex
	rb     *RingBuffer[message]
	// dead letter queue contains failed messages
	dlq     []dlq
	dlqLock sync.RWMutex
	retry   retrier
	// ids to callback to
	cback        map[string]*Context
	cbackTrigger map[string]atomic.Int64
}

func newTopicAll(retry retrier, cfg ConfigTopic) *topicAll {
	rb, _ := NewRingBuffer[message](1_024)
	return &topicAll{
		cfg:          cfg,
		rb:           rb,
		dlq:          []dlq{},
		retry:        retry,
		cback:        make(map[string]*Context),
		cbackTrigger: make(map[string]atomic.Int64),
	}
}

// start topic manager
func (t *topicAll) manage() {
	sem := make(chan interface{}, t.cfg.MaxConcurrentMessages)
	for {
		msgs := t.rb.PopN(math.MaxInt64)
		if len(msgs) == 0 {
			continue
		}

		for i := 0; i < len(msgs); i++ {
			if t.cfg.MaxConcurrentMessages == 1 {
				t.notify(msgs[i])
			} else {
				sem <- struct{}{}
				i := i
				go func() {
					t.notify(msgs[i])
					<-sem
				}()
			}
		}
	}
}

func (t *topicAll) Subscribe(handler MessageHandler) {
	if handler == nil {
		return
	}
	ptr := getHandlerPointer(handler)
	if _, ok := t.subs[ptr]; ok {
		return
	}

	t.hsLock.Lock()
	defer t.hsLock.Unlock()

	t.hs = append(t.hs, handler)
}

func (t *topicAll) Publish(msg []byte) {
	id, err := generateUid()
	if err != nil {
		panic(err)
	}
	t.rb.Put(message{
		id:   id,
		data: msg,
	})
}

// Publish with receiving an ackknowledgement
func (t *topicAll) PublishDone(msg []byte) *Context {
	id, err := generateUid()
	if err != nil {
		panic(err)
	}
	c := &Context{
		ch:  make(chan any),
		cnt: 1,
	}

	t.cback[id] = c

	t.rb.Put(message{
		id:   id,
		data: msg,
	})
	return c
}

func (t *topicAll) PublishBatch(msgs [][]byte) {
	id, err := generateUid()
	if err != nil {
		panic(err)
	}
	t.putN(id, msgs)
}

// Publish with receiving an ackknowldgement when all msgs in batch delivered
func (t *topicAll) PublishBatchDone(msgs [][]byte) *Context {
	id, err := generateUid()
	if err != nil {
		panic(err)
	}
	c := &Context{
		ch:  make(chan any),
		cnt: int64(len(msgs)),
	}
	t.cback[id] = c
	t.putN(id, msgs)
	return c
}

func (t *topicAll) putN(id string, msgs [][]byte) {
	ms := make([]message, 0, len(msgs))
	for _, m := range msgs {
		ms = append(ms, message{
			id:   id,
			data: m,
		})
	}
	t.rb.PutN(ms)
}

func (t *topicAll) notify(msg message) {
	sem := make(chan interface{}, t.cfg.MaxConcurrentSubscribers)
	for i := 0; i < len(t.subs); i++ {
		sem <- struct{}{}
		handler := t.hs[i]
		// If canceled return
		if t.isCanceled(msg) {
			return
		}
		go func() {
			if err := t.retry.DoTimeout(t.cfg.MessageTimeout, func() error {
				// If canceled return
				if t.isCanceled(msg) {
					return nil
				}
				if err := handler(msg.data); err != nil {
					return err
				}
				// Update context that message was delivered successfully
				if c, ok := t.cback[msg.id]; ok {
					c.done()
				}
				return nil
			}); err != nil {
				d := dlq{
					HandlerPtr: getHandlerPointer(handler),
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
	if c, ok := t.cback[msg.id]; ok {
		if c.canceled() {
			return true
		}
	}
	return false
}
