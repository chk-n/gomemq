package gomemq

import (
	"math"
	"sync"
)

type topicAll struct {
	cfg    ConfigTopic
	subs   map[string]interface{}
	hs     []MessageHandler
	hsLock sync.RWMutex
	rb     *RingBuffer[[]byte]
	// dead letter queue contains failed messages
	dlq     []dlq
	dlqLock sync.RWMutex
	retry   retrier
}

func newTopicAll(retry retrier, cfg ConfigTopic) *topicAll {
	rb, _ := NewRingBuffer[[]byte](1_024)
	return &topicAll{
		cfg:   cfg,
		rb:    rb,
		dlq:   []dlq{},
		retry: retry,
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
	t.rb.Put(msg)
}

func (t *topicAll) notify(msg []byte) {
	sem := make(chan interface{}, t.cfg.MaxConcurrentSubscribers)
	for i := 0; i < len(t.subs); i++ {
		sem <- struct{}{}
		handler := t.hs[i]
		go func() {
			if err := t.retry.DoTimeout(t.cfg.MessageTimeout, func() error {
				return handler(msg)
			}); err != nil {
				d := dlq{
					HandlerPtr: getHandlerPointer(handler),
					Msg:        msg,
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
