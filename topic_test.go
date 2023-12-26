package gomemq

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/chk-n/retry"
	"github.com/stretchr/testify/assert"
)

func TestTopicAll(t *testing.T) {
	tests := []struct {
		name              string
		maxConcurrentMsg  uint
		maxConcurrentSubs uint
		messageCount      int
		subscriberCount   int
		expectedError     bool
	}{
		{"no_subscribers", 10, 10, 5, 0, false},
		{"no_messages", 10, 10, 0, 5, false},
		{"max_concurrent_messages", 1, 10, 10, 5, false},
		{"max_concurrent_subscribers", 10, 1, 5, 10, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			topic := newTopicAll(retry.NewDefault(), ConfigTopic{
				MaxConcurrentMessages:    tt.maxConcurrentMsg,
				MaxConcurrentSubscribers: tt.maxConcurrentSubs,
				MessageTimeout:           10 * time.Second,
			})

			go topic.manage()

			var cnt int64
			for i := 0; i < tt.subscriberCount; i++ {
				topic.Subscribe(func(msg []byte) error {
					atomic.AddInt64(&cnt, 1)
					return nil
				})
			}

			for i := 0; i < tt.messageCount; i++ {
				topic.Publish([]byte("test message"))
			}

			time.Sleep(50 * time.Millisecond)

			assert.Equal(t, tt.subscriberCount*tt.messageCount, int(cnt))
		})
	}
}

func TestTopicAllPublishDone(t *testing.T) {
	// tests := []struct {
	// 	name string
	// }
}

func TestTopicAllPublishBatch(t *testing.T) {
	tests := []struct {
		name string
		msgs [][]byte
	}{
		{
			name: "empty",
			msgs: [][]byte{},
		},
		{
			name: "single",
			msgs: [][]byte{[]byte("1")},
		},
		{
			name: "multi",
			msgs: [][]byte{[]byte("1"), []byte("2"), []byte("3")},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			topic := newTopicAll(retry.NewDefault(), ConfigTopic{
				MaxConcurrentMessages:    10,
				MaxConcurrentSubscribers: 10,
				MessageTimeout:           10 * time.Second,
			})

			go topic.manage()

			var cnt int64
			topic.Subscribe(func(msg []byte) error {
				atomic.AddInt64(&cnt, 1)
				return nil
			})

			topic.PublishBatch(tt.msgs)

			time.Sleep(50 * time.Millisecond)

			assert.Equal(t, len(tt.msgs), int(cnt))
		})
	}
}

func TestTopicAllPublishBatchDone(t *testing.T) {
	// TODO: add tests for Cancel()
	tests := []struct {
		name string
		msgs [][]byte
	}{
		{
			name: "empty",
			msgs: [][]byte{},
		},
		{
			name: "single",
			msgs: [][]byte{[]byte("1")},
		},
		{
			name: "multi",
			msgs: [][]byte{[]byte("1"), []byte("2"), []byte("3")},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			topic := newTopicAll(retry.NewDefault(), ConfigTopic{
				MaxConcurrentMessages:    10,
				MaxConcurrentSubscribers: 10,
				MessageTimeout:           10 * time.Second,
			})

			go topic.manage()

			var cnt int64
			topic.Subscribe(func(msg []byte) error {
				atomic.AddInt64(&cnt, 1)
				return nil
			})

			ctx := topic.PublishBatchDone(tt.msgs)

			time.Sleep(50 * time.Millisecond)

			assert.Equal(t, len(tt.msgs), int(cnt))

			select {
			case <-ctx.Done():
			case <-time.After(200 * time.Millisecond):
				t.Error("ctx.Done() timedout")

			}
		})
	}
}

func TestConcurrencyOnSubscribeAndPublish(t *testing.T) {
	topic := newTopicAll(retry.NewDefault(), ConfigTopic{
		MaxConcurrentMessages:    10,
		MaxConcurrentSubscribers: 10,
		MessageTimeout:           10 * time.Second,
	})

	go topic.manage()

	var wg sync.WaitGroup
	subscriberCount := 100
	messageCount := 100

	wg.Add(subscriberCount)
	for i := 0; i < subscriberCount; i++ {
		go func() {
			defer wg.Done()
			topic.Subscribe(mockMessageHandler)
		}()
	}
	wg.Wait()

	wg.Add(messageCount)
	for i := 0; i < messageCount; i++ {
		go func(msg []byte) {
			defer wg.Done()
			topic.Publish(msg)
		}([]byte("test message"))
	}
	wg.Wait()

	// TODO: add check here

}

// Test utils

func mockMessageHandler(msg []byte) error {
	return nil
}
