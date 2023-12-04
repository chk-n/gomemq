package gomemq

import (
	"sync"
	"testing"
	"time"

	"github.com/chk-n/retry"
)

func TestTopicAll(t *testing.T) {
	var tests = []struct {
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

			for i := 0; i < tt.subscriberCount; i++ {
				topic.Subscribe(mockMessageHandler)
			}

			for i := 0; i < tt.messageCount; i++ {
				topic.Publish([]byte("test message"))
			}

			// TODO: add checks here
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
