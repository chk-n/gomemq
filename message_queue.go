package gomemq

import (
	"fmt"
	"sync"
	"time"
)

var (
	mq *messageQueue
	mu sync.RWMutex
)

type Config struct {
	Retry retrier
}

type ConfigTopic struct {
	// name of the topic, used to join topics by name
	Name string
	// Maximum cache size (number of messages) a subscriber can have before old messages get overwritten
	MaxSubscriberDelayCache uint8
	// Time duration before subscriber considered timedout
	MessageTimeout time.Duration
	// Maximum size of a message
	MaxMessageSize uint
	// Maximum concurrent message processing, setting this to above 1 can lead to out of order message handling
	MaxConcurrentMessages uint
	// Maximum number of concurrent subscribers processing a message
	MaxConcurrentSubscribers uint
	// defines how messages should be Published. Default: all
	PublishPolicy PublishPolicy
	// Keep message order in topic
	KeepOrder bool
	// Continue notifying subscribers even if retries have failed for previous subscribers
	ForceNotify bool
}

type messageQueue struct {
	cfg    Config
	topics *FastMap[string, topic[any]]
}

func New(cfg Config) *messageQueue {
	if mq == nil {
		mu.Lock()
		if mq != nil {
			return mq
		}
		mq = &messageQueue{
			topics: NewFM[topic[any]](),
		}
		mu.Unlock()
	}
	return mq
}

// create new topic
func NewTopic[T any](cfg ConfigTopic) (topic[T], error) {
	if cfg.Name == "" {
		return nil, fmt.Errorf("%s: topic name empty", errInvalidTopic)
	}

	mq := getInst()

	t := selectTopic[T](mq.cfg.Retry, cfg)
	if mq.topics.Exists(cfg.Name) {
		return nil, errTopicExists
	}
	mq.topics.Set(cfg.Name, t.(topic[any]))

	go t.manage()

	return t, nil
}

// Join a preexisting topic
func Join[T any](name string) (topic[T], error) {
	mq := getInst()

	tc, ok := mq.topics.Get(name)
	if !ok {
		return nil, fmt.Errorf("%s: topic not found", errInvalidTopic)
	}

	t, ok := tc.(topic[T])
	if !ok {
		return nil, fmt.Errorf("%s: topic of invalid type, got %T", errInvalidTopic, tc)
	}
	return t, nil
}

func getInst() *messageQueue {
	return mq
}
