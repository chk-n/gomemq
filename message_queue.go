package gomemq

import (
	"fmt"
	"sync"
	"time"
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
	cfg       Config
	topics    map[string]topic
	topicLock sync.RWMutex
}

func New(cfg Config) *messageQueue {
	return &messageQueue{
		topics: make(map[string]topic),
	}
}

// Creates new topic
func (m *messageQueue) Topic(cfg ConfigTopic) (topic, error) {
	if cfg.Name == "" {
		return nil, fmt.Errorf("%s: topic name empty", errInvalidTopic)
	}

	t := selectTopic(m.cfg.Retry, cfg)
	m.topicLock.Lock()
	if m.topicExists(cfg.Name) {
		return nil, errTopicExists
	}
	m.topics[cfg.Name] = t
	m.topicLock.Unlock()

	go t.manage()

	return t, nil
}

// Join existing topic
func (m *messageQueue) Join(topic string) (topic, error) {
	m.topicLock.Lock()
	defer m.topicLock.Unlock()

	if t, ok := m.topics[topic]; ok {
		return t, nil
	}
	return nil, errInvalidTopic
}

func (m *messageQueue) Subscribe(topic string, handler MessageHandler) error {
	m.topicLock.Lock()
	defer m.topicLock.Unlock()

	if m.topicExists(topic) {
		return errTopicExists
	}
	m.topics[topic].Subscribe(handler)
	return nil
}

func (m *messageQueue) Publish(topic string, msg []byte) error {
	m.topicLock.Lock()
	defer m.topicLock.Unlock()

	if m.topicExists(topic) {
		return errTopicExists
	}
	m.topics[topic].Publish(msg)
	return nil
}

func (m *messageQueue) topicExists(topic string) bool {
	_, ok := m.topics[topic]
	return ok
}
