package gomemq

import "time"

// Interfaces

type retrier interface {
	Do(func() error) error
	DoTimeout(time.Duration, func() error) error
}

type topic[T any] interface {
	manage()
	Subscribe(handler MessageHandler[T])
	Publish(msg T)
	PublishBatchDone(msgs []T) *Context
}

// Types

type MessageHandler[T any] func(msg T) error

type PublishMethod[T any] func(hs []MessageHandler[T], msg T)

type PublishPolicy uint8

type dlq[T any] struct {
	HandlerPtr string
	Msg        T
	Err        error
}

// Constants

const (
	// all subscribers receive message
	all PublishPolicy = iota
	// single subscriber receives message
	roundRobin
	// random subscriber receives message
	random
)

type message[T any] struct {
	id   string
	data T
}
