package gomemq

import "time"

// Interfaces

type retrier interface {
	Do(func() error) error
	DoTimeout(time.Duration, func() error) error
}

type topic interface {
	manage()
	Subscribe(handler MessageHandler)
	Publish(msg []byte)
}

// Types

type MessageHandler func(msg []byte) error

type PublishMethod func(hs []MessageHandler, msg []byte)

type PublishPolicy uint8

type dlq struct {
	HandlerPtr string
	Msg        []byte
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

type message struct {
	id   string
	data []byte
}
