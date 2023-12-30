# Gomemq

Lightweight and concurrent in-memory message queue written in golang with zero third-party dependencies*


\*Excluding packages used for tests

## Features

* Fast and lightweight
* Zero third party dependencies
* Type safe
* No serialisation
* Batch publishing
* Receive ACK after message received by all subscribers
* Receive 'DONE' after all subscribers successfully processed message(s)

## Get

`go get github.com/chk-n/gomemq`

## Example

```go

cfg := gomemq.Config{
  // retrier
}
mq := gomemq.New(cfg)

// create topic
cfgTopic := gomemq.ConfigTopic{
  // memory consumption
  // concurrency control
  // other topic configurations
}
t,_ := gomemq.NewTopic[[]byte](cfgTopic)

// publish to topic
var msg []byte
t.Publish(msg)

// subscribe to topic
t.Subscribe(func(b []byte) error {
  // handle message asychnronously
})

// join a topic
t,_ = gomemq.Join("topic")
```
