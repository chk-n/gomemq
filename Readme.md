# Gomemq

Lightweight and concurrent in-memory message queue written in golang with zero third-party dependencies*


\*Excluding packages used for tests

## Get

`go get github.com/chk-n/gomemq`

## Example

```go

cfg := gomemq.Config{
  // retrier
}
mq := memq.New(cfg)

// create topic
cfgTopic := gomemq.ConfigTopic{
  // memory consumption
  // concurrency control
  // other topic configurations
}
t,_ := mq.Topic(cfgTopic)

// publish to topic
var msg []byte
t.Publish("", msg)

// subscribe to topic
t.Subscribe(func(b []byte) error {
  // handle message asychnronously
})

// join a topic
t,_ = mq.Join("")

// It is also possible to publish directly through mq

// publish through mq
mq.Publish("", msg)

mq.Subscribe("", func(b []byte) error {
  // handle message asynchronously
})
```
