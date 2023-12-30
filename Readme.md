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

// Set up message queue
cfg := gomemq.Config{
  // retrier
}
mq := gomemq.New(cfg)

// Create topic
cfgTopic := gomemq.ConfigTopic{
  // memory consumption
  // concurrency control
  // other topic configurations
}
t,_ := gomemq.NewTopic[CustomType](cfgTopic)

// Publish to topic
var msg CustomType
t.Publish(msg)
// equivalent to gomemq.Publish("topic", msg)

// Subscribe to topic
t.Subscribe(func(msg CustomType) error {
  // handle message asychnronously
})

// Join existing topic  
t,_ = gomemq.Join("topic")

// Batch publish with ACK and DONE acknowledgement
ctx := gomemq.PublishBatchDone[CustomType]("topic", []CustomType{msg, msg})

select {
  case <-ctx.Ack():
    // message was acknowledged
  case <-ctx.WithAckTimeout(3 * time.Second):
    // message was not acknowledged within 3 seconds of publishing
  case <-ctx.WithDoneTimeout(2 * time.Minute):
    // message was not successfully processed within 2 minutes of ACK
  case <-ctx.Done():
    // yay, message was successfully processed
}
```
