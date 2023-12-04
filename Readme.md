# Gomemq



## Get

`go get github.com/chk-n/dash/pkg/gomemq`

## Example

```go

cfg := gomemq.Config{
  // timeout duratio
  // retry count
  // memory consumption
  // goroutine count
}
mq := memq.New(cfg)

// create topic
cfgTopic := gomemq.ConfigTopic{
  
}
t,_ := mq.Topic(cfgTopic)

// publish to topic
var msg []byte
t.Publish("", msg)
// handle error

// subscribe to topic
t.Subscribe(func(b []byte) error {
  // handle message asychnronously
})


// join a topic
t,_ = mq.Join("")

// publish through mq
mq.Publish("", msg)

mq.Subscribe("", func(b []byte) error {
  // handle message asynchronously
})
```


Note: This has no afiliation to pinterest's MemQ.
