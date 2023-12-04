package gomemq

import "fmt"

var (
	errInvalidTopic = fmt.Errorf("gomemq: invalid topic")
	errTopicExists  = fmt.Errorf("gomemq: topic already exists")
)
