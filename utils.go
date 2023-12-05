package gomemq

import (
	"fmt"
	"reflect"
)

// returns topic type based on config
func selectTopic(r retrier, cfg ConfigTopic) topic {
	switch cfg.PublishPolicy {
	default:
		return newTopicAll(r, cfg)
	}
}

func getHandlerPointer(m MessageHandler) string {
	return fmt.Sprintf("%d", reflect.ValueOf(m).Pointer())
}

func makeCopy[T any](s []T) []T {
	new := make([]T, len(s))
	copy(new, s)
	return new
}
