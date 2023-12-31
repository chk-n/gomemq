package gomemq

import (
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"reflect"
	"time"
)

// returns topic type based on config
func selectTopic[T any](r retrier, cfg ConfigTopic) topic[T] {
	switch cfg.PublishPolicy {
	default:
		return newTopicAll[T](r, cfg)
	}
}

func getHandlerPointer[T any](m MessageHandler[T]) string {
	return fmt.Sprintf("%d", reflect.ValueOf(m).Pointer())
}

func makeCopy[T any](s []T) []T {
	new := make([]T, len(s))
	copy(new, s)
	return new
}

func generateUid() (string, error) {
	tstamp := time.Now().UnixNano()

	timestampBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(timestampBytes, uint64(tstamp))

	// Generate random bytes
	randomBytes := make([]byte, 16)
	if _, err := rand.Read(randomBytes); err != nil {
		return "", err
	}

	ub := append(randomBytes, timestampBytes...)
	uid := fmt.Sprintf("%s", hex.EncodeToString(ub))

	return uid, nil
}
