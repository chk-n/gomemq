package gomemq

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewFM(t *testing.T) {
	fm := NewFM[int]()
	assert.Equal(t, SHARD_COUNT, len(fm.shards))
}

func TestSetAndGet(t *testing.T) {
	fm := NewFM[string]()
	testKey := "testKey"
	testValue := "testValue"

	fm.Set(testKey, testValue)
	value, ok := fm.Get(testKey)

	assert.True(t, ok)
	assert.Equal(t, testValue, value)
}

func TestExists(t *testing.T) {
	fm := NewFM[string]()
	testKey := "testKey"
	testValue := "testValue"

	fm.Set(testKey, testValue)

	assert.True(t, fm.Exists(testKey))
}

func TestConcurrentSet(t *testing.T) {
	cnt := 1000
	fm := NewFM[string]()
	for i := 0; i < cnt; i++ {
		kv := fmt.Sprintf("%d", i)
		go fm.Set(kv, kv)
	}

	// Verify
	for i := 0; i < cnt; i++ {
		k := fmt.Sprintf("%d", i)
		value, ok := fm.Get(k)
		assert.True(t, ok)
		assert.Equal(t, k, value)
	}
}

// TODO: add test for concurrent set and get
