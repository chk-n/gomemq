// Inspired by https://github.com/orcaman/concurrent-map

package gomemq

import (
	"sync"
)

const SHARD_COUNT = 64

type FastMap[K comparable, V any] struct {
	shards  []*FastMapShard[K, V]
	shardfn func(k K) uint64
}

func NewFM[V any]() *FastMap[string, V] {
	fm := &FastMap[string, V]{
		shards:  make([]*FastMapShard[string, V], SHARD_COUNT),
		shardfn: fnv64,
	}
	for i := 0; i < SHARD_COUNT; i++ {
		fm.shards[i] = &FastMapShard[string, V]{m: make(map[string]V)}
	}
	return fm
}

func (m *FastMap[K, V]) Set(k K, v V) {
	shard := m.getShard(k)
	shard.Set(k, v)
}

func (m *FastMap[K, V]) Get(k K) (V, bool) {
	shard := m.getShard(k)
	return shard.Get(k)
}

func (m *FastMap[K, V]) Exists(k K) bool {
	shard := m.getShard(k)
	return shard.Exists(k)
}

func (m *FastMap[K, V]) getShard(k K) *FastMapShard[K, V] {
	return m.shards[m.shardfn(k)%SHARD_COUNT]
}

type FastMapShard[K comparable, V any] struct {
	m  map[K]V
	mu sync.RWMutex
}

func (s *FastMapShard[K, V]) Set(k K, v V) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.m[k] = v
}

func (s *FastMapShard[K, V]) Get(k K) (V, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	v, ok := s.m[k]
	return v, ok
}

func (s *FastMapShard[K, V]) Exists(k K) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	_, ok := s.m[k]
	return ok
}

// Helper functions

// https://en.wikipedia.org/wiki/Fowler–Noll–Vo_hash_function
func fnv64(key string) uint64 {
	hash := uint64(14695981039346656037)  // FNV offset basis for 64 bits
	const prime64 = uint64(1099511628211) // FNV prime for 64 bits

	keyLen := len(key)
	for i := 0; i < keyLen; i++ {
		hash *= prime64
		hash ^= uint64(key[i])
	}
	return hash
}
