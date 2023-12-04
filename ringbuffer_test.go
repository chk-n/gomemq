package gomemq

import (
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"

	// "sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type ringBufferTestCase struct {
	name             string
	capacity         uint64
	operation        func(r *RingBuffer) [][]byte
	concurrent       bool
	expectedState    [][]byte
	expectedReturn   [][]byte
	expectedCapacity uint64
	expectedLength   uint64
	expectedHead     uint64
	expectedTail     uint64
}

type ringBufferPopBenchCase struct {
	name     string
	capacity uint64
	setup    func(r *RingBuffer)
}

var (
	r = rand.New(rand.NewSource(1))
)

func TestRingBufferInit(t *testing.T) {
	_, err := NewRingBuffer(0)
	assert.Error(t, err)

	_, err = NewRingBuffer(1)
	assert.Error(t, err)

	_, err = NewRingBuffer(2)
	assert.NoError(t, err)

	_, err = NewRingBuffer(6)
	assert.Error(t, err)

}

func TestRingBuffer(t *testing.T) {
	testCases := []ringBufferTestCase{
		{
			name:     "put_one",
			capacity: 2,
			operation: func(r *RingBuffer) [][]byte {
				r.Put([]byte("1"))
				return nil
			},
			expectedState:    [][]byte{[]byte("1"), nil},
			expectedCapacity: 2,
			expectedLength:   1,
			expectedHead:     0,
			expectedTail:     1,
		},
		{
			name:     "put_many",
			capacity: 4,
			operation: func(r *RingBuffer) [][]byte {
				r.Put([]byte("1"))
				r.Put([]byte("2"))
				return nil
			},
			expectedState:    [][]byte{[]byte("1"), []byte("2"), nil, nil},
			expectedCapacity: 4,
			expectedLength:   2,
			expectedHead:     0,
			expectedTail:     2,
		},
		{
			name:     "put_more_than_cap",
			capacity: 2,
			operation: func(r *RingBuffer) [][]byte {
				r.Put([]byte("1"))
				r.Put([]byte("2"))
				r.Put([]byte("3"))
				return nil
			},
			expectedState:    [][]byte{[]byte("1"), []byte("2"), []byte("3"), nil},
			expectedCapacity: 4,
			expectedLength:   3,
			expectedHead:     0,
			expectedTail:     3,
		},
		{
			name:     "pop_one",
			capacity: 4,
			operation: func(r *RingBuffer) [][]byte {
				r.Put([]byte("1"))
				r.Put([]byte("2"))
				return [][]byte{r.Pop()}
			},
			expectedReturn:   [][]byte{[]byte("1")},
			expectedState:    [][]byte{[]byte("1"), []byte("2"), nil, nil},
			expectedCapacity: 4,
			expectedLength:   1,
			expectedHead:     1,
			expectedTail:     2,
		},
		{
			name:     "pop_all",
			capacity: 4,
			operation: func(r *RingBuffer) [][]byte {
				r.Put([]byte("1"))
				r.Put([]byte("2"))
				return [][]byte{r.Pop(), r.Pop()}
			},
			expectedReturn:   [][]byte{[]byte("1"), []byte("2")},
			expectedState:    [][]byte{[]byte("1"), []byte("2"), nil, nil},
			expectedCapacity: 4,
			expectedLength:   0,
			expectedHead:     2,
			expectedTail:     2,
		},
		{
			name:     "pop_empty",
			capacity: 2,
			operation: func(r *RingBuffer) [][]byte {
				return [][]byte{r.Pop()}

			},
			expectedReturn:   [][]byte{nil},
			expectedState:    [][]byte{nil, nil},
			expectedCapacity: 2,
			expectedLength:   0,
			expectedHead:     0,
			expectedTail:     0,
		},
		{
			name:     "pop_more",
			capacity: 4,
			operation: func(r *RingBuffer) [][]byte {
				r.Put([]byte("1"))
				r.Put([]byte("2"))
				return [][]byte{r.Pop(), r.Pop(), r.Pop()}
			},
			expectedReturn:   [][]byte{[]byte("1"), []byte("2"), nil},
			expectedState:    [][]byte{[]byte("1"), []byte("2"), nil, nil},
			expectedCapacity: 4,
			expectedLength:   0,
			expectedHead:     2,
			expectedTail:     2,
		},
		{
			name:     "pop-wrapped",
			capacity: 4,
			operation: func(r *RingBuffer) [][]byte {
				r.Put([]byte("1"))
				r.Put([]byte("2"))
				r.Put([]byte("3"))
				r.Pop()
				r.Pop()
				r.Put([]byte("4"))
				r.Put([]byte("5"))
				return nil
			},
			expectedState:    [][]byte{[]byte("5"), []byte("2"), []byte("3"), []byte("4")},
			expectedCapacity: 4,
			expectedLength:   3,
			expectedHead:     2,
			expectedTail:     1,
		},
		{
			name:     "popn_equal",
			capacity: 4,
			operation: func(r *RingBuffer) [][]byte {
				r.Put([]byte("1"))
				r.Put([]byte("2"))
				return r.PopN(2)
			},
			expectedReturn:   [][]byte{[]byte("1"), []byte("2")},
			expectedState:    [][]byte{[]byte("1"), []byte("2"), nil, nil},
			expectedCapacity: 4,
			expectedLength:   0,
			expectedHead:     2,
			expectedTail:     2,
		},
		{
			name:     "popn-empty",
			capacity: 2,
			operation: func(r *RingBuffer) [][]byte {
				return r.PopN(3)
			},
			expectedState:    [][]byte{nil, nil},
			expectedCapacity: 2,
			expectedLength:   0,
			expectedHead:     0,
			expectedTail:     0,
		},
		{
			name:     "popn-wrapped_equal-n>=l",
			capacity: 4,
			operation: func(r *RingBuffer) [][]byte {
				r.Put([]byte("1"))
				r.Put([]byte("2"))
				r.Put([]byte("3"))
				r.PopN(1)
				r.PopN(1)
				r.Put([]byte("4"))
				r.Put([]byte("5"))
				return r.PopN(3)
			},
			expectedReturn:   [][]byte{[]byte("3"), []byte("4"), []byte("5")},
			expectedState:    [][]byte{[]byte("5"), []byte("2"), []byte("3"), []byte("4")},
			expectedCapacity: 4,
			expectedLength:   0,
			expectedHead:     0,
			expectedTail:     0,
		},
		{
			name:     "popn-wrapped_n<l",
			capacity: 4,
			operation: func(r *RingBuffer) [][]byte {
				r.Put([]byte("1"))
				r.Put([]byte("2"))
				r.PopN(1)
				r.Put([]byte("3"))
				r.Put([]byte("4"))
				return r.PopN(2)
			},
			expectedReturn:   [][]byte{[]byte("2"), []byte("3")},
			expectedState:    [][]byte{[]byte("1"), []byte("2"), []byte("3"), []byte("4")},
			expectedCapacity: 4,
			expectedLength:   1,
			expectedHead:     3,
			expectedTail:     0,
		},
		{
			name:     "popall",
			capacity: 4,
			operation: func(r *RingBuffer) [][]byte {
				r.Put([]byte("1"))
				r.Put([]byte("2"))
				return r.PopAll()
			},
			expectedReturn:   [][]byte{[]byte("1"), []byte("2"), nil, nil},
			expectedState:    [][]byte{[]byte("1"), []byte("2"), nil, nil},
			expectedCapacity: 4,
			expectedLength:   0,
			expectedHead:     0,
			expectedTail:     0,
		},
		{
			name:     "popall_empty",
			capacity: 4,
			operation: func(r *RingBuffer) [][]byte {
				return r.PopAll()
			},
			expectedReturn:   nil,
			expectedState:    [][]byte{nil, nil, nil, nil},
			expectedCapacity: 4,
			expectedLength:   0,
			expectedHead:     0,
			expectedTail:     0,
		},
		{
			name:     "put-async",
			capacity: 128,
			operation: func(r *RingBuffer) [][]byte {
				var wg sync.WaitGroup
				for i := 0; i < 127; i++ {
					i := i
					wg.Add(1)
					go func() {
						r.Put([]byte(fmt.Sprintf("%d", i)))
						wg.Done()
					}()
				}
				wg.Wait()
				return nil
			},
			concurrent:       true,
			expectedCapacity: 128,
			expectedLength:   127,
			expectedHead:     0,
			expectedTail:     127,
		},
		{
			name:     "pop-async",
			capacity: 128,
			operation: func(r *RingBuffer) [][]byte {
				var wg sync.WaitGroup
				for i := 0; i < 127; i++ {
					i := i
					r.Put([]byte(fmt.Sprintf("%d", i)))
				}
				for i := 0; i < 127; i++ {
					wg.Add(1)
					go func() {
						r.Pop()
						wg.Done()
					}()
				}
				wg.Wait()
				return nil
			},
			concurrent:       true,
			expectedCapacity: 128,
			expectedLength:   0,
			expectedHead:     127,
			expectedTail:     127,
		},
		{
			name:     "put_pop-async",
			capacity: 512,
			operation: func(r *RingBuffer) [][]byte {
				var wg sync.WaitGroup
				var ops int64 // To count the number of operations

				// Goroutine for Put
				wg.Add(1)
				go func() {
					defer wg.Done()
					for i := 0; i < 511; i++ {
						atomic.AddInt64(&ops, 1)
						r.Put([]byte(fmt.Sprintf("%d", i)))
					}
				}()

				// Goroutine for Pop
				wg.Add(1)
				go func() {
					defer wg.Done()
					for atomic.LoadInt64(&ops) < 2*511 { // Assuming you want to stop after 1022 operations
						if msg := r.Pop(); msg != nil {
							atomic.AddInt64(&ops, 1)
						}
					}
				}()

				wg.Wait()
				return nil
			},
			concurrent:       true,
			expectedCapacity: 512,
			expectedLength:   0,
			expectedHead:     511,
			expectedTail:     511,
		},
	}
	t.Parallel()
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			rb, err := NewRingBuffer(tc.capacity)
			assert.NoError(t, err)

			gotReturn := tc.operation(rb)

			assert.Equal(t, tc.expectedReturn, gotReturn)
			if !tc.concurrent {
				assert.Equal(t, tc.expectedState, *rb.buf(), "buffer array mismatch")
			}
			assert.Equal(t, tc.expectedCapacity, rb.Cap(), "buffer capacity mismatch")
			assert.Equal(t, tc.expectedLength, rb.Len(), "buffer length mismatch")
			assert.Equal(t, tc.expectedHead, rb.h.Load(), "buffer head mistmatch")
			assert.Equal(t, tc.expectedTail, rb.t.Load(), "buffer tail mismatch")
		})
	}
}

// func BenchmarkPut(b *testing.B) {
// {
//				name:     "1mil_messafes-0.01kb-grow",
//				capacity: 2,
//				operation: func(r *RingBuffer)[][]byte {
//					for i := 0; i < 1_000_000; i++ {
//						msg := make([]byte, 10)
//						r.Put(msg)
//					}
//				},
//				operation: func(r *RingBuffer) {
//					r.Pop()
//				},
//			},
// }

func BenchmarkPop(b *testing.B) {
	benchCases := []ringBufferPopBenchCase{
		{
			name:     "10k_messages-0.01kb",
			capacity: 65_536,
			setup: func(r *RingBuffer) {
				for i := 0; i < 10_000; i++ {
					msg := make([]byte, 10)
					r.Put(msg)
				}
			},
		},
		{
			name:     "1mil_messages-0.01kb",
			capacity: 1_048_576,
			setup: func(r *RingBuffer) {
				for i := 0; i < 1_000_000; i++ {
					msg := make([]byte, 10)
					r.Put(msg)
				}
			},
		},
		{
			name:     "10mil_messages-0.01kb",
			capacity: 16_777_216,
			setup: func(r *RingBuffer) {
				for i := 0; i < 10_000_00; i++ {
					msg := make([]byte, 10)
					r.Put(msg)
				}
			},
		},
	}

	for _, bc := range benchCases {
		b.Run(bc.name, func(b *testing.B) {
			rb, err := NewRingBuffer(bc.capacity)
			assert.NoError(b, err)

			bc.setup(rb)

			for i := 0; i < b.N; i++ {
				rb.Pop()
			}
		})

		b.Run(bc.name+"_pop-all", func(b *testing.B) {
			rb, err := NewRingBuffer(bc.capacity)
			assert.NoError(b, err)
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				bc.setup(rb)
				b.StartTimer()
				for {
					msg := rb.Pop()
					if msg == nil {
						break
					}
				}
			}

		})

	}
}

func waitRandomTime(min, max int) {
	duration := time.Duration(r.Intn(max-min)+min) * time.Millisecond
	time.Sleep(duration)
}
