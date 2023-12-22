package gomemq

import (
	"fmt"
	"sync"
	"sync/atomic"
)

type RingBuffer[T any] struct {
	ptr    atomic.Pointer[[]T]
	cap    atomic.Uint64
	h      atomic.Uint64
	t      atomic.Uint64
	ch     chan uint64
	doneCh chan any
	bLock  sync.RWMutex
}

func NewRingBuffer[T any](capacity uint64) (*RingBuffer[T], error) {
	if capacity < 2 || capacity&(capacity-1) != 0 {
		return nil, fmt.Errorf("capacity needs to be power of 2")
	}
	buf := make([]T, capacity)
	ptr := atomic.Pointer[[]T]{}
	ptr.Store(&buf)
	cap := atomic.Uint64{}
	cap.Store(capacity)
	r := &RingBuffer[T]{
		ptr:    ptr,
		cap:    cap,
		ch:     make(chan uint64),
		doneCh: make(chan any),
	}
	go r.manager()

	return r, nil
}

// Handles growing the buffer
func (r *RingBuffer[T]) manager() {
	for {
		prevcap, ok := <-r.ch
		if !ok {
			return
		}

		if prevcap < r.cap.Load() {
			r.doneCh <- struct{}{}
			return
		}

		bufN := make([]T, 2*r.Cap())

		r.bLock.Lock()
		copy(bufN, *r.buf())
		r.bLock.Unlock()

		r.ptr.Swap(&bufN)
		r.cap.Add(r.Cap())
		r.doneCh <- struct{}{}
	}
}

func (r *RingBuffer[T]) Len() uint64 {
	if r.t.Load() >= r.h.Load() {
		return r.t.Load() - r.h.Load()
	}
	return r.cap.Load() - r.h.Load() + r.t.Load()
}

func (r *RingBuffer[T]) Cap() uint64 {
	return r.cap.Load()
}

func (r *RingBuffer[T]) Put(v T) {
	if r.full() {
		// only one growth will occur
		r.ch <- r.Cap()
		<-r.doneCh
	}

	r.bLock.Lock()
	defer r.bLock.Unlock()
	prev := r.t.Swap((r.t.Load() + 1) & (r.Cap() - 1))
	(*r.buf())[prev] = v
}

func (r *RingBuffer[T]) Pop() T {
	if r.empty() {
		var zero T
		return zero
	}

	r.bLock.Lock()
	defer r.bLock.Unlock()

	prev := r.h.Swap((r.h.Load() + 1) & (r.Cap() - 1))
	v := (*r.buf())[prev]

	return v

}

func (r *RingBuffer[T]) PopN(n uint64) []T {
	if r.empty() {
		return nil
	}

	r.bLock.Lock()
	defer r.bLock.Unlock()
	h, t, l, c := r.h.Load(), r.t.Load(), r.Len(), r.Cap()

	// check whether buffer is wrapped
	if t > h {
		if n > t {
			// entire buffer will be popped
			n = t
			r.h.Swap(0)
			r.t.Swap(0)
		} else {
			r.h.Add(n)
		}
		v := (*r.buf())[h:n]
		return makeCopy[T](v)
	}
	if n >= l {
		// entire buffer will be popped
		r.h.Swap(0)
		r.t.Swap(0)
		v := append((*r.buf())[h:c], (*r.buf())[0:t]...)
		return makeCopy[T](v)
	}
	r.h.Add(n)
	n -= c - h

	v := append((*r.buf())[h:c-1], (*r.buf())[0:t-n-1]...)
	return makeCopy[T](v)
}

// Returns entire unordered buffer including empty fields.
// If you only want values ordered (FIFO) use PopN(matt.MaxInt64)
func (r *RingBuffer[T]) PopAll() []T {
	if r.empty() {
		return nil
	}
	r.t.Swap(r.h.Load())
	return *r.buf()
}

func (r *RingBuffer[T]) buf() *[]T {
	return r.ptr.Load()
}

func (r *RingBuffer[T]) empty() bool {
	return r.h.Load() == r.t.Load()
}

func (r *RingBuffer[T]) full() bool {
	if r.empty() {
		return false
	}
	return (r.t.Load()+1)&(r.Cap()-1) == r.h.Load()
}
