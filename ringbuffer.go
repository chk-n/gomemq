package gomemq

import (
	"errors"
	"sync"
	"sync/atomic"
)

// Thread safe ring buffer

type RingBuffer[T any] struct {
	ptr    atomic.Pointer[[]T]
	cap    uint64
	h      uint64
	t      uint64
	ch     chan uint64
	doneCh chan any
	bLock  sync.RWMutex
}

func NewRingBuffer[T any](capacity uint64) (*RingBuffer[T], error) {
	if capacity < 2 || capacity&(capacity-1) != 0 {
		return nil, errors.New("capacity needs to be power of 2")
	}
	buf := make([]T, capacity)
	ptr := atomic.Pointer[[]T]{}
	ptr.Store(&buf)
	//	cap := atomic.Uint64{}
	//	cap.Store(capacity)
	r := &RingBuffer[T]{
		ptr:    ptr,
		cap:    capacity,
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

		if prevcap < r.cap {
			r.doneCh <- struct{}{}
			return
		}

		bufN := make([]T, 2*r.cap)

		copy(bufN, *r.buf())

		r.ptr.Swap(&bufN)
		r.cap += r.cap
		r.doneCh <- struct{}{}
	}
}

func (r *RingBuffer[T]) Len() uint64 {
	r.bLock.Lock()
	defer r.bLock.Unlock()
	return r.len()
}

func (r *RingBuffer[T]) Cap() uint64 {
	r.bLock.Lock()
	defer r.bLock.Unlock()
	return r.cap
}

func (r *RingBuffer[T]) Put(v T) {
	r.bLock.Lock()
	defer r.bLock.Unlock()

	if r.full() {
		// only one growth will occur
		r.ch <- r.cap
		<-r.doneCh
	}

	prev := r.t
	r.t = (r.t + 1) & (r.cap - 1)
	(*r.buf())[prev] = v
}

func (r *RingBuffer[T]) PutN(v []T) {
	r.bLock.Lock()
	defer r.bLock.Unlock()

	n := uint64(len(v))
	if r.fullBatch(n) {
		// only one growth will occur
		r.ch <- r.cap
		<-r.doneCh
	}

	h, t, c := r.h, r.t, r.cap

	prev := t
	r.t = (t + n) & (c - 1)

	// remaining space before wrapping around
	rem := c - h
	if rem >= n {
		copy((*r.buf())[prev:prev+n], v)
	} else {
		copy((*r.buf())[h:c], v[:rem])
		copy((*r.buf())[0:t-(n-rem)], v[rem:])
	}
}

func (r *RingBuffer[T]) Pop() T {
	r.bLock.Lock()
	defer r.bLock.Unlock()

	if r.empty() {
		var zero T
		return zero
	}

	prev := r.h
	r.h = (r.h + 1) & (r.cap - 1)
	v := (*r.buf())[prev]

	return v

}

func (r *RingBuffer[T]) PopN(n uint64) []T {
	r.bLock.Lock()
	defer r.bLock.Unlock()

	if r.empty() {
		return nil
	}

	h, t, l, c := r.h, r.t, r.len(), r.cap

	// check whether buffer is wrapped
	if t > h {
		if n > t {
			// entire buffer will be popped
			n = t
			r.h = 0
			r.t = 0
		} else {
			r.h += n
		}
		v := (*r.buf())[h:n]
		return makeCopy[T](v)
	}
	if n >= l {
		// entire buffer will be popped
		r.h = 0
		r.t = 0
		v := append((*r.buf())[h:c], (*r.buf())[0:t]...)
		return makeCopy[T](v)
	}
	r.h += n
	n -= c - h

	v := append((*r.buf())[h:c-1], (*r.buf())[0:t-n-1]...)
	return makeCopy[T](v)
}

// Returns entire unordered buffer including empty fields.
// If you only want values ordered (FIFO) use PopN(matt.MaxInt64)
func (r *RingBuffer[T]) PopAll() []T {
	r.bLock.Lock()
	defer r.bLock.Unlock()

	if r.empty() {
		return nil
	}

	r.t = r.h
	return *r.buf()
}

func (r *RingBuffer[T]) len() uint64 {
	if r.t >= r.h {
		return r.t - r.h
	}
	return r.cap - r.h + r.t
}

func (r *RingBuffer[T]) buf() *[]T {
	return r.ptr.Load()
}

func (r *RingBuffer[T]) empty() bool {
	return r.h == r.t
}

func (r *RingBuffer[T]) full() bool {
	if r.empty() {
		return false
	}
	return (r.t+1)&(r.cap-1) == r.h
}

// checks whether buffer would be full after a batch insert
func (r *RingBuffer[T]) fullBatch(i uint64) bool {
	if r.cap-r.len() < i {
		return true
	}
	// NOTE check needs to come here and not before otherwise any putN call to empty buffer will yield false (even if n > cap)
	if r.empty() {
		return false
	}
	return (r.t+i)&(r.cap-1) == r.h
}
