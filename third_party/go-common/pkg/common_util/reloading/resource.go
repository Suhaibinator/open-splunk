// Package reloading owns replaceable clients with operation-scoped leases.
package reloading

import (
	"errors"
	"sync"
)

var ErrClosed = errors.New("reloading resource is closed")

type entry[T any] struct {
	value   T
	refs    int
	retired bool
}

// Resource caches only the active key. Failed construction never serves a stale
// client, and is retried on the next operation. Retired clients close when their
// last lease ends; no request migration or cancellation is required.
type Resource[K comparable, T any] struct {
	mu      sync.Mutex
	key     K
	current *entry[T]
	closed  bool
	build   func(K) (T, error)
	close   func(T)
}

func New[K comparable, T any](build func(K) (T, error), close func(T)) *Resource[K, T] {
	return &Resource[K, T]{build: build, close: close}
}

func (r *Resource[K, T]) Acquire(key K) (T, func(), error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	var zero T
	if r.closed {
		return zero, nil, ErrClosed
	}
	if r.current == nil || r.key != key {
		r.retire()
		value, err := r.build(key)
		if err != nil {
			return zero, nil, err
		}
		r.key = key
		r.current = &entry[T]{value: value}
	}
	e := r.current
	e.refs++
	var once sync.Once
	return e.value, func() {
		once.Do(func() {
			r.mu.Lock()
			defer r.mu.Unlock()
			e.refs--
			if e.retired && e.refs == 0 && r.close != nil {
				r.close(e.value)
			}
		})
	}, nil
}

func (r *Resource[K, T]) retire() {
	if r.current != nil {
		e := r.current
		e.retired = true
		if e.refs == 0 && r.close != nil {
			r.close(e.value)
		}
		r.current = nil
	}
}

func (r *Resource[K, T]) Close() { r.mu.Lock(); defer r.mu.Unlock(); r.closed = true; r.retire() }
