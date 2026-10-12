package config

import (
	"errors"
	"fmt"
	"io"
	"runtime/debug"
	"sync"
	"sync/atomic"
)

// ErrNilView is reported when the sampled view is nil; nil views are never
// derived from or remembered.
var ErrNilView = errors.New("config: provider returned a nil view")

// ErrViewPanicked reports a derivation that panicked. The memo treats the
// panic as a failed generation and keeps the recovered value in the error.
// Panic payloads may appear in returned errors and logs. Secret formatting
// redacts, but plaintext extracted with Value or StringValue must never be
// included in diagnostic payloads.
var ErrViewPanicked = errors.New("config: view panicked during derivation")

// GenerationMemo derives one immutable value per configuration generation.
//
// Only a sampled view whose exact dynamic type is RootView identifies a
// generation; facade narrowing preserves that type. Custom views, including
// wrappers embedding RootView, derive and report failures on every call.
// A nil interface or zero RootView is ErrNilView. A RootView derivation,
// including a failure or panic, is remembered while its entry remains current
// and reported once; concurrent reads share it and hits take no locks.
// Failed derivations retain the last valid value. Recovery covers derive,
// not provider sampling or getters called directly by constructors.
// Derived values are shared and must be treated as immutable.
type GenerationMemo[V any, T any] struct {
	derive  func(V) (T, error)
	onError func(error)
	mu      sync.Mutex // serializes derivation only
	last    atomic.Pointer[generationEntry[T]]
	strict  bool
}

type generationEntry[T any] struct {
	key      *rootGeneration // nil: entry identifies no generation
	value    T               // last valid value, inherited across failed generations
	hasValue bool
	err      error
}

// NewGenerationMemo builds a memo around derive; onError may be nil. It
// panics immediately when derive is nil.
func NewGenerationMemo[V any, T any](derive func(V) (T, error), onError func(error)) *GenerationMemo[V, T] {
	if derive == nil {
		panic("config invariant generation_memo_derive_present violated: NewGenerationMemo requires non-nil derive")
	}
	return &GenerationMemo[V, T]{derive: derive, onError: onError}
}

// generationKey recognizes only the exact RootView type and a nil interface.
func generationKey(view any) (*rootGeneration, bool) {
	switch v := view.(type) {
	case nil:
		return nil, true
	case RootView:
		return v.g, v.g == nil
	default:
		return nil, false
	}
}

// Seed installs value as the derivation of view (the startup generation).
func (m *GenerationMemo[V, T]) Seed(view V, value T) {
	key, _ := generationKey(view)
	m.last.Store(&generationEntry[T]{key: key, value: value, hasValue: true})
}

// Get returns the value for view's generation, or the last valid value when
// view is nil or fails to derive. The bool is false only when no valid value
// has ever been derived or seeded.
func (m *GenerationMemo[V, T]) Get(view V) (T, bool) {
	entry := m.get(view)
	return entry.value, entry.hasValue
}

func (m *GenerationMemo[V, T]) get(view V) *generationEntry[T] {
	key, nilView := generationKey(view)
	if nilView {
		m.report(ErrNilView)
		entry := &generationEntry[T]{err: ErrNilView}
		if previous := m.last.Load(); !m.strict && previous != nil {
			entry.value, entry.hasValue = previous.value, previous.hasValue
		}
		return entry
	}
	if entry := m.last.Load(); key != nil && entry != nil && entry.key == key {
		return entry
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	previous := m.last.Load()
	if key != nil && previous != nil && previous.key == key {
		return previous // derived while we waited
	}
	entry := &generationEntry[T]{key: key}
	if value, err := m.safeDerive(view); err != nil {
		m.report(err)
		entry.err = err
		if !m.strict && previous != nil {
			entry.value, entry.hasValue = previous.value, previous.hasValue
		}
	} else {
		entry.value, entry.hasValue = value, true
	}
	m.last.Store(entry)
	return entry
}

// safeDerive treats a panicking derivation as a failed generation.
func (m *GenerationMemo[V, T]) safeDerive(view V) (value T, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = &viewPanicError{value: fmt.Sprint(recovered), stack: debug.Stack()}
		}
	}()
	return m.derive(view)
}

// viewPanicError carries a contained derivation panic. Its message keeps the
// recovered value; the stack renders only under %+v (zap's errorVerbose field),
// so onError logs locate the panic without the stack reaching error strings.
type viewPanicError struct {
	value string
	stack []byte
}

func (e *viewPanicError) Error() string { return ErrViewPanicked.Error() + ": " + e.value }

func (e *viewPanicError) Unwrap() error { return ErrViewPanicked }

func (e *viewPanicError) Format(s fmt.State, verb rune) {
	_, _ = io.WriteString(s, e.Error())
	if verb == 'v' && s.Flag('+') {
		_, _ = io.WriteString(s, "\n")
		_, _ = s.Write(e.stack)
	}
}

// Provider adapts the memo into a hot provider: each call samples once and
// returns that generation's value (zero T if none is valid yet).
func (m *GenerationMemo[V, T]) Provider(sample func() V) func() T {
	return func() T {
		value, _ := m.Get(sample())
		return value
	}
}

func (m *GenerationMemo[V, T]) report(err error) {
	if m.onError != nil {
		m.onError(err)
	}
}

// StrictGenerationMemo shares GenerationMemo's identity cache and lock-free hit
// path, but never serves an earlier generation after derivation fails. Errors
// for RootView are remembered while its entry remains current; custom views
// derive again and nil views are rejected each time.
// Client construction should remain outside the memo so construction failures
// can be retried on the next operation.
type StrictGenerationMemo[V any, T any] struct{ memo *GenerationMemo[V, T] }

// NewStrictGenerationMemo builds an explicit no-fallback memo. It panics
// immediately when derive is nil.
func NewStrictGenerationMemo[V any, T any](derive func(V) (T, error), onError func(error)) *StrictGenerationMemo[V, T] {
	if derive == nil {
		panic("config invariant generation_memo_derive_present violated: NewStrictGenerationMemo requires non-nil derive")
	}
	m := NewGenerationMemo(derive, onError)
	m.strict = true
	return &StrictGenerationMemo[V, T]{memo: m}
}

// Get returns only the sampled generation's value or its derivation error.
func (m *StrictGenerationMemo[V, T]) Get(view V) (T, error) {
	entry := m.memo.get(view)
	return entry.value, entry.err
}
