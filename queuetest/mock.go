// Package queuetest provides a mock implementation of gas.JobQueueProvider
// for use in tests. The mock records all calls and allows configuring
// per-method behavior via function fields.
//
//	mock := &queuetest.MockQueue{}
//	mock.EnqueueFn = func(ctx context.Context, queue string, payload []byte, opts ...gas.EnqueueOption) error {
//	    return nil
//	}
package queuetest

import (
	"context"
	"sync"
	"time"

	"github.com/gasmod/gas"
)

// MockQueue is a configurable mock of gas.JobQueueProvider. Each method
// delegates to its corresponding Fn field if set, otherwise returns the
// zero value. All calls are recorded in the Calls slice for assertions.
type MockQueue struct {
	EnqueueFn func(ctx context.Context, queue string, payload []byte, opts ...gas.EnqueueOption) error
	DequeueFn func(ctx context.Context, queue string, maxMessages int, wait time.Duration) ([]gas.Job, error)
	AckFn     func(ctx context.Context, queue string, job gas.Job) error
	NackFn    func(ctx context.Context, queue string, job gas.Job) error
	Calls     []Call

	mu sync.Mutex
}

var _ gas.JobQueueProvider = (*MockQueue)(nil)

// Call records a single method invocation on the mock.
type Call struct {
	Method string
	Args   []any
}

func (m *MockQueue) record(method string, args ...any) {
	m.mu.Lock()
	m.Calls = append(m.Calls, Call{Method: method, Args: args})
	m.mu.Unlock()
}

// Enqueue records the call and delegates to EnqueueFn if set.
func (m *MockQueue) Enqueue(ctx context.Context, queue string, payload []byte, opts ...gas.EnqueueOption) error {
	m.record("Enqueue", queue, payload, opts)
	if m.EnqueueFn != nil {
		return m.EnqueueFn(ctx, queue, payload, opts...)
	}
	return nil
}

// Dequeue records the call and delegates to DequeueFn if set.
func (m *MockQueue) Dequeue(ctx context.Context, queue string, maxMessages int, wait time.Duration) ([]gas.Job, error) {
	m.record("Dequeue", queue, maxMessages, wait)
	if m.DequeueFn != nil {
		return m.DequeueFn(ctx, queue, maxMessages, wait)
	}
	return nil, nil
}

// Ack records the call and delegates to AckFn if set.
func (m *MockQueue) Ack(ctx context.Context, queue string, job gas.Job) error {
	m.record("Ack", queue, job)
	if m.AckFn != nil {
		return m.AckFn(ctx, queue, job)
	}
	return nil
}

// Nack records the call and delegates to NackFn if set.
func (m *MockQueue) Nack(ctx context.Context, queue string, job gas.Job) error {
	m.record("Nack", queue, job)
	if m.NackFn != nil {
		return m.NackFn(ctx, queue, job)
	}
	return nil
}

// Reset clears all recorded calls.
func (m *MockQueue) Reset() {
	m.mu.Lock()
	m.Calls = nil
	m.mu.Unlock()
}

// CallCount returns the number of times the given method was called.
func (m *MockQueue) CallCount(method string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	n := 0
	for _, c := range m.Calls {
		if c.Method == method {
			n++
		}
	}
	return n
}
