package queue

import "errors"

// Sentinel errors returned by JobQueueProvider implementations.
var (
	// ErrClosed is returned when an operation is attempted on a closed
	// queue service.
	ErrClosed = errors.New("queue: service is closed")
)
