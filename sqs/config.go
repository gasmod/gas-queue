package sqs

import (
	"errors"
	"time"

	env "github.com/gasmod/gas-config/extensions/gas-env"
)

const (
	defaultRegion            = "us-east-1"
	defaultVisibilityTimeout = 30 * time.Second
	defaultWaitTimeSeconds   = 20
)

// Config holds SQS queue settings.
type Config struct {
	env.WithGasEnv

	Queue Settings
}

// Settings represents the configuration for the SQS queue service.
type Settings struct {
	// Region is the AWS region for the SQS service.
	Region string

	// Endpoint is an optional custom endpoint URL (e.g. for LocalStack).
	// Empty means use the default AWS endpoint.
	Endpoint string

	// VisibilityTimeout is how long a dequeued message stays invisible
	// to other consumers. SQS max is 12 hours.
	VisibilityTimeout time.Duration

	// WaitTimeSeconds is the long-poll duration for ReceiveMessage calls.
	// Must be 0-20 (SQS hard limit).
	WaitTimeSeconds int
}

// DefaultConfig returns a Config with sensible defaults.
func DefaultConfig() *Config {
	return &Config{
		Queue: Settings{
			Region:            defaultRegion,
			VisibilityTimeout: defaultVisibilityTimeout,
			WaitTimeSeconds:   defaultWaitTimeSeconds,
		},
	}
}

// Validate checks the Config for correctness.
func (c *Config) Validate() error {
	if c.Queue.Region == "" {
		return errors.New("Queue.Region must not be empty")
	}
	if c.Queue.WaitTimeSeconds < 0 || c.Queue.WaitTimeSeconds > 20 {
		return errors.New("Queue.WaitTimeSeconds must be between 0 and 20")
	}
	if c.Queue.VisibilityTimeout < 0 {
		return errors.New("Queue.VisibilityTimeout must not be negative")
	}
	return nil
}
