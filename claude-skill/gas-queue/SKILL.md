---
name: gas-queue
description: >
  Reference documentation for the gas-queue Go package
  (github.com/gasmod/gas-queue) — the job queue service for the Gas ecosystem.
  Use this skill when writing, reviewing, or debugging Go code that uses
  gas-queue for async job/message queue processing with AWS SQS. Covers the sqs
  sub-package, queuetest mock, gas.JobQueueProvider implementation, sentinel
  errors, DI wiring, configuration binding, enqueue options (delay, FIFO group,
  deduplication, attributes), dequeue with long-polling, ack/nack semantics,
  and custom endpoint support for ElasticMQ. Make sure to use this skill
  whenever working with job queues in the Gas ecosystem, even if the user
  doesn't explicitly mention gas-queue — any code that imports gasmod/gas-queue
  or references gas.JobQueueProvider should trigger this skill.
---

# Gas Queue Package Reference

Job queue service for the Gas ecosystem. Provides a `gas.JobQueueProvider`
implementation backed by AWS SQS, plus a reusable test mock.

```
import queue "github.com/gasmod/gas-queue"
import queuesqs "github.com/gasmod/gas-queue/sqs"
import "github.com/gasmod/gas-queue/queuetest"
```

## Backends

| Backend | Package          | Service name    | Use case                          |
|---------|------------------|-----------------|-----------------------------------|
| SQS     | `gas-queue/sqs`  | `gas-queue-sqs` | Production (AWS SQS / ElasticMQ) |

The SQS backend implements `gas.Service`, `gas.JobQueueProvider`, and
`gas.ReadyReporter`. `CheckReady` returns an error before `Init` runs and
after `Close` is called (wrapping `queue.ErrClosed`); otherwise it returns
nil. The check is local-only — it does not call AWS — so readiness probes
remain cheap and independent of regional SQS availability.

## JobQueueProvider Interface

Defined in the gas core package:

```go
type JobQueueProvider interface {
    Enqueue(ctx context.Context, queue string, payload []byte, opts ...EnqueueOption) error
    Dequeue(ctx context.Context, queue string, maxMessages int, wait time.Duration) ([]Job, error)
    Ack(ctx context.Context, queue string, job Job) error
    Nack(ctx context.Context, queue string, job Job) error
}
```

The `queue` parameter in every method is the **SQS queue URL** directly.

### Job

```go
type Job struct {
    ID            string
    ReceiptHandle string            // opaque token used by Ack/Nack
    Attributes    map[string]string // merged system + message attributes
    Body          []byte
}
```

### EnqueueOption

```go
gas.WithDelay(d time.Duration)              // initial visibility delay
gas.WithGroupID(id string)                  // FIFO ordering (SQS: MessageGroupId)
gas.WithDedupeID(id string)                 // deduplication (SQS: MessageDeduplicationId)
gas.WithJobAttributes(attrs map[string]string) // provider-specific metadata
```

Implementations unpack options via `gas.ApplyEnqueueOptions(opts)`.

## Sentinel Errors

The root `queue` package defines sentinel errors:

```go
queue.ErrClosed // returned when an operation is attempted on a closed service
```

## SQS Backend

### Constructor

```go
func New(opts ...Option) func(gas.ConfigProvider, gas.Logger) *Service
```

`New` captures options and returns a DI-injectable constructor. The returned
func receives `gas.ConfigProvider` and `gas.Logger` from the DI container.

### Options

| Option                         | Description                                                      |
|--------------------------------|------------------------------------------------------------------|
| `WithConfig(cfg *Config)`      | Set configuration explicitly (skips config binding from DI)      |
| `WithClient(client sqsClient)` | Inject a pre-configured SQS client (testing or custom AWS creds) |

### Lifecycle (gas.Service)

| Method  | Signature   | Description                                           |
|---------|-------------|-------------------------------------------------------|
| `Name`  | `() string` | Returns `"gas-queue-sqs"`                             |
| `Init`  | `() error`  | Validates config, creates AWS SQS client              |
| `Close` | `() error`  | Marks service as closed                               |

### Behavior

- **Enqueue:** Calls SQS `SendMessage`. Supports delay, FIFO group ID,
  deduplication ID, and string message attributes via `EnqueueOption`.
- **Dequeue:** Calls SQS `ReceiveMessage` with long-polling. `maxMessages` is
  clamped to 1-10 (SQS hard limit). When `wait == 0`, uses the configured
  `WaitTimeSeconds`. Merges SQS system attributes and message attributes
  into `Job.Attributes`.
- **Ack:** Calls SQS `DeleteMessage` using `job.ReceiptHandle`.
- **Nack:** Calls SQS `ChangeMessageVisibility` with timeout 0, making the
  message immediately available for reprocessing.
- **No upfront connection:** The AWS SQS client is stateless HTTP — Init
  creates the client but does not verify connectivity. Connection errors
  surface at call time.
- **All methods return `queue.ErrClosed`** when the service has been closed.

### Config

```go
type Config struct {
    env.WithGasEnv
    Queue Settings
}

type Settings struct {
    Region            string        // default "us-east-1"
    Endpoint          string        // optional custom endpoint (e.g. ElasticMQ)
    VisibilityTimeout time.Duration // default 30s
    WaitTimeSeconds   int           // 0-20, default 20
}

func DefaultConfig() *Config
func (c *Config) Validate() error  // rejects empty Region, WaitTimeSeconds outside 0-20
```

## Test Mock

The `queuetest` package provides `MockQueue`, a configurable mock of
`gas.JobQueueProvider` for use in unit tests.

```go
import "github.com/gasmod/gas-queue/queuetest"
```

### MockQueue

```go
type MockQueue struct {
    EnqueueFn func(ctx context.Context, queue string, payload []byte, opts ...gas.EnqueueOption) error
    DequeueFn func(ctx context.Context, queue string, maxMessages int, wait time.Duration) ([]gas.Job, error)
    AckFn     func(ctx context.Context, queue string, job gas.Job) error
    NackFn    func(ctx context.Context, queue string, job gas.Job) error
    Calls     []Call
}
```

Each method delegates to its `Fn` field if set, otherwise returns zero value.
All calls are recorded in `Calls` for assertions. Thread-safe.

| Method                  | Description                                   |
|-------------------------|-----------------------------------------------|
| `Reset()`               | Clear all recorded calls                      |
| `CallCount(method) int` | Count calls by method name (e.g. `"Enqueue"`) |

## DI Wiring Patterns

### SQS backend

```go
app := gas.NewApp(
    gas.WithSingletonService[*queuesqs.Service](queuesqs.New()),
)
```

### With explicit config

```go
app := gas.NewApp(
    gas.WithSingletonService[*queuesqs.Service](
        queuesqs.New(queuesqs.WithConfig(&queuesqs.Config{
            Queue: queuesqs.Settings{
                Region:   "eu-west-1",
                Endpoint: "http://localhost:9324",
            },
        })),
    ),
)
```

### Consuming via gas.JobQueueProvider

Services receive the queue through the provider interface, never importing
gas-queue backends directly:

```go
type Service struct {
    queue gas.JobQueueProvider
}

func New(queue gas.JobQueueProvider) *Service {
    return &Service{queue: queue}
}

func (s *Service) Init() error {
    // use s.queue.Enqueue, Dequeue, Ack, Nack
    return nil
}
```

### Using the test mock

```go
mock := &queuetest.MockQueue{}
mock.EnqueueFn = func(ctx context.Context, queue string, payload []byte, opts ...gas.EnqueueOption) error {
    return nil
}

// inject mock as gas.JobQueueProvider in tests
```
