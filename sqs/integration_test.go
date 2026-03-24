package sqs

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/gasmod/gas"
	queue "github.com/gasmod/gas-queue"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// shared across all integration tests — one container per test run.
var (
	sqsEndpoint string
	sqsCleanup  func()
	sqsReady    sync.Once
	sqsSetupErr error
	queueSeq    int
	queueSeqMu  sync.Mutex
)

func setupElasticMQOnce(t *testing.T) string {
	t.Helper()
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	sqsReady.Do(func() {
		ctx := context.Background()
		req := testcontainers.ContainerRequest{
			Image:        "softwaremill/elasticmq-native:latest",
			ExposedPorts: []string{"9324/tcp"},
			WaitingFor:   wait.ForListeningPort("9324/tcp").WithStartupTimeout(60 * time.Second),
		}
		container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
			ContainerRequest: req,
			Started:          true,
		})
		if err != nil {
			sqsSetupErr = fmt.Errorf("start elasticmq: %w", err)
			return
		}
		host, err := container.Host(ctx)
		if err != nil {
			sqsSetupErr = fmt.Errorf("get host: %w", err)
			return
		}
		port, err := container.MappedPort(ctx, "9324")
		if err != nil {
			sqsSetupErr = fmt.Errorf("get port: %w", err)
			return
		}
		sqsEndpoint = fmt.Sprintf("http://%s:%s", host, port.Port())
		sqsCleanup = func() {
			if err := container.Terminate(ctx); err != nil {
				// best-effort
				_ = err
			}
		}
	})

	if sqsSetupErr != nil {
		t.Fatalf("elasticmq: %v", sqsSetupErr)
	}
	t.Cleanup(func() {
		// sqsCleanup is called only once, managed externally
	})
	return sqsEndpoint
}

// newSQSClient creates an SQS client pointed at the given endpoint with
// dummy credentials. ElasticMQ does not validate credentials.
func newSQSClient(t *testing.T, endpoint string) *awssqs.Client {
	t.Helper()
	ctx := context.Background()
	cfg, err := awsconfig.LoadDefaultConfig(ctx,
		awsconfig.WithRegion("us-east-1"),
		awsconfig.WithCredentialsProvider(aws.CredentialsProviderFunc(
			func(context.Context) (aws.Credentials, error) {
				return aws.Credentials{AccessKeyID: "test", SecretAccessKey: "test"}, nil
			},
		)),
	)
	if err != nil {
		t.Fatalf("load aws config: %v", err)
	}
	return awssqs.NewFromConfig(cfg, func(o *awssqs.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
}

func uniqueQueue(t *testing.T, endpoint string) string {
	t.Helper()
	queueSeqMu.Lock()
	queueSeq++
	name := fmt.Sprintf("test-%s-%d", t.Name(), queueSeq)
	queueSeqMu.Unlock()

	// SQS queue names: alphanumeric, hyphens, underscores only.
	name = strings.NewReplacer("/", "-", " ", "-").Replace(name)
	if len(name) > 80 {
		name = name[:80]
	}

	client := newSQSClient(t, endpoint)
	out, err := client.CreateQueue(context.Background(), &awssqs.CreateQueueInput{
		QueueName: aws.String(name),
	})
	if err != nil {
		t.Fatalf("create queue %q: %v", name, err)
	}
	return *out.QueueUrl
}

func uniqueFIFOQueue(t *testing.T, endpoint string) string {
	t.Helper()
	queueSeqMu.Lock()
	queueSeq++
	name := fmt.Sprintf("test-%d.fifo", queueSeq)
	queueSeqMu.Unlock()

	client := newSQSClient(t, endpoint)
	out, err := client.CreateQueue(context.Background(), &awssqs.CreateQueueInput{
		QueueName: aws.String(name),
		Attributes: map[string]string{
			"FifoQueue":                 "true",
			"ContentBasedDeduplication": "false",
		},
	})
	if err != nil {
		t.Fatalf("create FIFO queue %q: %v", name, err)
	}
	return *out.QueueUrl
}

func newIntegrationService(t *testing.T, endpoint string) *Service {
	t.Helper()

	cfg := DefaultConfig()
	cfg.Queue.Endpoint = endpoint
	cfg.Queue.Region = "us-east-1"
	cfg.Queue.WaitTimeSeconds = 1
	cfg.Queue.VisibilityTimeout = 5 * time.Second

	client := newSQSClient(t, endpoint)
	ctor := New(WithConfig(cfg), WithClient(client))
	svc := ctor(nil, gas.NewNopLogger()())
	if err := svc.Init(); err != nil {
		t.Fatalf("init: %v", err)
	}
	t.Cleanup(func() { svc.Close() })
	return svc
}

// --- basic round-trip ---

func TestIntegration_EnqueueDequeue(t *testing.T) {
	endpoint := setupElasticMQOnce(t)
	queueURL := uniqueQueue(t, endpoint)
	svc := newIntegrationService(t, endpoint)
	ctx := context.Background()

	payload := []byte(`{"task":"process","id":42}`)
	if err := svc.Enqueue(ctx, queueURL, payload); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	jobs, err := svc.Dequeue(ctx, queueURL, 10, time.Second)
	if err != nil {
		t.Fatalf("Dequeue: %v", err)
	}
	if len(jobs) != 1 {
		t.Fatalf("got %d jobs, want 1", len(jobs))
	}

	job := jobs[0]
	if job.ID == "" {
		t.Error("Job.ID is empty")
	}
	if job.ReceiptHandle == "" {
		t.Error("Job.ReceiptHandle is empty")
	}
	if string(job.Body) != string(payload) {
		t.Errorf("Body = %q, want %q", string(job.Body), string(payload))
	}
}

// --- ack removes message permanently ---

func TestIntegration_Ack(t *testing.T) {
	endpoint := setupElasticMQOnce(t)
	queueURL := uniqueQueue(t, endpoint)
	svc := newIntegrationService(t, endpoint)
	ctx := context.Background()

	if err := svc.Enqueue(ctx, queueURL, []byte("ack-me")); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	jobs, err := svc.Dequeue(ctx, queueURL, 1, time.Second)
	if err != nil {
		t.Fatalf("Dequeue: %v", err)
	}
	if len(jobs) != 1 {
		t.Fatalf("got %d jobs, want 1", len(jobs))
	}

	if err := svc.Ack(ctx, queueURL, jobs[0]); err != nil {
		t.Fatalf("Ack: %v", err)
	}

	remaining, err := svc.Dequeue(ctx, queueURL, 10, time.Second)
	if err != nil {
		t.Fatalf("Dequeue after Ack: %v", err)
	}
	if len(remaining) != 0 {
		t.Errorf("got %d jobs after Ack, want 0", len(remaining))
	}
}

// --- nack makes message immediately re-deliverable ---

func TestIntegration_Nack(t *testing.T) {
	endpoint := setupElasticMQOnce(t)
	queueURL := uniqueQueue(t, endpoint)
	svc := newIntegrationService(t, endpoint)
	ctx := context.Background()

	if err := svc.Enqueue(ctx, queueURL, []byte("nack-me")); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	jobs, err := svc.Dequeue(ctx, queueURL, 1, time.Second)
	if err != nil {
		t.Fatalf("Dequeue: %v", err)
	}
	if len(jobs) != 1 {
		t.Fatalf("got %d jobs, want 1", len(jobs))
	}

	if err := svc.Nack(ctx, queueURL, jobs[0]); err != nil {
		t.Fatalf("Nack: %v", err)
	}

	redelivered, err := svc.Dequeue(ctx, queueURL, 1, 2*time.Second)
	if err != nil {
		t.Fatalf("Dequeue after Nack: %v", err)
	}
	if len(redelivered) != 1 {
		t.Fatalf("got %d jobs after Nack, want 1", len(redelivered))
	}
	if string(redelivered[0].Body) != "nack-me" {
		t.Errorf("Body = %q, want %q", string(redelivered[0].Body), "nack-me")
	}
}

// --- custom message attributes survive round-trip ---

func TestIntegration_MessageAttributes(t *testing.T) {
	endpoint := setupElasticMQOnce(t)
	queueURL := uniqueQueue(t, endpoint)
	svc := newIntegrationService(t, endpoint)
	ctx := context.Background()

	attrs := map[string]string{"env": "staging", "priority": "high"}
	if err := svc.Enqueue(ctx, queueURL, []byte("with-attrs"), gas.WithJobAttributes(attrs)); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	jobs, err := svc.Dequeue(ctx, queueURL, 1, time.Second)
	if err != nil {
		t.Fatalf("Dequeue: %v", err)
	}
	if len(jobs) != 1 {
		t.Fatalf("got %d jobs, want 1", len(jobs))
	}

	for k, want := range attrs {
		if got := jobs[0].Attributes[k]; got != want {
			t.Errorf("Attributes[%q] = %q, want %q", k, got, want)
		}
	}
}

// --- closed service returns sentinel error ---

func TestIntegration_Close(t *testing.T) {
	endpoint := setupElasticMQOnce(t)
	queueURL := uniqueQueue(t, endpoint)

	cfg := DefaultConfig()
	cfg.Queue.Endpoint = endpoint
	cfg.Queue.Region = "us-east-1"
	cfg.Queue.WaitTimeSeconds = 1

	ctor := New(WithConfig(cfg))
	svc := ctor(nil, gas.NewNopLogger()())
	if err := svc.Init(); err != nil {
		t.Fatalf("init: %v", err)
	}
	if err := svc.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	ctx := context.Background()

	if err := svc.Enqueue(ctx, queueURL, []byte("x")); !errors.Is(err, queue.ErrClosed) {
		t.Errorf("Enqueue after Close: got %v, want ErrClosed", err)
	}
	if _, err := svc.Dequeue(ctx, queueURL, 1, time.Second); !errors.Is(err, queue.ErrClosed) {
		t.Errorf("Dequeue after Close: got %v, want ErrClosed", err)
	}
	if err := svc.Ack(ctx, queueURL, gas.Job{ReceiptHandle: "x"}); !errors.Is(err, queue.ErrClosed) {
		t.Errorf("Ack after Close: got %v, want ErrClosed", err)
	}
	if err := svc.Nack(ctx, queueURL, gas.Job{ReceiptHandle: "x"}); !errors.Is(err, queue.ErrClosed) {
		t.Errorf("Nack after Close: got %v, want ErrClosed", err)
	}
}

// --- batch: enqueue many, dequeue in batches, verify all arrive ---

func TestIntegration_BatchDequeue(t *testing.T) {
	endpoint := setupElasticMQOnce(t)
	queueURL := uniqueQueue(t, endpoint)
	svc := newIntegrationService(t, endpoint)
	ctx := context.Background()

	const total = 15
	for i := range total {
		payload := fmt.Sprintf("msg-%d", i)
		if err := svc.Enqueue(ctx, queueURL, []byte(payload)); err != nil {
			t.Fatalf("Enqueue msg-%d: %v", i, err)
		}
	}

	seen := make(map[string]bool)
	// SQS returns at most 10 per call — we need multiple rounds.
	for attempt := range 10 {
		jobs, err := svc.Dequeue(ctx, queueURL, 10, 2*time.Second)
		if err != nil {
			t.Fatalf("Dequeue attempt %d: %v", attempt, err)
		}
		for _, j := range jobs {
			seen[string(j.Body)] = true
			if err := svc.Ack(ctx, queueURL, j); err != nil {
				t.Fatalf("Ack: %v", err)
			}
		}
		if len(seen) == total {
			break
		}
	}

	if len(seen) != total {
		t.Errorf("received %d/%d messages", len(seen), total)
	}
	for i := range total {
		key := fmt.Sprintf("msg-%d", i)
		if !seen[key] {
			t.Errorf("missing message %q", key)
		}
	}
}

// --- delay: message with DelaySeconds should not be immediately visible ---

func TestIntegration_DelaySeconds(t *testing.T) {
	endpoint := setupElasticMQOnce(t)
	queueURL := uniqueQueue(t, endpoint)
	svc := newIntegrationService(t, endpoint)
	ctx := context.Background()

	if err := svc.Enqueue(ctx, queueURL, []byte("delayed"), gas.WithDelay(3*time.Second)); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	// Immediately dequeue — should get nothing (message is delayed).
	jobs, err := svc.Dequeue(ctx, queueURL, 1, time.Second)
	if err != nil {
		t.Fatalf("Dequeue (immediate): %v", err)
	}
	if len(jobs) != 0 {
		t.Errorf("got %d jobs immediately, want 0 (message has 3s delay)", len(jobs))
	}

	// Wait for delay to expire, then dequeue.
	time.Sleep(3 * time.Second)

	jobs, err = svc.Dequeue(ctx, queueURL, 1, 2*time.Second)
	if err != nil {
		t.Fatalf("Dequeue (after delay): %v", err)
	}
	if len(jobs) != 1 {
		t.Fatalf("got %d jobs after delay, want 1", len(jobs))
	}
	if string(jobs[0].Body) != "delayed" {
		t.Errorf("Body = %q, want %q", string(jobs[0].Body), "delayed")
	}
}

// --- double ack: second ack on same receipt handle should error ---

func TestIntegration_DoubleAck(t *testing.T) {
	endpoint := setupElasticMQOnce(t)
	queueURL := uniqueQueue(t, endpoint)
	svc := newIntegrationService(t, endpoint)
	ctx := context.Background()

	if err := svc.Enqueue(ctx, queueURL, []byte("double-ack")); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	jobs, err := svc.Dequeue(ctx, queueURL, 1, time.Second)
	if err != nil {
		t.Fatalf("Dequeue: %v", err)
	}
	if len(jobs) != 1 {
		t.Fatalf("got %d jobs, want 1", len(jobs))
	}

	if err := svc.Ack(ctx, queueURL, jobs[0]); err != nil {
		t.Fatalf("first Ack: %v", err)
	}

	// SQS silently accepts duplicate deletes; ElasticMQ rejects them
	// with ReceiptHandleIsInvalid. Both behaviors are acceptable — the
	// important thing is the service doesn't panic or corrupt state.
	if err := svc.Ack(ctx, queueURL, jobs[0]); err != nil {
		t.Logf("second Ack returned error (expected on ElasticMQ): %v", err)
	}
}

// --- empty queue returns zero jobs, not an error ---

func TestIntegration_DequeueEmpty(t *testing.T) {
	endpoint := setupElasticMQOnce(t)
	queueURL := uniqueQueue(t, endpoint)
	svc := newIntegrationService(t, endpoint)
	ctx := context.Background()

	jobs, err := svc.Dequeue(ctx, queueURL, 10, time.Second)
	if err != nil {
		t.Fatalf("Dequeue empty queue: %v", err)
	}
	if len(jobs) != 0 {
		t.Errorf("got %d jobs from empty queue, want 0", len(jobs))
	}
}

// --- large payload near SQS 256KB limit ---

func TestIntegration_LargePayload(t *testing.T) {
	endpoint := setupElasticMQOnce(t)
	queueURL := uniqueQueue(t, endpoint)
	svc := newIntegrationService(t, endpoint)
	ctx := context.Background()

	// SQS max is 256KB. Use 200KB to stay safely under.
	payload := []byte(strings.Repeat("X", 200*1024))
	if err := svc.Enqueue(ctx, queueURL, payload); err != nil {
		t.Fatalf("Enqueue large payload: %v", err)
	}

	jobs, err := svc.Dequeue(ctx, queueURL, 1, 2*time.Second)
	if err != nil {
		t.Fatalf("Dequeue: %v", err)
	}
	if len(jobs) != 1 {
		t.Fatalf("got %d jobs, want 1", len(jobs))
	}
	if len(jobs[0].Body) != len(payload) {
		t.Errorf("body length = %d, want %d", len(jobs[0].Body), len(payload))
	}
}

// --- oversized payload should be rejected by SQS ---
// NOTE: ElasticMQ does not enforce the 256KB message size limit.
// This test documents the expected behavior against real SQS.

func TestIntegration_OversizedPayload(t *testing.T) {
	endpoint := setupElasticMQOnce(t)
	queueURL := uniqueQueue(t, endpoint)
	svc := newIntegrationService(t, endpoint)
	ctx := context.Background()

	// 300KB — over the 256KB SQS limit.
	payload := []byte(strings.Repeat("X", 300*1024))
	err := svc.Enqueue(ctx, queueURL, payload)

	// Real SQS rejects this; ElasticMQ accepts it.
	// We log the behavior rather than hard-failing so the test is
	// informative in both environments.
	if err != nil {
		t.Logf("oversized payload rejected (expected on real SQS): %v", err)
	} else {
		t.Log("oversized payload accepted (ElasticMQ does not enforce 256KB limit)")
	}
}

// --- empty body ---

func TestIntegration_EmptyBody(t *testing.T) {
	endpoint := setupElasticMQOnce(t)
	queueURL := uniqueQueue(t, endpoint)
	svc := newIntegrationService(t, endpoint)
	ctx := context.Background()

	// SQS requires a non-empty MessageBody — an empty string should fail.
	err := svc.Enqueue(ctx, queueURL, []byte(""))
	if err == nil {
		t.Fatal("Enqueue with empty body should have failed")
	}
}

// --- concurrent producers ---

func TestIntegration_ConcurrentEnqueue(t *testing.T) {
	endpoint := setupElasticMQOnce(t)
	queueURL := uniqueQueue(t, endpoint)
	svc := newIntegrationService(t, endpoint)
	ctx := context.Background()

	const goroutines = 10
	const perGoroutine = 5
	total := goroutines * perGoroutine

	var wg sync.WaitGroup
	errs := make(chan error, total)

	for g := range goroutines {
		wg.Add(1)
		go func(gid int) {
			defer wg.Done()
			for i := range perGoroutine {
				payload := fmt.Sprintf("g%d-m%d", gid, i)
				if err := svc.Enqueue(ctx, queueURL, []byte(payload)); err != nil {
					errs <- fmt.Errorf("g%d-m%d: %w", gid, i, err)
				}
			}
		}(g)
	}
	wg.Wait()
	close(errs)

	for err := range errs {
		t.Errorf("concurrent enqueue error: %v", err)
	}

	// Drain and count.
	seen := 0
	for attempt := range 20 {
		jobs, err := svc.Dequeue(ctx, queueURL, 10, 2*time.Second)
		if err != nil {
			t.Fatalf("Dequeue attempt %d: %v", attempt, err)
		}
		for _, j := range jobs {
			seen++
			if err := svc.Ack(ctx, queueURL, j); err != nil {
				t.Fatalf("Ack: %v", err)
			}
		}
		if seen == total {
			break
		}
	}
	if seen != total {
		t.Errorf("received %d/%d messages", seen, total)
	}
}

// --- FIFO queue: ordering + deduplication ---

func TestIntegration_FIFOQueue(t *testing.T) {
	endpoint := setupElasticMQOnce(t)
	queueURL := uniqueFIFOQueue(t, endpoint)
	svc := newIntegrationService(t, endpoint)
	ctx := context.Background()

	// Send 5 ordered messages in the same group.
	for i := range 5 {
		payload := fmt.Sprintf("fifo-%d", i)
		err := svc.Enqueue(ctx, queueURL, []byte(payload),
			gas.WithGroupID("group-1"),
			gas.WithDedupeID(fmt.Sprintf("dedup-%d", i)),
		)
		if err != nil {
			t.Fatalf("Enqueue fifo-%d: %v", i, err)
		}
	}

	// FIFO queues guarantee ordering within a message group.
	var received []string
	for attempt := range 10 {
		jobs, err := svc.Dequeue(ctx, queueURL, 10, 2*time.Second)
		if err != nil {
			t.Fatalf("Dequeue attempt %d: %v", attempt, err)
		}
		for _, j := range jobs {
			received = append(received, string(j.Body))
			if err := svc.Ack(ctx, queueURL, j); err != nil {
				t.Fatalf("Ack: %v", err)
			}
		}
		if len(received) == 5 {
			break
		}
	}

	if len(received) != 5 {
		t.Fatalf("got %d messages, want 5", len(received))
	}
	for i, body := range received {
		want := fmt.Sprintf("fifo-%d", i)
		if body != want {
			t.Errorf("message[%d] = %q, want %q (ordering broken)", i, body, want)
		}
	}
}

// --- FIFO deduplication: same deduplication ID should be silently dropped ---

func TestIntegration_FIFODeduplication(t *testing.T) {
	endpoint := setupElasticMQOnce(t)
	queueURL := uniqueFIFOQueue(t, endpoint)
	svc := newIntegrationService(t, endpoint)
	ctx := context.Background()

	// Send the same deduplication ID twice.
	for range 2 {
		err := svc.Enqueue(ctx, queueURL, []byte("dedupe-test"),
			gas.WithGroupID("group-1"),
			gas.WithDedupeID("same-id"),
		)
		if err != nil {
			t.Fatalf("Enqueue: %v", err)
		}
	}

	// Only one message should be delivered.
	var count int
	for attempt := range 3 {
		jobs, err := svc.Dequeue(ctx, queueURL, 10, 2*time.Second)
		if err != nil {
			t.Fatalf("Dequeue attempt %d: %v", attempt, err)
		}
		for _, j := range jobs {
			count++
			if err := svc.Ack(ctx, queueURL, j); err != nil {
				t.Fatalf("Ack: %v", err)
			}
		}
		if count > 0 && len(jobs) == 0 {
			break
		}
	}

	if count != 1 {
		t.Errorf("got %d messages, want 1 (deduplication failed)", count)
	}
}

// --- system attributes (e.g. SentTimestamp) are populated ---

func TestIntegration_SystemAttributes(t *testing.T) {
	endpoint := setupElasticMQOnce(t)
	queueURL := uniqueQueue(t, endpoint)
	svc := newIntegrationService(t, endpoint)
	ctx := context.Background()

	if err := svc.Enqueue(ctx, queueURL, []byte("sys-attrs")); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	jobs, err := svc.Dequeue(ctx, queueURL, 1, time.Second)
	if err != nil {
		t.Fatalf("Dequeue: %v", err)
	}
	if len(jobs) != 1 {
		t.Fatalf("got %d jobs, want 1", len(jobs))
	}

	// SQS always populates SentTimestamp as a system attribute.
	ts, ok := jobs[0].Attributes["SentTimestamp"]
	if !ok {
		t.Error("SentTimestamp not found in Attributes")
	} else if ts == "" {
		t.Error("SentTimestamp is empty")
	}

	// ApproximateReceiveCount should be "1" on first receive.
	rc, ok := jobs[0].Attributes["ApproximateReceiveCount"]
	if !ok {
		t.Error("ApproximateReceiveCount not found in Attributes")
	} else if rc != "1" {
		t.Errorf("ApproximateReceiveCount = %q, want %q", rc, "1")
	}
}

// --- context cancellation ---

func TestIntegration_ContextCancelled(t *testing.T) {
	endpoint := setupElasticMQOnce(t)
	queueURL := uniqueQueue(t, endpoint)
	svc := newIntegrationService(t, endpoint)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	err := svc.Enqueue(ctx, queueURL, []byte("should-fail"))
	if err == nil {
		t.Fatal("Enqueue with cancelled context should fail")
	}

	_, err = svc.Dequeue(ctx, queueURL, 1, time.Second)
	if err == nil {
		t.Fatal("Dequeue with cancelled context should fail")
	}
}

// --- nack then ack: full retry cycle ---

func TestIntegration_NackThenAck(t *testing.T) {
	endpoint := setupElasticMQOnce(t)
	queueURL := uniqueQueue(t, endpoint)
	svc := newIntegrationService(t, endpoint)
	ctx := context.Background()

	if err := svc.Enqueue(ctx, queueURL, []byte("retry-me")); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	// First consume — nack it.
	jobs, err := svc.Dequeue(ctx, queueURL, 1, time.Second)
	if err != nil {
		t.Fatalf("Dequeue (first): %v", err)
	}
	if len(jobs) != 1 {
		t.Fatalf("got %d jobs, want 1", len(jobs))
	}
	if err := svc.Nack(ctx, queueURL, jobs[0]); err != nil {
		t.Fatalf("Nack: %v", err)
	}

	// Second consume — ack it.
	jobs, err = svc.Dequeue(ctx, queueURL, 1, 2*time.Second)
	if err != nil {
		t.Fatalf("Dequeue (second): %v", err)
	}
	if len(jobs) != 1 {
		t.Fatalf("got %d jobs on retry, want 1", len(jobs))
	}
	if string(jobs[0].Body) != "retry-me" {
		t.Errorf("Body = %q, want %q", string(jobs[0].Body), "retry-me")
	}
	if err := svc.Ack(ctx, queueURL, jobs[0]); err != nil {
		t.Fatalf("Ack: %v", err)
	}

	// Should be gone now.
	remaining, err := svc.Dequeue(ctx, queueURL, 1, time.Second)
	if err != nil {
		t.Fatalf("Dequeue (final): %v", err)
	}
	if len(remaining) != 0 {
		t.Errorf("got %d jobs after ack, want 0", len(remaining))
	}
}

// --- Client() returns a usable *sqs.Client for advanced ops ---

func TestIntegration_ClientAccessor(t *testing.T) {
	endpoint := setupElasticMQOnce(t)
	svc := newIntegrationService(t, endpoint)

	client := svc.Client()
	if client == nil {
		t.Fatal("Client() returned nil")
	}

	// Use it to create a queue directly — proves it's wired to ElasticMQ.
	out, err := client.CreateQueue(context.Background(), &awssqs.CreateQueueInput{
		QueueName: aws.String("client-accessor-test"),
	})
	if err != nil {
		t.Fatalf("CreateQueue via Client(): %v", err)
	}
	if out.QueueUrl == nil || *out.QueueUrl == "" {
		t.Error("CreateQueue returned empty URL")
	}
}

// --- special characters in payload ---

func TestIntegration_SpecialCharPayload(t *testing.T) {
	endpoint := setupElasticMQOnce(t)
	queueURL := uniqueQueue(t, endpoint)
	svc := newIntegrationService(t, endpoint)
	ctx := context.Background()

	payloads := []string{
		`<xml>hello & "world"</xml>`,
		"line1\nline2\ttab",
		`{"emoji":"🔥","unicode":"日本語"}`,
		strings.Repeat("嗨", 1000),
	}

	for _, p := range payloads {
		if err := svc.Enqueue(ctx, queueURL, []byte(p)); err != nil {
			t.Fatalf("Enqueue %q: %v", p[:20], err)
		}
	}

	seen := make(map[string]bool)
	for attempt := range 10 {
		jobs, err := svc.Dequeue(ctx, queueURL, 10, 2*time.Second)
		if err != nil {
			t.Fatalf("Dequeue attempt %d: %v", attempt, err)
		}
		for _, j := range jobs {
			seen[string(j.Body)] = true
			if err := svc.Ack(ctx, queueURL, j); err != nil {
				t.Fatalf("Ack: %v", err)
			}
		}
		if len(seen) == len(payloads) {
			break
		}
	}

	for _, p := range payloads {
		if !seen[p] {
			t.Errorf("payload not received: %q", p[:20])
		}
	}
}
