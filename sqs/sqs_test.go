package sqs

import (
	"context"
	"errors"
	"testing"
	"time"

	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/gasmod/gas"
	queue "github.com/gasmod/gas-queue"
)

// --- mock SQS client ---

type mockSQSClient struct {
	sendMessageFn             func(ctx context.Context, params *awssqs.SendMessageInput, optFns ...func(*awssqs.Options)) (*awssqs.SendMessageOutput, error)
	receiveMessageFn          func(ctx context.Context, params *awssqs.ReceiveMessageInput, optFns ...func(*awssqs.Options)) (*awssqs.ReceiveMessageOutput, error)
	deleteMessageFn           func(ctx context.Context, params *awssqs.DeleteMessageInput, optFns ...func(*awssqs.Options)) (*awssqs.DeleteMessageOutput, error)
	changeMessageVisibilityFn func(ctx context.Context, params *awssqs.ChangeMessageVisibilityInput, optFns ...func(*awssqs.Options)) (*awssqs.ChangeMessageVisibilityOutput, error)
}

func (m *mockSQSClient) SendMessage(ctx context.Context, params *awssqs.SendMessageInput, optFns ...func(*awssqs.Options)) (*awssqs.SendMessageOutput, error) {
	return m.sendMessageFn(ctx, params, optFns...)
}

func (m *mockSQSClient) ReceiveMessage(ctx context.Context, params *awssqs.ReceiveMessageInput, optFns ...func(*awssqs.Options)) (*awssqs.ReceiveMessageOutput, error) {
	return m.receiveMessageFn(ctx, params, optFns...)
}

func (m *mockSQSClient) DeleteMessage(ctx context.Context, params *awssqs.DeleteMessageInput, optFns ...func(*awssqs.Options)) (*awssqs.DeleteMessageOutput, error) {
	return m.deleteMessageFn(ctx, params, optFns...)
}

func (m *mockSQSClient) ChangeMessageVisibility(ctx context.Context, params *awssqs.ChangeMessageVisibilityInput, optFns ...func(*awssqs.Options)) (*awssqs.ChangeMessageVisibilityOutput, error) {
	return m.changeMessageVisibilityFn(ctx, params, optFns...)
}

// --- helpers ---

func newTestService(t *testing.T, mock *mockSQSClient) *Service {
	t.Helper()
	ctor := New(WithConfig(DefaultConfig()), WithClient(mock))
	svc := ctor(nil, gas.NewNopLogger()())
	if err := svc.Init(); err != nil {
		t.Fatalf("init: %v", err)
	}
	return svc
}

// --- config tests ---

func TestDefaultConfig(t *testing.T) {
	t.Parallel()
	cfg := DefaultConfig()

	if cfg.Queue.Region != defaultRegion {
		t.Errorf("Region = %q, want %q", cfg.Queue.Region, defaultRegion)
	}
	if cfg.Queue.VisibilityTimeout != defaultVisibilityTimeout {
		t.Errorf("VisibilityTimeout = %v, want %v", cfg.Queue.VisibilityTimeout, defaultVisibilityTimeout)
	}
	if cfg.Queue.WaitTimeSeconds != defaultWaitTimeSeconds {
		t.Errorf("WaitTimeSeconds = %d, want %d", cfg.Queue.WaitTimeSeconds, defaultWaitTimeSeconds)
	}
}

func TestConfigValidate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		modify  func(*Config)
		wantErr bool
	}{
		{name: "valid defaults", modify: func(_ *Config) {}, wantErr: false},
		{name: "empty region", modify: func(c *Config) { c.Queue.Region = "" }, wantErr: true},
		{name: "wait too low", modify: func(c *Config) { c.Queue.WaitTimeSeconds = -1 }, wantErr: true},
		{name: "wait too high", modify: func(c *Config) { c.Queue.WaitTimeSeconds = 21 }, wantErr: true},
		{name: "negative visibility", modify: func(c *Config) { c.Queue.VisibilityTimeout = -1 }, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			cfg := DefaultConfig()
			tt.modify(cfg)
			err := cfg.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

// --- enqueue tests ---

func TestEnqueue(t *testing.T) {
	t.Parallel()
	var captured *awssqs.SendMessageInput

	mock := &mockSQSClient{
		sendMessageFn: func(_ context.Context, params *awssqs.SendMessageInput, _ ...func(*awssqs.Options)) (*awssqs.SendMessageOutput, error) {
			captured = params
			return &awssqs.SendMessageOutput{MessageId: new("msg-1")}, nil
		},
	}

	svc := newTestService(t, mock)
	err := svc.Enqueue(context.Background(), "https://sqs.us-east-1.amazonaws.com/123/test-queue", []byte(`{"task":"run"}`))
	if err != nil {
		t.Fatalf("Enqueue() error = %v", err)
	}

	if *captured.QueueUrl != "https://sqs.us-east-1.amazonaws.com/123/test-queue" {
		t.Errorf("QueueUrl = %q", *captured.QueueUrl)
	}
	if *captured.MessageBody != `{"task":"run"}` {
		t.Errorf("MessageBody = %q", *captured.MessageBody)
	}
}

func TestEnqueueWithOptions(t *testing.T) {
	t.Parallel()
	var captured *awssqs.SendMessageInput

	mock := &mockSQSClient{
		sendMessageFn: func(_ context.Context, params *awssqs.SendMessageInput, _ ...func(*awssqs.Options)) (*awssqs.SendMessageOutput, error) {
			captured = params
			return &awssqs.SendMessageOutput{MessageId: new("msg-2")}, nil
		},
	}

	svc := newTestService(t, mock)
	err := svc.Enqueue(context.Background(), "https://sqs/q",
		[]byte("body"),
		gas.WithDelay(5*time.Second),
		gas.WithGroupID("grp-1"),
		gas.WithDedupeID("dup-1"),
		gas.WithJobAttributes(map[string]string{"env": "test"}),
	)
	if err != nil {
		t.Fatalf("Enqueue() error = %v", err)
	}

	if captured.DelaySeconds != 5 {
		t.Errorf("DelaySeconds = %d, want 5", captured.DelaySeconds)
	}
	if *captured.MessageGroupId != "grp-1" {
		t.Errorf("MessageGroupId = %q", *captured.MessageGroupId)
	}
	if *captured.MessageDeduplicationId != "dup-1" {
		t.Errorf("MessageDeduplicationId = %q", *captured.MessageDeduplicationId)
	}
	attr, ok := captured.MessageAttributes["env"]
	if !ok {
		t.Fatal("missing message attribute 'env'")
	}
	if *attr.StringValue != "test" {
		t.Errorf("attr env = %q", *attr.StringValue)
	}
}

func TestEnqueueClosed(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, &mockSQSClient{})
	_ = svc.Close()

	err := svc.Enqueue(context.Background(), "https://sqs/q", []byte("x"))
	if !errors.Is(err, queue.ErrClosed) {
		t.Errorf("got %v, want ErrClosed", err)
	}
}

// --- dequeue tests ---

func TestDequeue(t *testing.T) {
	t.Parallel()

	mock := &mockSQSClient{
		receiveMessageFn: func(_ context.Context, params *awssqs.ReceiveMessageInput, _ ...func(*awssqs.Options)) (*awssqs.ReceiveMessageOutput, error) {
			return &awssqs.ReceiveMessageOutput{
				Messages: []types.Message{
					{
						MessageId:     new("id-1"),
						ReceiptHandle: new("rh-1"),
						Body:          new(`{"job":1}`),
						MessageAttributes: map[string]types.MessageAttributeValue{
							"priority": {StringValue: new("high")},
						},
					},
				},
			}, nil
		},
	}

	svc := newTestService(t, mock)
	jobs, err := svc.Dequeue(context.Background(), "https://sqs/q", 5, 10*time.Second)
	if err != nil {
		t.Fatalf("Dequeue() error = %v", err)
	}
	if len(jobs) != 1 {
		t.Fatalf("got %d jobs, want 1", len(jobs))
	}

	j := jobs[0]
	if j.ID != "id-1" {
		t.Errorf("ID = %q", j.ID)
	}
	if j.ReceiptHandle != "rh-1" {
		t.Errorf("ReceiptHandle = %q", j.ReceiptHandle)
	}
	if string(j.Body) != `{"job":1}` {
		t.Errorf("Body = %q", string(j.Body))
	}
	if j.Attributes["priority"] != "high" {
		t.Errorf("Attributes[priority] = %q", j.Attributes["priority"])
	}
}

func TestDequeueEmpty(t *testing.T) {
	t.Parallel()

	mock := &mockSQSClient{
		receiveMessageFn: func(_ context.Context, _ *awssqs.ReceiveMessageInput, _ ...func(*awssqs.Options)) (*awssqs.ReceiveMessageOutput, error) {
			return &awssqs.ReceiveMessageOutput{}, nil
		},
	}

	svc := newTestService(t, mock)
	jobs, err := svc.Dequeue(context.Background(), "https://sqs/q", 1, 0)
	if err != nil {
		t.Fatalf("Dequeue() error = %v", err)
	}
	if len(jobs) != 0 {
		t.Errorf("got %d jobs, want 0", len(jobs))
	}
}

func TestDequeueClamps(t *testing.T) {
	t.Parallel()
	var captured *awssqs.ReceiveMessageInput

	mock := &mockSQSClient{
		receiveMessageFn: func(_ context.Context, params *awssqs.ReceiveMessageInput, _ ...func(*awssqs.Options)) (*awssqs.ReceiveMessageOutput, error) {
			captured = params
			return &awssqs.ReceiveMessageOutput{}, nil
		},
	}

	svc := newTestService(t, mock)
	_, err := svc.Dequeue(context.Background(), "https://sqs/q", 50, time.Second)
	if err != nil {
		t.Fatalf("Dequeue() error = %v", err)
	}
	if captured.MaxNumberOfMessages != 10 {
		t.Errorf("MaxNumberOfMessages = %d, want 10", captured.MaxNumberOfMessages)
	}
}

// --- ack tests ---

func TestAck(t *testing.T) {
	t.Parallel()
	var captured *awssqs.DeleteMessageInput

	mock := &mockSQSClient{
		deleteMessageFn: func(_ context.Context, params *awssqs.DeleteMessageInput, _ ...func(*awssqs.Options)) (*awssqs.DeleteMessageOutput, error) {
			captured = params
			return &awssqs.DeleteMessageOutput{}, nil
		},
	}

	svc := newTestService(t, mock)
	err := svc.Ack(context.Background(), "https://sqs/q", gas.Job{
		ID:            "id-1",
		ReceiptHandle: "rh-1",
	})
	if err != nil {
		t.Fatalf("Ack() error = %v", err)
	}
	if *captured.ReceiptHandle != "rh-1" {
		t.Errorf("ReceiptHandle = %q", *captured.ReceiptHandle)
	}
}

// --- nack tests ---

func TestNack(t *testing.T) {
	t.Parallel()
	var captured *awssqs.ChangeMessageVisibilityInput

	mock := &mockSQSClient{
		changeMessageVisibilityFn: func(_ context.Context, params *awssqs.ChangeMessageVisibilityInput, _ ...func(*awssqs.Options)) (*awssqs.ChangeMessageVisibilityOutput, error) {
			captured = params
			return &awssqs.ChangeMessageVisibilityOutput{}, nil
		},
	}

	svc := newTestService(t, mock)
	err := svc.Nack(context.Background(), "https://sqs/q", gas.Job{
		ID:            "id-1",
		ReceiptHandle: "rh-1",
	})
	if err != nil {
		t.Fatalf("Nack() error = %v", err)
	}
	if *captured.ReceiptHandle != "rh-1" {
		t.Errorf("ReceiptHandle = %q", *captured.ReceiptHandle)
	}
	if captured.VisibilityTimeout != 0 {
		t.Errorf("VisibilityTimeout = %d, want 0", captured.VisibilityTimeout)
	}
}

func TestNackClosed(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, &mockSQSClient{})
	_ = svc.Close()

	err := svc.Nack(context.Background(), "https://sqs/q", gas.Job{ReceiptHandle: "rh"})
	if !errors.Is(err, queue.ErrClosed) {
		t.Errorf("got %v, want ErrClosed", err)
	}
}
