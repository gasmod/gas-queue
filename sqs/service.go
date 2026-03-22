package sqs

import (
	"context"
	"fmt"
	"math"
	"sync/atomic"
	"time"

	"github.com/gasmod/gas"
	queue "github.com/gasmod/gas-queue"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

const serviceName = "gas-queue-sqs"

// sqsClient is the subset of the AWS SQS client API used by this service.
// The real *sqs.Client satisfies this interface.
type sqsClient interface {
	SendMessage(ctx context.Context, params *awssqs.SendMessageInput, optFns ...func(*awssqs.Options)) (*awssqs.SendMessageOutput, error)
	ReceiveMessage(ctx context.Context, params *awssqs.ReceiveMessageInput, optFns ...func(*awssqs.Options)) (*awssqs.ReceiveMessageOutput, error)
	DeleteMessage(ctx context.Context, params *awssqs.DeleteMessageInput, optFns ...func(*awssqs.Options)) (*awssqs.DeleteMessageOutput, error)
	ChangeMessageVisibility(ctx context.Context, params *awssqs.ChangeMessageVisibilityInput, optFns ...func(*awssqs.Options)) (*awssqs.ChangeMessageVisibilityOutput, error)
}

// Service is an SQS-backed queue implementing gas.Service and
// gas.JobQueueProvider.
type Service struct {
	client sqsClient
	cfg    *Config
	logger gas.Logger

	cfgProvider          gas.ConfigProvider
	customConfigProvided bool
	customClientProvided bool
	closed               atomic.Bool
}

var _ gas.Service = (*Service)(nil)
var _ gas.JobQueueProvider = (*Service)(nil)

// Client returns the underlying *sqs.Client for advanced operations
// beyond the JobQueueProvider interface (e.g. CreateQueue, PurgeQueue,
// GetQueueAttributes). Returns nil if a custom sqsClient was injected
// via WithClient that is not an *sqs.Client.
func (s *Service) Client() *awssqs.Client {
	c, _ := s.client.(*awssqs.Client)
	return c
}

// Option configures a Service.
type Option func(*Service)

// WithConfig sets a custom configuration.
func WithConfig(cfg *Config) Option {
	return func(s *Service) {
		s.cfg = cfg
		s.customConfigProvided = true
	}
}

// WithClient injects a pre-configured SQS client. Useful for testing
// or when the caller manages AWS credentials externally.
func WithClient(client sqsClient) Option {
	return func(s *Service) {
		s.client = client
		s.customClientProvided = true
	}
}

// New captures options and returns a DI-injectable constructor.
func New(opts ...Option) func(gas.ConfigProvider, gas.Logger) *Service {
	return func(cfgProvider gas.ConfigProvider, logger gas.Logger) *Service {
		s := &Service{
			cfg:         DefaultConfig(),
			cfgProvider: cfgProvider,
			logger:      logger.With().Str("service", serviceName).Logger(),
		}
		for _, opt := range opts {
			opt(s)
		}
		return s
	}
}

// Name returns the service identifier.
func (s *Service) Name() string { return serviceName }

// Init validates the configuration and creates the SQS client.
func (s *Service) Init() error {
	if !s.customConfigProvided {
		if s.cfgProvider != nil {
			if err := s.cfgProvider.Bind(s.cfg); err != nil {
				return fmt.Errorf("%s: config binding: %w", s.Name(), err)
			}
		}
	}

	if err := s.cfg.Validate(); err != nil {
		s.logger.Error("invalid queue configuration").Err("error", err).Send()
		return err
	}

	if !s.customClientProvided {
		if err := s.createClient(); err != nil {
			return err
		}
	}

	s.closed.Store(false)
	s.logger.Info("sqs queue initialized").Str("region", s.cfg.Queue.Region).Send()
	return nil
}

func (s *Service) createClient() error {
	opts := []func(*awsconfig.LoadOptions) error{
		awsconfig.WithRegion(s.cfg.Queue.Region),
	}

	awsCfg, err := awsconfig.LoadDefaultConfig(context.Background(), opts...)
	if err != nil {
		return fmt.Errorf("%s: load aws config: %w", s.Name(), err)
	}

	var sqsOpts []func(*awssqs.Options)
	if s.cfg.Queue.Endpoint != "" {
		sqsOpts = append(sqsOpts, func(o *awssqs.Options) {
			o.BaseEndpoint = aws.String(s.cfg.Queue.Endpoint)
		})
	}

	s.client = awssqs.NewFromConfig(awsCfg, sqsOpts...)
	return nil
}

// Close marks the service as closed.
func (s *Service) Close() error {
	s.closed.Store(true)
	s.logger.Info("sqs queue closed").Send()
	return nil
}

// Enqueue sends a message to the specified SQS queue.
func (s *Service) Enqueue(ctx context.Context, queueURL string, payload []byte, opts ...gas.EnqueueOption) error {
	if s.closed.Load() {
		return queue.ErrClosed
	}

	delay, groupID, dedupeID, attrs := gas.ApplyEnqueueOptions(opts)

	input := &awssqs.SendMessageInput{
		QueueUrl:    &queueURL,
		MessageBody: aws.String(string(payload)),
	}

	if delay > 0 {
		input.DelaySeconds = int32(delay.Seconds())
	}
	if groupID != "" {
		input.MessageGroupId = &groupID
	}
	if dedupeID != "" {
		input.MessageDeduplicationId = &dedupeID
	}
	if len(attrs) > 0 {
		input.MessageAttributes = make(map[string]types.MessageAttributeValue, len(attrs))
		for k, v := range attrs {
			input.MessageAttributes[k] = types.MessageAttributeValue{
				DataType:    aws.String("String"),
				StringValue: aws.String(v),
			}
		}
	}

	if _, err := s.client.SendMessage(ctx, input); err != nil {
		return fmt.Errorf("%s: enqueue: %w", s.Name(), err)
	}
	return nil
}

// Dequeue receives messages from the specified SQS queue.
func (s *Service) Dequeue(ctx context.Context, queueURL string, maxMessages int, wait time.Duration) ([]gas.Job, error) {
	if s.closed.Load() {
		return nil, queue.ErrClosed
	}

	maxNumMessages, waitSeconds, visibilityTimeout, err := s.receiveMessageParams(maxMessages, wait)
	if err != nil {
		return nil, err
	}

	input := &awssqs.ReceiveMessageInput{
		QueueUrl:                    &queueURL,
		MaxNumberOfMessages:         maxNumMessages,
		WaitTimeSeconds:             waitSeconds,
		VisibilityTimeout:           visibilityTimeout,
		MessageAttributeNames:       []string{"All"},
		MessageSystemAttributeNames: []types.MessageSystemAttributeName{types.MessageSystemAttributeNameAll},
	}

	out, err := s.client.ReceiveMessage(ctx, input)
	if err != nil {
		return nil, fmt.Errorf("%s: dequeue: %w", s.Name(), err)
	}

	jobs := make([]gas.Job, 0, len(out.Messages))
	for _, msg := range out.Messages {
		job := gas.Job{
			ID:            aws.ToString(msg.MessageId),
			ReceiptHandle: aws.ToString(msg.ReceiptHandle),
			Body:          []byte(aws.ToString(msg.Body)),
		}

		// Merge system attributes and message attributes.
		if len(msg.Attributes) > 0 || len(msg.MessageAttributes) > 0 {
			job.Attributes = make(map[string]string, len(msg.Attributes)+len(msg.MessageAttributes))
			for k, v := range msg.Attributes {
				job.Attributes[k] = v
			}
			for k, v := range msg.MessageAttributes {
				if v.StringValue != nil {
					job.Attributes[k] = *v.StringValue
				}
			}
		}

		jobs = append(jobs, job)
	}

	return jobs, nil
}

func (s *Service) receiveMessageParams(maxMessages int, wait time.Duration) (maxNumMessages, waitSeconds, visibilityTimeout int32, err error) {
	// Clamp maxMessages to SQS limits (1-10).
	if maxMessages < 1 {
		maxMessages = 1
	}
	if maxMessages > 10 {
		s.logger.Warn("maxMessages clamped to 10 (SQS limit)").Int("requested", maxMessages).Send()
		maxMessages = 10
	}

	waitSeconds, err = safeInt32(wait.Seconds())
	if err != nil {
		err = fmt.Errorf("invalid wait duration: %w", err)
		return
	}

	if waitSeconds == 0 {
		waitSeconds, err = safeInt32(s.cfg.Queue.WaitTimeSeconds)
		if err != nil {
			err = fmt.Errorf("invalid wait time: %w", err)
			return
		}
	}

	maxNumMessages, err = safeInt32(maxMessages)
	if err != nil {
		err = fmt.Errorf("invalid maxMessages: %w", err)
		return
	}

	visibilityTimeout, err = safeInt32(s.cfg.Queue.VisibilityTimeout.Seconds())
	if err != nil {
		err = fmt.Errorf("invalid visibility timeout: %w", err)
		return
	}

	return
}

// Ack acknowledges a job by deleting it from the queue.
func (s *Service) Ack(ctx context.Context, queueURL string, job gas.Job) error {
	if s.closed.Load() {
		return queue.ErrClosed
	}

	_, err := s.client.DeleteMessage(ctx, &awssqs.DeleteMessageInput{
		QueueUrl:      &queueURL,
		ReceiptHandle: &job.ReceiptHandle,
	})
	if err != nil {
		return fmt.Errorf("%s: ack: %w", s.Name(), err)
	}
	return nil
}

// Nack makes a job immediately available for reprocessing by setting
// its visibility timeout to zero.
func (s *Service) Nack(ctx context.Context, queueURL string, job gas.Job) error {
	if s.closed.Load() {
		return queue.ErrClosed
	}

	_, err := s.client.ChangeMessageVisibility(ctx, &awssqs.ChangeMessageVisibilityInput{
		QueueUrl:          &queueURL,
		ReceiptHandle:     &job.ReceiptHandle,
		VisibilityTimeout: 0,
	})
	if err != nil {
		return fmt.Errorf("%s: nack: %w", s.Name(), err)
	}
	return nil
}

// safeInt32 converts an int/float to int32, returning an error if the value overflows the range of int32.
func safeInt32[T int | int32 | int64 | float32 | float64](n T) (int32, error) {
	if n > math.MaxInt32 || n < math.MinInt32 {
		return 0, fmt.Errorf("safeInt32: value %v overflows int32", n)
	}
	return int32(n), nil
}
