// Package kafka publishes outbox events to Kafka.
//
// The producer is deliberately thin. All durability decisions belong to the outbox table
// and the relayer; the producer's only job is to hand a message to the broker and report
// whether that attempt is worth retrying. See ADR 0002.
package kafka

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// Producer wraps the underlying client with the platform's delivery semantics.
type Producer struct {
	producer *kafka.Producer
	enabled  bool
	timeout  time.Duration
}

// NewProducer creates a producer configured for at-least-once, lossless delivery.
//
// Idempotence plus RequiredAcks=All means the broker acknowledges only once the record
// is written to every in-sync replica. Without both, a leader failover between the ack and
// the client's next read can silently lose a payment event.
//
// A single in-flight request per partition is enforced so the idempotent producer's
// ordering guarantee holds.
func NewProducer(brokers []string, clientID string, enabled bool, timeout time.Duration) (*Producer, error) {
	if !enabled {
		return &Producer{enabled: false, timeout: timeout}, nil
	}
	if len(brokers) == 0 {
		return nil, errors.New("no Kafka brokers configured")
	}

	cfg := &kafka.ConfigMap{
		"bootstrap.servers": joinBrokers(brokers),
		"client.id":         clientID,

		// RequiredAcks=-1 means "all in-sync replicas". On the single-broker local stack
		// this resolves to one replica; it becomes a real redundancy requirement as
		// brokers are added.
		"acks": "all",

		// Enables the producer idempotence protocol, which requires acks=all and a
		// maximum of one in-flight request per partition.
		"enable.idempotence": true,

		// Bounded so a broker stall cannot pin a relayer worker indefinitely.
		"message.timeout.ms": int(timeout.Milliseconds()),

		// Deliver async and batch, then Flush on shutdown. Synchronous per-message
		// delivery would make relaying a batch a sequence of broker round-trips.
		"compression.type": "snappy",

		"go.delivery.reports": true,
	}

	p, err := kafka.NewProducer(cfg)
	if err != nil {
		return nil, fmt.Errorf("create kafka producer: %w", err)
	}

	return &Producer{producer: p, enabled: true, timeout: timeout}, nil
}

// Enabled reports whether publishing is active. When false, Publish succeeds without
// contacting the broker so a disabled or unavailable Kafka never blocks the outbox.
func (p *Producer) Enabled() bool {
	return p != nil && p.enabled
}

// PublishResult describes the outcome of a single delivery attempt.
type PublishResult struct {
	Partition int32
	Offset    kafka.Offset
}

// Publish sends one message, blocking until the broker acknowledges it or the timeout
// expires.
//
// The returned error is already classified by IsRetriable, so callers do not need to know
// which librdkafka error codes mean "try again" and which mean "this will never work".
func (p *Producer) Publish(ctx context.Context, topic, key string, value []byte, headers map[string]string) (PublishResult, error) {
	if !p.enabled {
		return PublishResult{}, nil
	}

	message := &kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Key:            []byte(key),
		Value:          value,
		Timestamp:      time.Now(),
	}
	for name, headerValue := range headers {
		message.Headers = append(message.Headers, kafka.Header{Key: name, Value: []byte(headerValue)})
	}

	// The context deadline bounds how long a broker stall can hold the caller, on top of
	// message.timeout.ms, which the broker enforces independently.
	publishCtx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()

	deliveryChan := make(chan kafka.Event, 1)
	if err := p.producer.Produce(message, deliveryChan); err != nil {
		return PublishResult{}, classify(err)
	}

	select {
	case event := <-deliveryChan:
		delivered, ok := event.(*kafka.Message)
		if !ok {
			return PublishResult{}, &TerminalError{Err: fmt.Errorf("unexpected delivery event %T", event)}
		}
		if delivered.TopicPartition.Error != nil {
			return PublishResult{}, classify(delivered.TopicPartition.Error)
		}
		return PublishResult{
			Partition: delivered.TopicPartition.Partition,
			Offset:    delivered.TopicPartition.Offset,
		}, nil

	case <-publishCtx.Done():
		return PublishResult{}, classify(publishCtx.Err())

	case <-time.After(p.timeout):
		return PublishResult{}, &RetriableError{Err: ErrPublishTimeout}
	}
}

// Flush waits up to the timeout for buffered records to be delivered, returning the number
// still undelivered.
func (p *Producer) Flush(timeout time.Duration) int {
	if !p.enabled || p.producer == nil {
		return 0
	}
	return p.producer.Flush(int(timeout.Milliseconds()))
}

// Close shuts the producer down, discarding anything still buffered.
func (p *Producer) Close() {
	if p.enabled && p.producer != nil {
		p.producer.Close()
	}
}

// IsRetriable reports whether an error represents a transient condition where the same
// message may succeed later.
//
// This distinction is the whole basis of dead-letter routing: retrying a message that can
// never be delivered blocks the outbox forever, while dead-lettering a message that could
// have succeeded loses an event.
//
// librdkafka does expose Error.IsRetriable, but it is only populated for the
// transactional producer API and is always false here, so the classification is done
// explicitly against the error codes this deployment can actually encounter.
func IsRetriable(err error) bool {
	if err == nil {
		return false
	}

	var terminal *TerminalError
	if errors.As(err, &terminal) {
		return false
	}
	var retriable *RetriableError
	if errors.As(err, &retriable) {
		return true
	}

	// ErrorCode has String() but not Error(), so it does not satisfy the error interface
	// and cannot be targeted directly by errors.As. The kafka.Error interface is the
	// supported way to reach the underlying code.
	var kafkaErr kafka.Error
	if errors.As(err, &kafkaErr) {
		switch kafkaErr.Code() {
		case kafka.ErrMsgTimedOut,
			kafka.ErrTimedOut,
			kafka.ErrTransport,
			kafka.ErrResolve,
			kafka.ErrBrokerNotAvailable,
			kafka.ErrLeaderNotAvailable,
			kafka.ErrNotLeaderForPartition,
			kafka.ErrRequestTimedOut,
			kafka.ErrNetworkException,
			kafka.ErrAllBrokersDown,
			kafka.ErrNotEnoughReplicas,
			kafka.ErrNotEnoughReplicasAfterAppend,
			kafka.ErrKafkaStorageError,
			kafka.ErrReassignmentInProgress,
			kafka.ErrUnknownTopicOrPart:
			// ErrUnknownTopicOrPart is retriable on purpose: a relayer that starts before
			// kafka-init has run will hit it, and it resolves once the topic exists. Such
			// rows are only dead-lettered once MaxAttempts is exhausted.
			return true

		case kafka.ErrInvalidMsg,
			kafka.ErrInvalidMsgSize,
			kafka.ErrMsgSizeTooLarge,
			kafka.ErrTopicAuthorizationFailed,
			kafka.ErrGroupAuthorizationFailed,
			kafka.ErrClusterAuthorizationFailed,
			kafka.ErrTransactionalIDAuthorizationFailed,
			kafka.ErrUnknownProducerID,
			kafka.ErrTopicAlreadyExists,
			kafka.ErrInvalidPartitions:
			// Retrying these cannot help. An oversized or unauthorised message would loop
			// forever and hold every later event behind it.
			return false
		}
	}

	// Context cancellation is not a broker condition and must not be retried by the
	// relayer loop; the caller's shutdown handling decides what happens next.
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}

	return false
}

func classify(err error) error {
	if err == nil {
		return nil
	}
	if IsRetriable(err) {
		return &RetriableError{Err: err}
	}
	return &TerminalError{Err: err}
}

func joinBrokers(brokers []string) string {
	out := ""
	for i, b := range brokers {
		if i > 0 {
			out += ","
		}
		out += b
	}
	return out
}
