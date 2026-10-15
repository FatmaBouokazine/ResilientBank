package kafka

import (
	"context"
	"errors"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Producer is the franz-go backed Publisher.
//
// franz-go implements the Kafka protocol in pure Go, so this builds and tests with no C
// toolchain and no platform-specific static libraries.
type Producer struct {
	client  *kgo.Client
	enabled bool
	timeout time.Duration
}

// compile-time proof that the concrete type satisfies the interface the relayer depends on.
var _ Publisher = (*Producer)(nil)

// NewProducer creates a producer configured for at-least-once, lossless delivery.
//
// Idempotence is enabled by default in franz-go, and combined with RequiredAcks=All it
// means the broker acknowledges a record only once it is durable on every in-sync replica.
// Without both, a leader failover between the acknowledgement and the client's next read
// can silently lose a payment event.
func NewProducer(brokers []string, clientID string, enabled bool, timeout time.Duration) (*Producer, error) {
	if !enabled {
		// A disabled producer still satisfies Publisher. Publishing becomes a no-op, so
		// running without a broker never blocks the outbox.
		return &Producer{enabled: false, timeout: timeout}, nil
	}
	if len(brokers) == 0 {
		return nil, errors.New("no Kafka brokers configured")
	}

	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ClientID(clientID),

		// All in-sync replicas. On the single-broker local stack this resolves to one
		// replica; it becomes a genuine redundancy requirement as brokers are added.
		kgo.RequiredAcks(kgo.AllISRAcks()),

		// Bounds how long a broker stall can hold the relayer.
		kgo.RecordDeliveryTimeout(timeout),

		// Small linger so a batch of relayed events is sent as one request rather than one
		// round-trip per message, without adding meaningful latency.
		kgo.ProducerLinger(5*time.Millisecond),

		// Auto topic creation stays disabled (franz-go's default). Topics are created
		// explicitly by deploy/local/kafka/topics.sh so that a misspelled topic name fails
		// loudly instead of silently producing a single-partition topic that serialises
		// every event.
	)
	if err != nil {
		return nil, err
	}

	return &Producer{client: client, enabled: true, timeout: timeout}, nil
}

// Enabled reports whether publishing is active.
func (p *Producer) Enabled() bool {
	return p != nil && p.enabled
}

// Publish sends one message, blocking until the broker acknowledges it or the timeout
// expires.
//
// The key is the partition key: every event for one payment uses the payment id, so all of
// that payment's events land on one partition and retain their relative order.
func (p *Producer) Publish(ctx context.Context, topic, key string, value []byte, headers map[string]string) (PublishResult, error) {
	if !p.enabled {
		return PublishResult{Offset: unknownOffset}, nil
	}

	record := &kgo.Record{
		Topic:     topic,
		Key:       []byte(key),
		Value:     value,
		Timestamp: time.Now(),
	}
	for name, headerValue := range headers {
		record.Headers = append(record.Headers, kgo.RecordHeader{Key: name, Value: []byte(headerValue)})
	}

	// ProduceSync awaits the broker acknowledgement for exactly this record, which is what
	// lets the relayer mark an outbox row PUBLISHED only once the event is genuinely durable.
	results := p.client.ProduceSync(ctx, record)
	producedRecord, err := results.First()
	if err != nil {
		return PublishResult{}, classify(err)
	}

	result := PublishResult{Offset: unknownOffset}
	if producedRecord != nil {
		result.Partition = producedRecord.Partition
		// The record's offset is populated in place by ProduceSync when acks are required.
		result.Offset = record.Offset
	}
	return result, nil
}

// Flush waits up to the timeout for buffered records to be delivered, returning how many
// remain undelivered.
func (p *Producer) Flush(timeout time.Duration) int {
	if !p.enabled || p.client == nil {
		return 0
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	if err := p.client.Flush(ctx); err != nil {
		// A failed flush means records remain buffered. The exact count is not exposed by
		// the client, so report 1 as "at least one undelivered" rather than claiming zero.
		return 1
	}
	return 0
}

// Close releases the client. Anything still buffered is discarded, which is why callers
// must Flush before shutting down.
func (p *Producer) Close() {
	if p.enabled && p.client != nil {
		p.client.Close()
	}
}
