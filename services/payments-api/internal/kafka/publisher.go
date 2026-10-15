// Package kafka publishes outbox events to Kafka.
//
// The client is reached exclusively through Publisher so the concrete Kafka library stays
// swappable. The relayer, dead-letter routing, metrics, and tests depend on this interface
// rather than on any franz-go or librdkafka type.
//
// Delivery semantics belong to the outbox table and the relayer, not to this package: the
// producer's only job is to hand a message to the broker and report whether the attempt is
// worth retrying. See ADR 0002.
package kafka

import (
	"context"
	"time"
)

// Publisher is the contract the relayer depends on.
//
// Implementations MUST guarantee that a nil error means the broker acknowledged the
// record, and that every non-nil error is classified by IsRetriable.
type Publisher interface {
	// Publish sends one message and blocks until the broker acknowledges it or the
	// timeout expires. The key determines the partition and therefore the ordering of all
	// messages that share it.
	Publish(ctx context.Context, topic, key string, value []byte, headers map[string]string) (PublishResult, error)

	// Enabled reports whether publishing is active. A disabled publisher must succeed
	// without contacting a broker so that a Kafka outage can never block the outbox.
	Enabled() bool

	// Flush waits up to the timeout for buffered records to be delivered, returning how
	// many remain undelivered.
	Flush(timeout time.Duration) int

	// Close releases the client's resources, discarding anything still buffered.
	Close()
}

// PublishResult describes where a record landed.
type PublishResult struct {
	// Partition the broker assigned. Only meaningful when the record was keyed, in which
	// case the key hashes to a stable partition and all events for one payment stay
	// ordered.
	Partition int32

	// Offset within that partition, or -1 when the broker did not report one.
	Offset int64
}

// unknownOffset is reported when acks are not required and no offset is available.
const unknownOffset int64 = -1
