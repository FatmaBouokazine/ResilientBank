package kafka

import (
	"context"
	"errors"
	"testing"

	kafkalib "github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// The retriable/terminal split is what decides whether an event is retried or
// dead-lettered. Getting it wrong is not a performance problem: misclassifying a
// permanent failure as retriable blocks the outbox forever behind one poison message,
// while misclassifying a transient failure as terminal silently loses a payment event.
func TestIsRetriableForTransientBrokerConditions(t *testing.T) {
	transient := map[string]kafkalib.ErrorCode{
		"broker down":                kafkalib.ErrAllBrokersDown,
		"transport failure":          kafkalib.ErrTransport,
		"message timed out":          kafkalib.ErrMsgTimedOut,
		"request timed out":          kafkalib.ErrRequestTimedOut,
		"leader not available":       kafkalib.ErrLeaderNotAvailable,
		"not leader for partition":   kafkalib.ErrNotLeaderForPartition,
		"broker not available":       kafkalib.ErrBrokerNotAvailable,
		"network exception":          kafkalib.ErrNetworkException,
		"not enough replicas":        kafkalib.ErrNotEnoughReplicas,
		"not enough replicas append": kafkalib.ErrNotEnoughReplicasAfterAppend,
		"dns resolve failure":        kafkalib.ErrResolve,
		"topic not yet created":      kafkalib.ErrUnknownTopicOrPart,
	}

	for name, code := range transient {
		t.Run(name, func(t *testing.T) {
			if !IsRetriable(newKafkaError(code)) {
				t.Errorf("IsRetriable(%s) = false, want true; a relayer starting before "+
					"kafka-init must retry rather than dead-letter the event", name)
			}
		})
	}
}

func TestIsRetriableForPermanentFailures(t *testing.T) {
	permanent := map[string]kafkalib.ErrorCode{
		"invalid message":         kafkalib.ErrInvalidMsg,
		"message too large":       kafkalib.ErrMsgSizeTooLarge,
		"invalid message size":    kafkalib.ErrInvalidMsgSize,
		"topic unauthorized":      kafkalib.ErrTopicAuthorizationFailed,
		"cluster unauthorized":    kafkalib.ErrClusterAuthorizationFailed,
		"unknown producer id":     kafkalib.ErrUnknownProducerID,
		"invalid partition count": kafkalib.ErrInvalidPartitions,
	}

	for name, code := range permanent {
		t.Run(name, func(t *testing.T) {
			if IsRetriable(newKafkaError(code)) {
				t.Errorf("IsRetriable(%s) = true, want false; retrying this would loop "+
					"forever and hold every later event behind it", name)
			}
		})
	}
}

func TestIsRetriableWrapperTypes(t *testing.T) {
	if !IsRetriable(&RetriableError{Err: errors.New("anything")}) {
		t.Error("an error already classified as retriable must report true")
	}
	if IsRetriable(&TerminalError{Err: errors.New("anything")}) {
		t.Error("an error already classified as terminal must report false")
	}
	if IsRetriable(nil) {
		t.Error("nil must not be retriable")
	}
}

// A cancelled context means the operator is shutting the relayer down. Retrying would
// contradict that intent and could republish during a controlled restart.
func TestIsRetriableRejectsContextErrors(t *testing.T) {
	if IsRetriable(context.Canceled) {
		t.Error("context.Canceled must not be retriable")
	}
	if IsRetriable(context.DeadlineExceeded) {
		t.Error("context.DeadlineExceeded must not be retriable")
	}
}

// The library's own IsRetriable is only populated for transactional producers, so the
// platform classification must not depend on it.
func TestLibraryRetriableFlagIsNotReliedUpon(t *testing.T) {
	kafkaErr := newKafkaError(kafkalib.ErrAllBrokersDown)
	var typed kafkalib.Error
	if !errors.As(kafkaErr, &typed) {
		t.Fatal("test helper did not produce a kafka.Error")
	}
	if typed.IsRetriable() {
		t.Skip("this build populates the transactional retriable flag; the platform " +
			"classification is still authoritative and must be tested independently")
	}
	if !IsRetriable(kafkaErr) {
		t.Error("platform classification must not depend on the library's retriable flag")
	}
}

func TestClassifyWrapsAccordingToRetriability(t *testing.T) {
	retriable := classify(newKafkaError(kafkalib.ErrAllBrokersDown))
	var re *RetriableError
	if !errors.As(retriable, &re) {
		t.Errorf("transient error classified as %T, want *RetriableError", retriable)
	}

	terminal := classify(newKafkaError(kafkalib.ErrMsgSizeTooLarge))
	var te *TerminalError
	if !errors.As(terminal, &te) {
		t.Errorf("permanent error classified as %T, want *TerminalError", terminal)
	}

	if got := classify(nil); got != nil {
		t.Errorf("classify(nil) = %v, want nil", got)
	}
}

// Errors must remain inspectable through Unwrap, otherwise the relayer cannot log the
// underlying broker message.
func TestWrappedErrorsUnwrap(t *testing.T) {
	inner := errors.New("broker unreachable")
	if !errors.Is(&RetriableError{Err: inner}, inner) {
		t.Error("RetriableError must unwrap to the cause")
	}
	if !errors.Is(&TerminalError{Err: inner}, inner) {
		t.Error("TerminalError must unwrap to the cause")
	}
}

// A disabled producer must succeed without contacting a broker, which is what allows a
// broker outage to stop nothing.
func TestDisabledProducerPublishesWithoutBroker(t *testing.T) {
	p, err := NewProducer(nil, "test", false, 0)
	if err != nil {
		t.Fatalf("NewProducer(disabled) returned error: %v", err)
	}
	if p.Enabled() {
		t.Error("Enabled() = true for a disabled producer")
	}

	res, err := p.Publish(context.Background(), "any-topic", "key", []byte("payload"), nil)
	if err != nil {
		t.Errorf("Publish on a disabled producer returned %v, want nil", err)
	}
	if res.Offset != 0 {
		t.Errorf("Offset = %v, want the zero value for a skipped publish", res.Offset)
	}

	// Shutdown paths must be safe with no underlying client.
	if remaining := p.Flush(0); remaining != 0 {
		t.Errorf("Flush = %d, want 0 for a disabled producer", remaining)
	}
	p.Close()
}

func TestNewProducerRequiresBrokersWhenEnabled(t *testing.T) {
	if _, err := NewProducer(nil, "test", true, 0); err == nil {
		t.Error("NewProducer with no brokers must fail; an enabled producer with no " +
			"brokers would silently never deliver")
	}
}

// newKafkaError builds an error carrying a specific librdkafka error code, mirroring what
// the client surfaces on a failed delivery.
func newKafkaError(code kafkalib.ErrorCode) error {
	return kafkalib.NewError(code, code.String(), false)
}
