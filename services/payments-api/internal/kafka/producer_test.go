package kafka

import (
	"context"
	"errors"
	"fmt"
	"net"
	"testing"

	"github.com/twmb/franz-go/pkg/kerr"
)

// The retriable/terminal split decides whether an event is retried or dead-lettered. A
// misclassification is not a performance problem: treating a permanent failure as
// retriable blocks the outbox forever behind one poison message, and treating a transient
// failure as terminal silently loses a payment event.
func TestIsRetriableForTransientBrokerConditions(t *testing.T) {
	transient := map[string]error{
		"broker unavailable":        kerr.BrokerNotAvailable,
		"leader electing":           kerr.LeaderNotAvailable,
		"not leader for partition":  kerr.NotLeaderForPartition,
		"request timed out":         kerr.RequestTimedOut,
		"network exception":         kerr.NetworkException,
		"not enough replicas":       kerr.NotEnoughReplicas,
		"not enough replicas write": kerr.NotEnoughReplicasAfterAppend,
		"storage error":             kerr.KafkaStorageError,

		// A relayer started before kafka-init has run sees this. It resolves once the
		// topic exists, so it must be retried rather than dead-lettered.
		"topic not yet created": kerr.UnknownTopicOrPartition,

		// Overridden below: the library marks this terminal, but a reassignment is
		// transient and resolves on its own.
		"reassignment in progress": kerr.ReassignmentInProgress,

		// Overridden below: quota rejections are the canonical reason to back off.
		"throttled by quota": kerr.ThrottlingQuotaExceeded,
	}

	for name, err := range transient {
		t.Run(name, func(t *testing.T) {
			if !IsRetriable(err) {
				t.Errorf("IsRetriable(%s) = false, want true", name)
			}
		})
	}
}

func TestIsRetriableForPermanentFailures(t *testing.T) {
	permanent := map[string]error{
		"message too large":    kerr.MessageTooLarge,
		"broker rejected msg":  kerr.InvalidRecord,
		"invalid topic":        kerr.InvalidTopicException,
		"topic unauthorized":   kerr.TopicAuthorizationFailed,
		"cluster unauthorized": kerr.ClusterAuthorizationFailed,
		"unsupported codec":    kerr.UnsupportedCompressionType,

		// Overridden below: the library marks this retriable, but a record that fails its
		// checksum will fail identically forever.
		"corrupt message": kerr.CorruptMessage,
	}

	for name, err := range permanent {
		t.Run(name, func(t *testing.T) {
			if IsRetriable(err) {
				t.Errorf("IsRetriable(%s) = true, want false; retrying would loop forever "+
					"and hold every later event behind it", name)
			}
		})
	}
}

// These two guard the policy overrides specifically: if franz-go changes its own
// retriability flags, the tests below would silently invert.
func TestPolicyOverridesLibraryDefaults(t *testing.T) {
	t.Run("reassignment is retriable despite library default", func(t *testing.T) {
		if kerr.ReassignmentInProgress.Retriable {
			t.Skip("franz-go now marks reassignment retriable; the override is redundant")
		}
		if !IsRetriable(kerr.ReassignmentInProgress) {
			t.Error("a partition reassignment is transient and must be retried with backoff")
		}
	})

	t.Run("corrupt message is terminal despite library default", func(t *testing.T) {
		if !kerr.CorruptMessage.Retriable {
			t.Skip("franz-go no longer marks corrupt messages retriable; the override is redundant")
		}
		if IsRetriable(kerr.CorruptMessage) {
			t.Error("a record that fails its checksum will never succeed; retrying it " +
				"poisons the outbox head position forever")
		}
	})
}

// The library's own flag must be the base signal, not the only one.
func TestBrokerSuppliedFlagIsUsed(t *testing.T) {
	if !IsRetriable(kerr.BrokerNotAvailable) {
		t.Error("a broker-reported retriable error must be treated as retriable")
	}
	if IsRetriable(kerr.TopicAuthorizationFailed) {
		t.Error("a broker-reported permanent error must be treated as terminal")
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

// A cancelled context means the operator is shutting down. Retrying would contradict that
// and could republish during a controlled restart.
func TestIsRetriableRejectsContextErrors(t *testing.T) {
	if IsRetriable(context.Canceled) {
		t.Error("context.Canceled must not be retriable")
	}
	if IsRetriable(context.DeadlineExceeded) {
		t.Error("context.DeadlineExceeded must not be retriable")
	}
}

// A client-side failure never reached a broker, so it carries no kerr code, but it is
// transient by definition.
func TestIsRetriableForClientSideNetworkFailure(t *testing.T) {
	opErr := &net.OpError{Op: "dial", Net: "tcp", Err: errors.New("connection refused")}
	if !IsRetriable(opErr) {
		t.Error("a dial failure must be retriable; the next attempt may connect")
	}

	// An unknown error with no Kafka or network signal must default to terminal, because
	// retrying it forever would block the outbox.
	if IsRetriable(errors.New("something unexplained")) {
		t.Error("an unrecognised error must default to terminal so it reaches the DLQ " +
			"for human triage rather than looping")
	}
}

func TestClassifyWrapsAccordingToRetriability(t *testing.T) {
	retriable := classify(kerr.BrokerNotAvailable)
	var re *RetriableError
	if !errors.As(retriable, &re) {
		t.Errorf("transient error classified as %T, want *RetriableError", retriable)
	}

	terminal := classify(kerr.MessageTooLarge)
	var te *TerminalError
	if !errors.As(terminal, &te) {
		t.Errorf("permanent error classified as %T, want *TerminalError", terminal)
	}

	if got := classify(nil); got != nil {
		t.Errorf("classify(nil) = %v, want nil", got)
	}
}

// Errors must stay inspectable through Unwrap, otherwise the relayer cannot log the
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
// Kafka outage to stop nothing.
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
	if res.Offset != unknownOffset {
		t.Errorf("Offset = %d, want %d for a skipped publish", res.Offset, unknownOffset)
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

// The producer must satisfy the interface the relayer is written against, so the concrete
// client stays swappable.
func TestProducerSatisfiesPublisher(t *testing.T) {
	var p interface{} = &Producer{}
	if _, ok := p.(Publisher); !ok {
		t.Error("*Producer does not satisfy Publisher")
	}
}

// A record with no error must still carry a usable partition so the relayer can log where
// an event landed.
func TestPublishResultUnknownOffsetIsNegative(t *testing.T) {
	if unknownOffset >= 0 {
		t.Errorf("unknownOffset = %d, want a negative sentinel distinguishable from a real offset", unknownOffset)
	}
	if fmt.Sprint(unknownOffset) == "" {
		t.Error("unknownOffset must be printable for logs")
	}
}
