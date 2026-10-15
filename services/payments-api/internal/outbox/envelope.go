// Package outbox builds the domain events staged for publication to Kafka.
//
// Events are constructed here rather than at the call site so that envelope metadata
// stays consistent across every producer, and so the wire format is defined in exactly
// one place. See ADR 0002.
package outbox

import (
	"time"

	"github.com/google/uuid"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	eventsv1 "github.com/YOUR_GITHUB_USER/resilientbank/services/payments-api/gen/events/v1"
)

// Event metadata for the payment domain.
//
// event_type is the routing key consumers branch on and must never be reused for a
// semantically different event. event_version is incremented only for incompatible
// payload changes; additive, backward-compatible fields must NOT bump it.
const (
	EventTypePaymentCreated           = "payment.created"
	EventVersionPaymentCreated uint32 = 1

	AggregateTypePayment = "payment"
	ProducerPaymentsAPI  = "payments-api"
)

// PaymentRef identifies the payment an event describes, together with the database
// timestamp the event must carry.
//
// CreatedAt is a time.Time taken from INSERT ... RETURNING rather than the application
// clock, so that occurred_at and the payment row it describes can never disagree.
// CorrelationID is the client-supplied idempotency key, which links the emitted event
// back to the originating gRPC call.
type PaymentRef struct {
	PaymentID     string
	FromAccount   string
	ToAccount     string
	AmountCents   int64
	Currency      string
	Status        string
	CorrelationID string
	CreatedAt     time.Time
}

// NewEventID returns a unique identifier for a single event instance.
//
// Consumers deduplicate on this value, because the outbox provides at-least-once
// delivery: a relayer that crashes after publishing but before marking the row
// PUBLISHED will republish the same event.
func NewEventID() string {
	return uuid.New().String()
}

// BuildPaymentCreated constructs the payment.created envelope.
//
// Kept separate from Marshal so tests can assert on the typed message directly and so
// the caller controls the event_id, which makes the serialized output deterministic.
func BuildPaymentCreated(ref PaymentRef, eventID string) *eventsv1.PaymentEvent {
	// A single timestamp instance backs both envelope and payload so the two can never
	// drift apart if this is ever refactored.
	occurredAt := timestamppb.New(ref.CreatedAt)

	return &eventsv1.PaymentEvent{
		EventId:       eventID,
		EventType:     EventTypePaymentCreated,
		EventVersion:  EventVersionPaymentCreated,
		OccurredAt:    occurredAt,
		CorrelationId: ref.CorrelationID,
		AggregateType: AggregateTypePayment,
		AggregateId:   ref.PaymentID,
		Producer:      ProducerPaymentsAPI,
		Payload: &eventsv1.PaymentEvent_PaymentCreated{
			PaymentCreated: &eventsv1.PaymentCreated{
				PaymentId:   ref.PaymentID,
				FromAccount: ref.FromAccount,
				ToAccount:   ref.ToAccount,
				AmountCents: ref.AmountCents,
				Currency:    ref.Currency,
				Status:      ref.Status,
				CreatedAt:   occurredAt,
			},
		},
	}
}

// Marshal serializes an event envelope for storage in the outbox payload column.
//
// UseProtoNames is enabled so stored keys are snake_case, matching the surrounding
// database schema rather than protobuf's lowerCamelCase default.
//
// NOTE ON int64: protobuf JSON encodes 64-bit integers as JSON *strings* to avoid
// precision loss in JavaScript consumers, so amount_cents is stored as "5000" and not
// 5000. This is correct per the protobuf JSON mapping, but consumers using a generic
// JSON parser must expect a string.
func Marshal(event *eventsv1.PaymentEvent) ([]byte, error) {
	opts := protojson.MarshalOptions{
		UseProtoNames: true,
	}
	return opts.Marshal(event)
}

// Unmarshal parses a stored outbox payload back into its typed envelope.
func Unmarshal(data []byte) (*eventsv1.PaymentEvent, error) {
	opts := protojson.UnmarshalOptions{
		DiscardUnknown: true,
	}
	var event eventsv1.PaymentEvent
	if err := opts.Unmarshal(data, &event); err != nil {
		return nil, err
	}
	return &event, nil
}

// interface guard: Marshal/Unmarshal must accept the generated envelope type.
var _ proto.Message = (*eventsv1.PaymentEvent)(nil)
