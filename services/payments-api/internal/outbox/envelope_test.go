package outbox

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
)

var testTime = time.Date(2026, 3, 14, 15, 9, 26, 0, time.UTC)

func sampleRef() PaymentRef {
	return PaymentRef{
		PaymentID:     "3f2504e0-4f89-41d3-9a0c-0305e82c3301",
		FromAccount:   "acc-alice",
		ToAccount:     "acc-bob",
		AmountCents:   5000,
		Currency:      "EUR",
		Status:        "ACCEPTED",
		CorrelationID: "smoke-key-123",
		CreatedAt:     testTime,
	}
}

func TestBuildPaymentCreatedPopulatesEnvelope(t *testing.T) {
	event := BuildPaymentCreated(sampleRef(), "evt-1")

	if event.GetEventId() != "evt-1" {
		t.Errorf("EventId = %q, want evt-1", event.GetEventId())
	}
	if event.GetEventType() != EventTypePaymentCreated {
		t.Errorf("EventType = %q, want %q", event.GetEventType(), EventTypePaymentCreated)
	}
	if event.GetEventVersion() != EventVersionPaymentCreated {
		t.Errorf("EventVersion = %d, want %d", event.GetEventVersion(), EventVersionPaymentCreated)
	}
	if event.GetCorrelationId() != "smoke-key-123" {
		t.Errorf("CorrelationId = %q, want the idempotency key", event.GetCorrelationId())
	}
	if event.GetAggregateType() != AggregateTypePayment {
		t.Errorf("AggregateType = %q, want %q", event.GetAggregateType(), AggregateTypePayment)
	}
	if event.GetAggregateId() != "3f2504e0-4f89-41d3-9a0c-0305e82c3301" {
		t.Errorf("AggregateId = %q, want the payment id", event.GetAggregateId())
	}
	if event.GetProducer() != ProducerPaymentsAPI {
		t.Errorf("Producer = %q, want %q", event.GetProducer(), ProducerPaymentsAPI)
	}
}

func TestBuildPaymentCreatedPopulatesPayload(t *testing.T) {
	event := BuildPaymentCreated(sampleRef(), "evt-1")

	payload := event.GetPaymentCreated()
	if payload == nil {
		t.Fatal("payload is nil; the oneof was not set")
	}
	if payload.GetPaymentId() != "3f2504e0-4f89-41d3-9a0c-0305e82c3301" {
		t.Errorf("payload.PaymentId = %q", payload.GetPaymentId())
	}
	if payload.GetFromAccount() != "acc-alice" {
		t.Errorf("payload.FromAccount = %q", payload.GetFromAccount())
	}
	if payload.GetToAccount() != "acc-bob" {
		t.Errorf("payload.ToAccount = %q", payload.GetToAccount())
	}
	if payload.GetAmountCents() != 5000 {
		t.Errorf("payload.AmountCents = %d, want 5000", payload.GetAmountCents())
	}
	if payload.GetCurrency() != "EUR" {
		t.Errorf("payload.Currency = %q", payload.GetCurrency())
	}
	if payload.GetStatus() != "ACCEPTED" {
		t.Errorf("payload.Status = %q", payload.GetStatus())
	}
}

// occurred_at and the payload's created_at must be the same instant, taken from the
// database. If these ever diverge, event ordering stops matching the records it
// describes.
func TestOccurredAtMatchesCreatedAt(t *testing.T) {
	event := BuildPaymentCreated(sampleRef(), "evt-1")

	occurred := event.GetOccurredAt().AsTime()
	created := event.GetPaymentCreated().GetCreatedAt().AsTime()

	if !occurred.Equal(created) {
		t.Errorf("occurred_at %v != created_at %v", occurred, created)
	}
	if !occurred.Equal(testTime) {
		t.Errorf("occurred_at = %v, want %v", occurred, testTime)
	}
}

func TestNewEventIDIsUnique(t *testing.T) {
	seen := make(map[string]struct{}, 1000)
	for i := 0; i < 1000; i++ {
		id := NewEventID()
		if id == "" {
			t.Fatal("NewEventID returned an empty string")
		}
		if _, dup := seen[id]; dup {
			t.Fatalf("NewEventID produced a duplicate id %q", id)
		}
		seen[id] = struct{}{}
	}
}

func TestMarshalUsesProtoNames(t *testing.T) {
	raw, err := Marshal(BuildPaymentCreated(sampleRef(), "evt-1"))
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	got := string(raw)

	for _, key := range []string{
		`"event_id"`,
		`"event_type"`,
		`"event_version"`,
		`"occurred_at"`,
		`"correlation_id"`,
		`"aggregate_type"`,
		`"aggregate_id"`,
		`"producer"`,
		`"payment_created"`,
		`"payment_id"`,
		`"from_account"`,
		`"amount_cents"`,
	} {
		if !strings.Contains(got, key) {
			t.Errorf("serialized payload missing %s\ngot: %s", key, got)
		}
	}

	// Guard against a future protojson default silently reverting to lowerCamelCase.
	if strings.Contains(got, `"paymentId"`) || strings.Contains(got, `"eventId"`) {
		t.Errorf("payload used camelCase keys, UseProtoNames is not in effect: %s", got)
	}
}

// The protobuf JSON mapping encodes 64-bit integers as JSON strings to avoid precision
// loss in JavaScript consumers. Consumers using a generic JSON parser must be told this,
// so it is pinned by an explicit assertion rather than left to documentation.
func TestMarshalEncodesInt64AsJSONString(t *testing.T) {
	raw, err := Marshal(BuildPaymentCreated(sampleRef(), "evt-1"))
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}

	var decoded map[string]any
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatalf("stored payload is not valid JSON: %v", err)
	}

	payload, ok := decoded["payment_created"].(map[string]any)
	if !ok {
		t.Fatalf("payment_created missing or wrong type in %s", raw)
	}

	amount, present := payload["amount_cents"]
	if !present {
		t.Fatalf("amount_cents missing in %s", raw)
	}
	str, isString := amount.(string)
	if !isString {
		t.Fatalf("amount_cents decoded as %T (%v); protobuf JSON encodes int64 as a JSON string. "+
			"If this assertion starts failing, a protojson upgrade changed the wire format and "+
			"every existing consumer must be re-checked.", amount, amount)
	}
	if str != "5000" {
		t.Errorf("amount_cents = %q, want \"5000\"", str)
	}
}

func TestMarshalIsDeterministic(t *testing.T) {
	// Same inputs and same event_id must produce byte-identical output, so a retry of an
	// uncommitted write cannot produce a different stored payload.
	ref := sampleRef()

	first, err := Marshal(BuildPaymentCreated(ref, "evt-1"))
	if err != nil {
		t.Fatalf("first Marshal: %v", err)
	}
	second, err := Marshal(BuildPaymentCreated(ref, "evt-1"))
	if err != nil {
		t.Fatalf("second Marshal: %v", err)
	}

	if string(first) != string(second) {
		t.Errorf("Marshal is not deterministic:\nfirst:  %s\nsecond: %s", first, second)
	}
}

func TestMarshalRoundTrip(t *testing.T) {
	original := BuildPaymentCreated(sampleRef(), "evt-1")

	raw, err := Marshal(original)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	decoded, err := Unmarshal(raw)
	if err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}

	if !proto.Equal(original, decoded) {
		t.Errorf("round trip lost data\noriginal: %v\ndecoded:  %v", original, decoded)
	}
}

// A consumer on an older version must still parse a payload carrying fields it does not
// know about, otherwise additive schema evolution would break deployed consumers.
func TestUnmarshalDiscardsUnknownFields(t *testing.T) {
	raw := []byte(`{
		"event_id": "evt-1",
		"event_type": "payment.created",
		"event_version": 1,
		"correlation_id": "smoke-key-123",
		"aggregate_type": "payment",
		"aggregate_id": "3f2504e0-4f89-41d3-9a0c-0305e82c3301",
		"producer": "payments-api",
		"a_field_from_the_future": "surprise",
		"payment_created": {
			"payment_id": "3f2504e0-4f89-41d3-9a0c-0305e82c3301",
			"amount_cents": "5000"
		}
	}`)

	event, err := Unmarshal(raw)
	if err != nil {
		t.Fatalf("Unmarshal rejected a payload with an unknown field: %v", err)
	}
	if event.GetEventId() != "evt-1" {
		t.Errorf("EventId = %q, want evt-1", event.GetEventId())
	}
	if got := event.GetPaymentCreated().GetAmountCents(); got != 5000 {
		t.Errorf("AmountCents = %d, want 5000", got)
	}
}

func TestUnmarshalRejectsMalformedPayload(t *testing.T) {
	if _, err := Unmarshal([]byte(`{"event_id": `)); err == nil {
		t.Error("Unmarshal accepted malformed JSON; a corrupt outbox row would pass silently")
	}
}
