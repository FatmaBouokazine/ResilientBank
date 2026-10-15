package kafka

import (
	"context"
	"errors"
	"fmt"
	"net"

	"github.com/twmb/franz-go/pkg/kerr"
)

// ErrPublishTimeout is returned when the broker did not acknowledge a message within the
// configured timeout. It is retriable: the broker may simply be slow or unreachable.
var ErrPublishTimeout = errors.New("kafka publish timed out")

// RetriableError marks a transient failure. The same message may succeed on a later
// attempt, so the caller schedules a backoff and retries rather than dead-lettering it.
type RetriableError struct {
	Err error
}

func (e *RetriableError) Error() string {
	return fmt.Sprintf("retriable kafka error: %v", e.Err)
}

func (e *RetriableError) Unwrap() error { return e.Err }

// TerminalError marks a failure that will never succeed on retry: an oversized message, a
// rejected encoding, or an authorisation problem. The caller must route these to the
// dead-letter topic, because retrying them forever would block every event behind them.
type TerminalError struct {
	Err error
}

func (e *TerminalError) Error() string {
	return fmt.Sprintf("terminal kafka error: %v", e.Err)
}

func (e *TerminalError) Unwrap() error { return e.Err }

// IsRetriable reports whether an error represents a transient condition where the same
// message may succeed later.
//
// This distinction is the whole basis of dead-letter routing. Misclassifying a permanent
// failure as retriable blocks the outbox forever behind one poison message; misclassifying
// a transient failure as terminal silently loses a payment event.
//
// franz-go populates kerr.Error.Retriable from the broker's own error code, which is a
// better signal than any hand-maintained list. Policy overrides are applied first,
// because this deployment needs different trade-offs from a generic client.
func IsRetriable(err error) bool {
	if err == nil {
		return false
	}

	// Already classified: preserve the original decision.
	var terminal *TerminalError
	if errors.As(err, &terminal) {
		return false
	}
	var retriable *RetriableError
	if errors.As(err, &retriable) {
		return true
	}

	// A cancelled or expired context means the operator is shutting the relayer down.
	// Retrying would contradict that intent and could republish during a controlled
	// restart.
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}

	// Policy overrides, applied before the library default.
	switch {
	case errors.Is(err, kerr.MessageTooLarge):
		// The payload will never fit. Retrying it forever would stall the outbox.
		return false

	case errors.Is(err, kerr.InvalidRecord), errors.Is(err, kerr.InvalidTopicException):
		// The broker rejected the record itself. A different record would behave the
		// same way, so this is a poison pill rather than a transient fault.
		return false

	case errors.Is(err, kerr.CorruptMessage):
		// franz-go marks this retriable, but a record that fails its checksum will fail
		// identically forever. Treating it as terminal protects the outbox from a
		// permanently poisoned head position.
		return false

	case errors.Is(err, kerr.ReassignmentInProgress):
		// franz-go marks this terminal, but a partition reassignment is transient and
		// resolves on its own. With backoff, retrying is correct.
		return true

	case errors.Is(err, kerr.ThrottlingQuotaExceeded):
		// Quota rejections are explicitly retryable and are the main reason to back off
		// rather than fail a payment event.
		return true
	}

	// Broker-reported errors carry an accurate retriability flag.
	if kerr.IsRetriable(err) {
		return true
	}

	// Client-side network failures never reached a broker, so they carry no kerr code.
	// They are transient by definition: the next attempt may connect.
	var netErr net.Error
	if errors.As(err, &netErr) {
		return true
	}

	return false
}

// classify wraps an error according to IsRetriable so the relayer does not need to know
// which error codes mean "try again".
func classify(err error) error {
	if err == nil {
		return nil
	}
	if IsRetriable(err) {
		return &RetriableError{Err: err}
	}
	return &TerminalError{Err: err}
}
