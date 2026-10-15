package kafka

import (
	"errors"
	"fmt"
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
