package outbox

import (
	"math"
	"math/rand"
	"time"
)

// BackoffPolicy computes how long to wait before the next publish attempt.
type BackoffPolicy struct {
	// Base is the delay after the first failed attempt.
	Base time.Duration
	// Max caps the delay so an event never waits so long that it looks abandoned.
	Max time.Duration
	// JitterFraction spreads retries to avoid a thundering herd when a broker recovers
	// and a large backlog becomes eligible at the same instant.
	JitterFraction float64
}

// DefaultBackoffPolicy is the policy used unless configured otherwise.
func DefaultBackoffPolicy() BackoffPolicy {
	return BackoffPolicy{
		Base:           time.Second,
		Max:            5 * time.Minute,
		JitterFraction: 0.2,
	}
}

// Delay returns how long to wait before attempt number `attempts` is retried.
//
// attempts is 1-based: the first failure (attempts == 1) waits Base, the second waits
// 2*Base, and so on, doubling each time until Max is reached.
//
// The exponent is computed with integer shifting rather than math.Pow so a large attempts
// count cannot overflow into a negative duration.
func (p BackoffPolicy) Delay(attempts int) time.Duration {
	if attempts < 1 {
		attempts = 1
	}

	base := p.Base
	if base <= 0 {
		base = time.Second
	}

	// Cap the shift at 32: 2^32 * 1s already dwarfs any sane Max, and shifting further
	// would overflow int64.
	shift := attempts - 1
	if shift > 32 {
		shift = 32
	}

	delay := time.Duration(math.MinInt64)
	// Guard against overflow when doubling repeatedly.
	if base > time.Duration(math.MaxInt64/(1<<uint(shift))) {
		delay = p.maxOrDefault()
	} else {
		delay = base * time.Duration(int64(1)<<uint(shift))
	}

	if delay > p.maxOrDefault() {
		delay = p.maxOrDefault()
	}

	return p.applyJitter(delay)
}

func (p BackoffPolicy) maxOrDefault() time.Duration {
	if p.Max > 0 {
		return p.Max
	}
	return 5 * time.Minute
}

// applyJitter scales the delay by a random factor in [1-JitterFraction, 1+JitterFraction]
// so many events failing against the same broker do not all retry in lockstep.
func (p BackoffPolicy) applyJitter(delay time.Duration) time.Duration {
	if p.JitterFraction <= 0 {
		return delay
	}
	if p.JitterFraction > 1 {
		p.JitterFraction = 1
	}

	//nolint:gosec // jitter does not need a cryptographic source
	factor := 1 + (rand.Float64()*2-1)*p.JitterFraction

	jittered := time.Duration(float64(delay) * factor)
	if jittered < 0 {
		return delay
	}
	return jittered
}
