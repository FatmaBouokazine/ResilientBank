package outbox

import (
	"testing"
	"time"
)

func TestBackoffGrowsExponentially(t *testing.T) {
	// Jitter is disabled so the exponential shape is directly observable.
	policy := BackoffPolicy{Base: time.Second, Max: time.Minute, JitterFraction: 0}

	want := []time.Duration{
		1 * time.Second,  // attempts == 1
		2 * time.Second,  // 2
		4 * time.Second,  // 3
		8 * time.Second,  // 4
		16 * time.Second, // 5
	}

	for i, expected := range want {
		attempts := i + 1
		if got := policy.Delay(attempts); got != expected {
			t.Errorf("Delay(%d) = %v, want %v", attempts, got, expected)
		}
	}
}

func TestBackoffRespectsMax(t *testing.T) {
	policy := BackoffPolicy{Base: time.Second, Max: 10 * time.Second}

	if got := policy.Delay(20); got != 10*time.Second {
		t.Errorf("Delay(20) = %v, want the %v cap", got, 10*time.Second)
	}
}

// A very large attempt count must not overflow into a negative duration, which would make
// the row immediately eligible again and produce a hot retry loop.
func TestBackoffDoesNotOverflow(t *testing.T) {
	policy := BackoffPolicy{Base: time.Second, Max: 5 * time.Minute}

	for _, attempts := range []int{31, 32, 33, 63, 64, 1000, 1 << 20} {
		got := policy.Delay(attempts)
		if got < 0 {
			t.Errorf("Delay(%d) = %v, must never be negative", attempts, got)
		}
		if got > policy.Max {
			t.Errorf("Delay(%d) = %v, exceeds the %v cap", attempts, got, policy.Max)
		}
	}
}

// attempts is 1-based, but a caller passing 0 must not produce a zero delay, which would
// cause an immediate retry storm.
func TestBackoffTreatsNonPositiveAttemptsAsFirst(t *testing.T) {
	policy := BackoffPolicy{Base: time.Second, Max: time.Minute, JitterFraction: 0}

	for _, attempts := range []int{0, -1, -100} {
		if got := policy.Delay(attempts); got != time.Second {
			t.Errorf("Delay(%d) = %v, want %v", attempts, got, time.Second)
		}
	}
}

func TestBackoffJitterStaysWithinBounds(t *testing.T) {
	policy := BackoffPolicy{Base: 10 * time.Second, Max: time.Minute, JitterFraction: 0.2}

	// Delay(3) is Base * 2^2 = 40s, so +/-20% lands in [32s, 48s].
	low := 32 * time.Second
	high := 48 * time.Second

	for i := 0; i < 500; i++ {
		got := policy.Delay(3)
		if got < low || got > high {
			t.Fatalf("Delay(3) = %v, want within [%v, %v]", got, low, high)
		}
	}
}

// Jitter exists to desynchronise retries after a broker recovery. If it collapsed to a
// constant it would defeat that purpose.
func TestBackoffJitterActuallyVaries(t *testing.T) {
	policy := BackoffPolicy{Base: 10 * time.Second, Max: time.Minute, JitterFraction: 0.2}

	seen := make(map[time.Duration]struct{}, 100)
	for i := 0; i < 100; i++ {
		seen[policy.Delay(3)] = struct{}{}
	}
	if len(seen) < 2 {
		t.Errorf("Delay produced %d distinct value(s); jitter is not spreading retries", len(seen))
	}
}

func TestBackoffZeroBaseFallsBackToDefault(t *testing.T) {
	policy := BackoffPolicy{Base: 0, Max: 0}
	got := policy.Delay(1)
	if got <= 0 {
		t.Errorf("Delay(1) = %v with a zero Base; must fall back to a sane default", got)
	}
}

func TestFormatInterval(t *testing.T) {
	cases := []struct {
		in   time.Duration
		want string
	}{
		{30 * time.Second, "30.000000 seconds"},
		{1500 * time.Millisecond, "1.500000 seconds"},
		{0, "0.000000 seconds"},
		// A negative interval would be rejected by PostgreSQL; clamping keeps a bad
		// configuration from producing a query error on every poll.
		{-5 * time.Second, "0.000000 seconds"},
	}

	for _, tc := range cases {
		if got := formatInterval(tc.in); got != tc.want {
			t.Errorf("formatInterval(%v) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestTruncateBoundsHeaderValue(t *testing.T) {
	if got := truncate("short", 10); got != "short" {
		t.Errorf("truncate(short) = %q, want unchanged", got)
	}
	long := "abcdefghijklmnop"
	if got := truncate(long, 5); got != "abcde" {
		t.Errorf("truncate(%q, 5) = %q, want %q", long, got, "abcde")
	}
}
