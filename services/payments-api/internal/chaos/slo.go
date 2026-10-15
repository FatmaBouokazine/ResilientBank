package chaos

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// SLOName identifies a service-level objective under test.
type SLOName string

const (
	// SLOPaymentAvailability: a committed payment is never lost, however the dependencies
	// behave. This is the platform's core promise and must survive every fault.
	SLOPaymentAvailability SLOName = "payment_availability"

	// SLOEventDurability: no event is ever lost. A fault may delay delivery indefinitely,
	// but once the fault clears every committed payment's event must still arrive. This is
	// exactly the guarantee the transactional outbox provides.
	//
	// Note this is deliberately distinct from freshness: a partition delays events without
	// destroying them, and conflating the two would make a healthy design look broken.
	SLOEventDurability SLOName = "event_durability"

	// SLOEventFreshness: events are being delivered within an acceptable lag. This is the
	// objective that legitimately degrades while a dependency is faulted, which is why it is
	// not treated as an invariant violation.
	SLOEventFreshness SLOName = "event_freshness"

	// SLORecoveryTime: the system returns to its pre-fault backlog within a bounded time
	// after the fault is removed. A system that survives but never recovers is not
	// resilient, merely stuck.
	SLORecoveryTime SLOName = "recovery_time"
)

// Measurement is a single objective evaluated at a point in time.
type Measurement struct {
	SLOName SLOName

	// Healthy reports whether the objective currently holds.
	Healthy bool

	// Detail explains a failure. An operator reading a chaos report should not have to
	// guess what broke.
	Detail string

	// ObservedAt is when the objective was evaluated.
	ObservedAt time.Time
}

// Observer evaluates objectives against a live system.
//
// It is an interface so the SLO layer is independent of how it is measured. Reading
// Prometheus, querying Postgres directly, or asserting on an in-process counter are all
// legitimate backends, and none of them should change what an "SLO" means.
type Observer interface {
	// Observe evaluates every objective once. It must not mutate the system.
	Observe(ctx context.Context) ([]Measurement, error)
}

// ObservationSet is the result of evaluating all objectives at one instant.
type ObservationSet struct {
	Measurements []Measurement
	ObservedAt   time.Time
}

// Healthy reports whether every objective currently holds, and returns the failures.
func (o ObservationSet) Healthy() (bool, []Measurement) {
	var failed []Measurement
	for _, m := range o.Measurements {
		if !m.Healthy {
			failed = append(failed, m)
		}
	}
	return len(failed) == 0, failed
}

// Verdict is the outcome of a chaos experiment.
type Verdict string

const (
	// VerdictResilient means the fault was injected, objectives held throughout, and the
	// system recovered.
	VerdictResilient Verdict = "resilient"

	// VerdictDegraded means the objectives were violated while the fault was active, which
	// is the expected and correct result for an objective that depends on the faulted
	// component. The system behaved as designed.
	VerdictDegraded Verdict = "degraded_as_expected"

	// VerdictViolated means a guarantee that must hold regardless of faults was broken.
	// This is the only verdict that indicates a real defect.
	VerdictViolated Verdict = "invariant_violated"

	// VerdictFaultNotObserved means the injection failed or had no measurable effect. This
	// is the most dangerous outcome to report as a pass: it looks like resilience while
	// proving nothing.
	VerdictFaultNotObserved Verdict = "fault_not_observed"

	// VerdictRecoveryFailed means the fault was injected but the system did not return to
	// health after it was removed.
	VerdictRecoveryFailed Verdict = "recovery_failed"
)

// Report is the full, auditable outcome of an experiment.
type Report struct {
	Experiment Experiment
	Verdict    Verdict
	StartedAt  time.Time
	EndedAt    time.Time

	// Observations sampled while the fault was active.
	DuringFault []ObservationSet

	// Observations sampled after recovery, used to judge recovery.
	AfterRecovery []ObservationSet

	// ObjectiveBreakdown maps an objective to whether it held. Objectives that are expected
	// to degrade while their dependency is faulted are listed separately.
	ObjectiveBreakdown []ObjectiveResult

	// RecoveryTime is how long the system took to become healthy again, or zero if it never
	// did.
	//
	// This may legitimately be zero: an immediate recovery and a measurement coarser than
	// the event both yield 0ns, since some platforms have coarse clock granularity. Use
	// RecoveryObservations to establish that recovery actually happened; RecoveryTime alone
	// cannot carry that proof.
	RecoveryTime time.Duration

	// RecoveryObservations counts how many post-recovery samples were taken before the
	// system was judged healthy. Zero means recovery was never observed.
	RecoveryObservations int

	// Notes carries engine-level detail such as injection failures.
	Notes []string
}

// ObjectiveResult records one objective's outcome across the whole experiment.
type ObjectiveResult struct {
	SLOName SLOName

	// HeldDuringFault is whether the objective survived the injected fault.
	HeldDuringFault bool

	// SamplesDuringFault is how many times it was evaluated under fault.
	SamplesDuringFault int

	// Recovered is whether it returned to healthy after the fault was removed.
	Recovered bool
}

// Summary renders a single-line verdict suitable for CI output.
func (r Report) Summary() string {
	return fmt.Sprintf("experiment=%q verdict=%s recovery=%s", r.Experiment.Name, r.Verdict, r.RecoveryTime)
}

// Passed reports whether the experiment demonstrated the expected behaviour.
//
// A verdict of VerdictDegraded is a pass: the system degraded in exactly the way it was
// supposed to and then recovered. Only an invariant violation or a failed recovery is a
// genuine failure, and a fault that was never observed is never a pass.
func (r Report) Passed() bool {
	switch r.Verdict {
	case VerdictResilient, VerdictDegraded:
		return true
	default:
		return false
	}
}

// recorder collects observations across an experiment without holding a lock per sample.
type recorder struct {
	mu      sync.Mutex
	sets    []ObservationSet
	maxKeep int
}

func newRecorder(maxKeep int) *recorder {
	if maxKeep <= 0 {
		maxKeep = 64
	}
	return &recorder{maxKeep: maxKeep}
}

func (r *recorder) add(set ObservationSet) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.sets = append(r.sets, set)
	// Bound memory: a long experiment must not grow the report without limit.
	if len(r.sets) > r.maxKeep {
		r.sets = r.sets[len(r.sets)-r.maxKeep:]
	}
}

func (r *recorder) all() []ObservationSet {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]ObservationSet, len(r.sets))
	copy(out, r.sets)
	return out
}
