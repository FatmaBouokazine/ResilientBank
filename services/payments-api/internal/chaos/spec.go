// Package chaos injects faults into a running system and verifies that the system's
// guarantees survive them.
//
// THE PRIMITIVE IDEA:
// A resilience claim is worthless without a way to try to break it. This package makes
// "the outbox means a Kafka outage loses no payments" an executable statement: kill the
// broker mid-relay, and assert that every committed payment still ends up published.
//
// THE CRITICAL SAFETY PROPERTY:
// Recovery is not optional. An experiment that injects a fault and then fails before
// recovering would leave the system broken and, worse, give a false impression that it was
// resilient. Every injector is therefore paired with a recover step, and the runner treats
// a failed recovery as the most severe outcome possible rather than a cleanup detail.
package chaos

import (
	"fmt"
	"strings"
	"time"
)

// FaultKind enumerates the fault classes this engine can inject.
type FaultKind string

const (
	// FaultPartition severs network connectivity to or from the target.
	FaultPartition FaultKind = "partition"

	// FaultLatency delays responses without severing them, which is how a degraded
	// network differs from a dead one. A timeout-based client fails differently from one
	// facing a slow one, so both need testing.
	FaultLatency FaultKind = "latency"

	// FaultKill stops the target process or pod outright.
	FaultKill FaultKind = "kill"
)

// Valid reports whether the kind is one the engine understands.
func (k FaultKind) Valid() bool {
	switch k {
	case FaultPartition, FaultLatency, FaultKill:
		return true
	}
	return false
}

// TargetKind selects which backend performs the injection.
type TargetKind string

const (
	// TargetDocker injects into the local Docker Compose stack.
	TargetDocker TargetKind = "docker"
	// TargetKubernetes injects into a cluster workload.
	TargetKubernetes TargetKind = "kubernetes"
	// TargetLocal injects into a process the engine supervises. Used by tests.
	TargetLocal TargetKind = "local"
)

// Valid reports whether the backend is one the engine understands.
func (t TargetKind) Valid() bool {
	switch t {
	case TargetDocker, TargetKubernetes, TargetLocal:
		return true
	}
	return false
}

// Experiment is a declarative fault-injection plan.
//
// Declarative rather than procedural so an experiment can be reviewed, stored, and replayed
// by someone who did not write it. A chaos run is a test, and tests belong in version
// control.
type Experiment struct {
	// Name identifies the experiment and appears in logs and results.
	Name string `json:"name"`

	// Kind is the fault to inject.
	Kind FaultKind `json:"kind"`

	// Target selects the injection backend.
	Target TargetKind `json:"target"`

	// Subject is the specific thing being faulted: a container name, or "namespace/name"
	// for a Kubernetes workload.
	Subject string `json:"subject"`

	// Duration is how long the fault stays in place before recovery.
	//
	// It must be long enough for the fault to be observable by the system under test. A
	// partition recovered in 200ms may be masked entirely by connection reuse, producing a
	// green result that proves nothing.
	Duration time.Duration `json:"duration"`

	// SettleTime is how long to wait after recovery before declaring the verdict, allowing
	// in-flight work to complete and metrics to converge. Without it a system is judged
	// while it is still recovering.
	SettleTime time.Duration `json:"settleTime"`
}

// Defaults for fields that are usually left unset.
const (
	DefaultDuration   = 10 * time.Second
	DefaultSettleTime = 10 * time.Second
	MaxDuration       = 5 * time.Minute
	MinDuration       = time.Second
)

// Validate rejects an experiment that could not do what it claims.
//
// The duration bounds matter more than they look. A zero duration injects and immediately
// recovers, which reliably produces a false pass; an unbounded duration can wedge a shared
// development cluster. Both are refused rather than clamped, because a silent clamp would
// mean the experiment did not run as written.
func (e Experiment) Validate() error {
	if strings.TrimSpace(e.Name) == "" {
		return fmt.Errorf("experiment name is required")
	}
	if !e.Kind.Valid() {
		return fmt.Errorf("unknown fault kind %q", e.Kind)
	}
	if !e.Target.Valid() {
		return fmt.Errorf("unknown target %q", e.Target)
	}
	if strings.TrimSpace(e.Subject) == "" {
		return fmt.Errorf("subject is required")
	}
	if e.Duration < MinDuration {
		return fmt.Errorf("duration %v is below the %v minimum; a fault this short is usually "+
			"masked by connection reuse and proves nothing", e.Duration, MinDuration)
	}
	if e.Duration > MaxDuration {
		return fmt.Errorf("duration %v exceeds the %v maximum", e.Duration, MaxDuration)
	}
	return nil
}

// withDefaults returns a copy with unset timing fields filled in.
func (e Experiment) withDefaults() Experiment {
	out := e
	if out.Duration == 0 {
		out.Duration = DefaultDuration
	}
	if out.SettleTime == 0 {
		out.SettleTime = DefaultSettleTime
	}
	return out
}
