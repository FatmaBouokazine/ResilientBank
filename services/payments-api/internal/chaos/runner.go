package chaos

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// Runner executes an experiment and produces a verdict.
//
// It samples the SLOs before, during, and after the fault so the report can distinguish
// "survived it" from "was never really faulted", which are easy to confuse and have
// opposite meanings.
type Runner struct {
	// Observer evaluates the objectives.
	Observer Observer

	// SampleInterval is how often objectives are evaluated while a fault is active.
	SampleInterval time.Duration

	// RecoveryTimeout bounds how long the system has to recover before the experiment is
	// declared a failure.
	RecoveryTimeout time.Duration

	// Logger receives progress. Optional.
	Logger Logger
}

// Logger is the minimal logging surface the engine needs.
type Logger interface {
	Infof(format string, args ...any)
	Errorf(format string, args ...any)
}

type nopLogger struct{}

func (nopLogger) Infof(string, ...any)  {}
func (nopLogger) Errorf(string, ...any) {}

func (r *Runner) logger() Logger {
	if r.Logger != nil {
		return r.Logger
	}
	return nopLogger{}
}

// ErrNoInjector is returned when no backend is registered for the experiment's target.
var ErrNoInjector = errors.New("no injector registered for target")

// Run executes the experiment and returns a report.
//
// The experiment is guaranteed to attempt recovery even when injection or observation
// fails, because leaving a fault in place would corrupt the environment for every later
// run. A failed recovery is reported as the worst outcome rather than being swallowed.
func (r *Runner) Run(ctx context.Context, exp Experiment) (Report, error) {
	exp = exp.withDefaults()

	report := Report{
		Experiment: exp,
		StartedAt:  time.Now(),
	}

	if err := exp.Validate(); err != nil {
		report.EndedAt = time.Now()
		report.Verdict = VerdictFaultNotObserved
		report.Notes = append(report.Notes, fmt.Sprintf("invalid experiment: %v", err))
		return report, fmt.Errorf("invalid experiment %q: %w", exp.Name, err)
	}

	injector, err := InjectorFor(exp)
	if err != nil {
		report.EndedAt = time.Now()
		report.Verdict = VerdictFaultNotObserved
		report.Notes = append(report.Notes, err.Error())
		return report, err
	}

	log := r.logger()
	log.Infof("experiment %q: %s", exp.Name, injector.Describe(exp))

	// Baseline before any fault, so a later violation can be attributed to the experiment
	// rather than to a system that was already broken.
	baseline, err := r.observe(ctx)
	if err != nil {
		report.EndedAt = time.Now()
		report.Verdict = VerdictFaultNotObserved
		report.Notes = append(report.Notes, "baseline observation failed: "+err.Error())
		return report, err
	}
	report.AfterRecovery = append(report.AfterRecovery, baseline)

	// --- inject -------------------------------------------------------------
	if err := injector.Inject(ctx, exp); err != nil {
		// Attempt recovery anyway: a partially applied fault still needs reversing.
		if recErr := injector.Recover(context.WithoutCancel(ctx), exp); recErr != nil {
			report.Notes = append(report.Notes, "recovery after failed injection also failed: "+recErr.Error())
		}
		report.EndedAt = time.Now()
		report.Verdict = VerdictFaultNotObserved
		report.Notes = append(report.Notes, "injection failed: "+err.Error())
		return report, fmt.Errorf("inject fault for %q: %w", exp.Name, err)
	}

	faultActive := true
	// Recovery runs even if the caller cancels, so a cancelled experiment still leaves the
	// system usable.
	defer func() {
		if faultActive {
			recCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
			defer cancel()
			if recErr := injector.Recover(recCtx, exp); recErr != nil {
				report.Notes = append(report.Notes, "RECOVERY FAILED: "+recErr.Error())
			}
		}
	}()

	// --- sample under fault --------------------------------------------------
	rec := newRecorder(128)
	sampleCtx, stopSampling := context.WithTimeout(ctx, exp.Duration)
	ticker := time.NewTicker(r.sampleInterval())
	defer ticker.Stop()
	defer stopSampling()

	for {
		if obs, err := r.observe(sampleCtx); err == nil {
			rec.add(obs)
		}
		select {
		case <-sampleCtx.Done():
			// Drain one final observation before recovering, so the last state under fault
			// is captured.
			if obs, err := r.observe(context.WithoutCancel(ctx)); err == nil {
				rec.add(obs)
			}
			goto recovered
		case <-ticker.C:
		}
	}

recovered:
	// --- recover -------------------------------------------------------------
	faultActive = false
	report.DuringFault = rec.all()

	recoverStarted := time.Now()
	if err := injector.Recover(context.WithoutCancel(ctx), exp); err != nil {
		report.EndedAt = time.Now()
		report.Verdict = VerdictRecoveryFailed
		report.Notes = append(report.Notes, "recovery failed: "+err.Error())
		return report, fmt.Errorf("recover %q: %w", exp.Name, err)
	}

	// --- observe recovery ----------------------------------------------------
	timeout := r.recoveryTimeout()
	deadline := time.Now().Add(timeout)
	for {
		obs, err := r.observe(ctx)
		if err != nil {
			report.Notes = append(report.Notes, "recovery observation failed: "+err.Error())
		} else {
			report.AfterRecovery = append(report.AfterRecovery, obs)
			report.RecoveryObservations++
			if healthy, _ := obs.Healthy(); healthy {
				report.RecoveryTime = time.Since(recoverStarted)
				break
			}
		}
		if time.Now().After(deadline) {
			report.EndedAt = time.Now()
			report.Verdict = VerdictRecoveryFailed
			report.Notes = append(report.Notes,
				fmt.Sprintf("system did not return to health within %v of recovery", timeout))
			report.ObjectiveBreakdown = summarise(report.DuringFault, report.AfterRecovery[1:])
			return report, fmt.Errorf("experiment %q did not recover within %v", exp.Name, timeout)
		}
		time.Sleep(r.sampleInterval())
	}

	report.EndedAt = time.Now()

	// The breakdown MUST be computed before the verdict. judge() reads
	// ObjectiveBreakdown, and judging first means it reads an empty slice and returns
	// "resilient" for every experiment regardless of what was observed.
	report.ObjectiveBreakdown = summarise(report.DuringFault, report.AfterRecovery[1:])
	report.Verdict = judge(report)

	return report, nil
}

func (r *Runner) sampleInterval() time.Duration {
	if r.SampleInterval <= 0 {
		return time.Second
	}
	return r.SampleInterval
}

func (r *Runner) recoveryTimeout() time.Duration {
	if r.RecoveryTimeout <= 0 {
		return 60 * time.Second
	}
	return r.RecoveryTimeout
}

func (r *Runner) observe(ctx context.Context) (ObservationSet, error) {
	if r.Observer == nil {
		return ObservationSet{}, errors.New("no observer configured")
	}
	measurements, err := r.Observer.Observe(ctx)
	if err != nil {
		return ObservationSet{}, err
	}
	now := time.Now()
	for i := range measurements {
		if measurements[i].ObservedAt.IsZero() {
			measurements[i].ObservedAt = now
		}
	}
	return ObservationSet{Measurements: measurements, ObservedAt: now}, nil
}

// judge decides the verdict from what was observed.
//
// The distinction that matters: objectives that are EXPECTED to degrade while their own
// dependency is faulted are not violations. A payment API whose database is partitioned
// legitimately stops accepting writes. What must never break is the guarantee that an
// already-committed payment is never lost and its event never silently disappears.
func judge(report Report) Verdict {
	if len(report.DuringFault) == 0 {
		// The fault was applied but nothing was observed under it, so the run says nothing
		// either way. Reporting this as resilient would be a false pass.
		return VerdictFaultNotObserved
	}

	// Objectives that must hold no matter what. Losing one of these is a real defect.
	violations := 0
	for _, res := range report.ObjectiveBreakdown {
		if !res.HeldDuringFault && isCritical(res.SLOName) {
			violations++
		}
	}

	if violations > 0 {
		return VerdictViolated
	}

	// Nothing critical broke. If something degraded, that is the designed behaviour.
	for _, res := range report.ObjectiveBreakdown {
		if !res.HeldDuringFault {
			return VerdictDegraded
		}
	}
	return VerdictResilient
}

// isCritical identifies objectives that must survive every fault.
func isCritical(name SLOName) bool {
	switch name {
	case SLOPaymentAvailability, SLOEventDurability:
		return true
	}
	return false
}

// summarise reduces the sampled observations into one row per objective.
func summarise(duringFault, afterRecovery []ObservationSet) []ObjectiveResult {
	type accumulator struct {
		samples int
		healthy int
	}
	underFault := map[SLOName]*accumulator{}
	recovered := map[SLOName]*accumulator{}

	for _, set := range duringFault {
		for _, m := range set.Measurements {
			a, ok := underFault[m.SLOName]
			if !ok {
				a = &accumulator{}
				underFault[m.SLOName] = a
			}
			a.samples++
			if m.Healthy {
				a.healthy++
			}
		}
	}
	for _, set := range afterRecovery {
		for _, m := range set.Measurements {
			a, ok := recovered[m.SLOName]
			if !ok {
				a = &accumulator{}
				recovered[m.SLOName] = a
			}
			a.samples++
			if m.Healthy {
				a.healthy++
			}
		}
	}

	results := make([]ObjectiveResult, 0, len(underFault))
	for name, a := range underFault {
		res := ObjectiveResult{
			SLOName:            name,
			SamplesDuringFault: a.samples,
			HeldDuringFault:    a.samples > 0 && a.healthy == a.samples,
		}
		if r, ok := recovered[name]; ok {
			res.Recovered = r.samples > 0 && r.healthy > 0
		}
		results = append(results, res)
	}
	return results
}
