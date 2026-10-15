package chaos

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeInjector records what the runner asked it to do, and can be told to fail either step.
type fakeInjector struct {
	mu sync.Mutex

	injected  []Experiment
	recovered []Experiment

	injectErr    error
	recoverErr   error
	describe     string
	recoverCalls int

	// onInject and onRecover let a test make its observer reflect the injector, the way a
	// real observer reflects live system state.
	onInject  func()
	onRecover func()
}

func (f *fakeInjector) Inject(_ context.Context, exp Experiment) error {
	f.mu.Lock()
	err := f.injectErr
	if err == nil {
		f.injected = append(f.injected, exp)
	}
	hook := f.onInject
	f.mu.Unlock()

	if err != nil {
		return err
	}
	if hook != nil {
		hook()
	}
	return nil
}

func (f *fakeInjector) Recover(_ context.Context, exp Experiment) error {
	f.mu.Lock()
	f.recoverCalls++
	err := f.recoverErr
	if err == nil {
		f.recovered = append(f.recovered, exp)
	}
	hook := f.onRecover
	f.mu.Unlock()

	if err != nil {
		return err
	}
	if hook != nil {
		hook()
	}
	return nil
}

func (f *fakeInjector) Describe(Experiment) string {
	if f.describe == "" {
		return "fake injector"
	}
	return f.describe
}

func (f *fakeInjector) counts() (injected, recovered int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.injected), len(f.recovered)
}

// stateObserver reports whatever the system is currently doing.
//
// It is state-driven rather than a fixed script because the runner samples continuously
// while a fault is active. A scripted sequence runs out mid-fault and silently reverts to
// "healthy", which would make every experiment look resilient. A real observer reflects
// live system state, and so must this one.
type stateObserver struct {
	mu         sync.Mutex
	current    []Measurement
	observeErr error
}

func (o *stateObserver) set(ms []Measurement) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.current = ms
}

func (o *stateObserver) Observe(context.Context) ([]Measurement, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.observeErr != nil {
		return nil, o.observeErr
	}
	if o.current == nil {
		return healthyAll(), nil
	}
	return o.current, nil
}

// bindFaultState makes the observer reflect the injector: unhealthy while a fault is
// applied, healthy once it is recovered.
func bindFaultState(injector *fakeInjector, observer *stateObserver, underFault []Measurement) {
	injector.onInject = func() { observer.set(underFault) }
	injector.onRecover = func() { observer.set(healthyAll()) }
}

func healthyAll() []Measurement {
	return []Measurement{
		{SLOName: SLOPaymentAvailability, Healthy: true},
		{SLOName: SLOEventDurability, Healthy: true},
		{SLOName: SLOEventFreshness, Healthy: true},
	}
}
func unhealthy(names ...SLOName) []Measurement {
	want := map[SLOName]bool{}
	for _, n := range names {
		want[n] = true
	}
	return []Measurement{
		{SLOName: SLOPaymentAvailability, Healthy: !want[SLOPaymentAvailability]},
		{SLOName: SLOEventDurability, Healthy: !want[SLOEventDurability]},
		{SLOName: SLOEventFreshness, Healthy: !want[SLOEventFreshness]},
	}
}
func newTestInjector(f *fakeInjector) {
	replaceFactoryForTest(TargetLocal, func() (Injector, error) { return f, nil })
}

// validExperiment is the shortest experiment the runner will accept.
func validExperiment() Experiment {
	return Experiment{
		Name:       "unit-test",
		Kind:       FaultKill,
		Target:     TargetLocal,
		Subject:    "test-subject",
		Duration:   2 * time.Second,
		SettleTime: time.Second,
	}
}

func TestValidateRejectsExperimentsThatWouldProveNothing(t *testing.T) {
	cases := []struct {
		name string
		exp  Experiment
		want string
	}{
		{"no name", Experiment{Kind: FaultKill, Target: TargetLocal, Subject: "s", Duration: time.Second}, "name is required"},
		{"unknown kind", Experiment{Name: "n", Kind: "explode", Target: TargetLocal, Subject: "s", Duration: time.Second}, "unknown fault kind"},
		{"unknown target", Experiment{Name: "n", Kind: FaultKill, Target: "cloud", Subject: "s", Duration: time.Second}, "unknown target"},
		{"no subject", Experiment{Name: "n", Kind: FaultKill, Target: TargetLocal, Duration: time.Second}, "subject is required"},
		// The important one: a sub-second fault is masked by connection reuse and yields a
		// green run that demonstrates nothing.
		{"too short", Experiment{Name: "n", Kind: FaultKill, Target: TargetLocal, Subject: "s", Duration: 200 * time.Millisecond}, "below the"},
		{"zero duration", Experiment{Name: "n", Kind: FaultKill, Target: TargetLocal, Subject: "s"}, "below the"},
		{"too long", Experiment{Name: "n", Kind: FaultKill, Target: TargetLocal, Subject: "s", Duration: time.Hour}, "maximum"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.exp.Validate()
			if err == nil {
				t.Fatalf("Validate() = nil, want an error mentioning %q", tc.want)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("Validate() = %q, want it to mention %q", err, tc.want)
			}
		})
	}
}

func TestValidateAcceptsAWellFormedExperiment(t *testing.T) {
	if err := validExperiment().Validate(); err != nil {
		t.Errorf("Validate() = %v, want nil", err)
	}
}

func TestDefaultsAreApplied(t *testing.T) {
	got := Experiment{Name: "n", Kind: FaultKill, Target: TargetLocal, Subject: "s"}.withDefaults()
	if got.Duration != DefaultDuration {
		t.Errorf("Duration = %v, want %v", got.Duration, DefaultDuration)
	}
	if got.SettleTime != DefaultSettleTime {
		t.Errorf("SettleTime = %v, want %v", got.SettleTime, DefaultSettleTime)
	}
}

// A fault that was never applied must NEVER be reported as a pass. This is the single most
// dangerous failure mode of a chaos engine: reporting resilience for a fault that did not
// happen.
func TestFaultNotObservedIsNeverAPass(t *testing.T) {
	injector := &fakeInjector{injectErr: errors.New("container not found")}
	newTestInjector(injector)

	r := &Runner{Observer: &stateObserver{}, SampleInterval: 50 * time.Millisecond}

	report, err := r.Run(context.Background(), validExperiment())
	if err == nil {
		t.Fatal("Run() returned nil error for a failed injection")
	}
	if report.Verdict != VerdictFaultNotObserved {
		t.Errorf("Verdict = %s, want %s", report.Verdict, VerdictFaultNotObserved)
	}
	if report.Passed() {
		t.Error("Passed() = true for a fault that was never injected")
	}
}

// Recovery must be attempted even when injection fails, or a partially applied fault is
// left behind to corrupt every later experiment.
func TestRecoveryRunsEvenWhenInjectionFails(t *testing.T) {
	injector := &fakeInjector{injectErr: errors.New("partial apply")}
	newTestInjector(injector)

	r := &Runner{Observer: &stateObserver{}, SampleInterval: 50 * time.Millisecond}
	report, _ := r.Run(context.Background(), validExperiment())

	if injector.recoverCalls == 0 {
		t.Error("Recover was never called after a failed injection; a partial fault would be " +
			"left in place and corrupt subsequent experiments")
	}
	if report.Verdict == VerdictResilient {
		t.Error("a failed injection must not yield a resilient verdict")
	}
}
func TestRecoveryFailureIsReportedAsWorstCase(t *testing.T) {
	injector := &fakeInjector{recoverErr: errors.New("cannot undo partition")}
	newTestInjector(injector)

	r := &Runner{Observer: &stateObserver{}, SampleInterval: 50 * time.Millisecond, RecoveryTimeout: time.Second}
	report, err := r.Run(context.Background(), validExperiment())

	if err == nil {
		t.Fatal("Run() returned nil error despite recovery failing")
	}
	if report.Verdict != VerdictRecoveryFailed {
		t.Errorf("Verdict = %s, want %s", report.Verdict, VerdictRecoveryFailed)
	}
	if report.Passed() {
		t.Error("a failed recovery must never be a pass")
	}
}

// The headline guarantee: a committed payment is never lost, even while its event pipeline
// is being torn down.
func TestCommittedPaymentSurvivesFault(t *testing.T) {
	injector := &fakeInjector{}
	newTestInjector(injector)

	// Delivery degrades while the fault is active, then recovers. The payment guarantee is
	// unaffected throughout, which is the behaviour the outbox exists to provide.
	observer := &stateObserver{current: healthyAll()}
	bindFaultState(injector, observer, unhealthy(SLOEventFreshness))

	r := &Runner{Observer: observer, SampleInterval: 100 * time.Millisecond, RecoveryTimeout: 3 * time.Second}
	report, err := r.Run(context.Background(), validExperiment())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	if report.Verdict != VerdictDegraded {
		t.Errorf("Verdict = %s, want %s (delivery degraded, nothing lost)", report.Verdict, VerdictDegraded)
	}
	if !report.Passed() {
		t.Error("degraded-but-recovered must pass; that is the designed behaviour")
	}
	// RecoveryTime can legitimately be 0ns on a platform with coarse clock granularity,
	// so the assertion that recovery was observed cannot depend on it.
	if report.RecoveryObservations == 0 {
		t.Error("no post-recovery observation was taken, so recovery was never confirmed")
	}

	injected, recovered := injector.counts()
	if injected != 1 || recovered != 1 {
		t.Errorf("injector saw inject=%d recover=%d, want 1 and 1", injected, recovered)
	}
}

// Losing a committed payment is a real defect, not designed degradation, and must be
// reported as a violation.
func TestLostPaymentIsAnInvariantViolation(t *testing.T) {
	injector := &fakeInjector{}
	newTestInjector(injector)

	// A committed payment disappears while the fault is active.
	observer := &stateObserver{current: healthyAll()}
	bindFaultState(injector, observer, unhealthy(SLOPaymentAvailability))

	r := &Runner{Observer: observer, SampleInterval: 100 * time.Millisecond, RecoveryTimeout: 3 * time.Second}
	report, err := r.Run(context.Background(), validExperiment())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	if report.Verdict != VerdictViolated {
		t.Errorf("Verdict = %s, want %s", report.Verdict, VerdictViolated)
	}
	if report.Passed() {
		t.Error("losing a committed payment must never be reported as a pass")
	}
}

// A system that never returns to health has not demonstrated resilience.
func TestSystemThatNeverRecoversFails(t *testing.T) {
	injector := &fakeInjector{}
	newTestInjector(injector)

	observer := &alwaysUnhealthyObserver{}
	r := &Runner{Observer: observer, SampleInterval: 50 * time.Millisecond, RecoveryTimeout: 500 * time.Millisecond}

	report, err := r.Run(context.Background(), validExperiment())
	if err == nil {
		t.Fatal("Run() returned nil error for a system that never recovered")
	}
	if report.Verdict != VerdictRecoveryFailed {
		t.Errorf("Verdict = %s, want %s", report.Verdict, VerdictRecoveryFailed)
	}
}

// alwaysUnhealthyObserver reports the durability objective as failed forever.
type alwaysUnhealthyObserver struct{}

func (alwaysUnhealthyObserver) Observe(context.Context) ([]Measurement, error) {
	return []Measurement{
		{SLOName: SLOPaymentAvailability, Healthy: false, Detail: "down"},
		{SLOName: SLOEventDurability, Healthy: false, Detail: "backlog growing"},
	}, nil
}

func TestReportPassedSemantics(t *testing.T) {
	cases := map[Verdict]bool{
		VerdictResilient:        true,
		VerdictDegraded:         true,
		VerdictViolated:         false,
		VerdictFaultNotObserved: false,
		VerdictRecoveryFailed:   false,
	}
	for verdict, want := range cases {
		if got := (Report{Verdict: verdict}).Passed(); got != want {
			t.Errorf("Report{Perdict: %s}.Passed() = %v, want %v", verdict, got, want)
		}
	}
}

func TestObservationSetHealthyReportsFailures(t *testing.T) {
	allGood := ObservationSet{Measurements: healthyAll()}
	if ok, failed := allGood.Healthy(); !ok || len(failed) != 0 {
		t.Errorf("Healthy() = %v, %d failures; want true, 0", ok, len(failed))
	}

	mixed := ObservationSet{Measurements: unhealthy(SLOEventDurability)}
	ok, failed := mixed.Healthy()
	if ok {
		t.Error("Healthy() = true with a failing objective")
	}
	if len(failed) != 1 || failed[0].SLOName != SLOEventDurability {
		t.Errorf("failures = %+v, want one SLOEventDurability", failed)
	}
}
