package chaos

import (
	"context"
	"fmt"
	"sync"
)

// Injector applies and reverses a fault.
//
// RECOVERY IS PART OF THE CONTRACT, NOT AN OPTIONAL EXTRA.
// Every implementation must guarantee that Recover restores the subject to a usable state,
// because a half-applied partition left behind would corrupt every later experiment and,
// worse, make a broken system look healthy.
type Injector interface {
	// Inject applies the fault. It returns an error if the fault could NOT be applied,
	// which the runner treats as a failed experiment rather than a pass.
	Inject(ctx context.Context, exp Experiment) error

	// Recover reverses the fault. It must be safe to call even if Inject failed or was
	// only partially applied.
	Recover(ctx context.Context, exp Experiment) error

	// Describe returns a human-readable summary of what this injector would do.
	Describe(exp Experiment) string
}

// InjectorFactory builds an Injector for a target kind.
type InjectorFactory func() (Injector, error)

var (
	factoriesMu sync.RWMutex
	factories   = map[TargetKind]InjectorFactory{}
)

// Register makes an Injector available to the engine.
//
// Registration exists so tests and future backends can plug in without the runner knowing
// about them. It panics on a duplicate registration because that can only ever be a
// programming error, and silently ignoring it would leave a backend quietly unreachable.
func Register(kind TargetKind, factory InjectorFactory) {
	factoriesMu.Lock()
	defer factoriesMu.Unlock()
	if _, exists := factories[kind]; exists {
		panic(fmt.Sprintf("chaos: injector already registered for target %q", kind))
	}
	factories[kind] = factory
}

// InjectorFor returns an Injector for the experiment's target.
func InjectorFor(exp Experiment) (Injector, error) {
	factoriesMu.RLock()
	factory, ok := factories[exp.Target]
	factoriesMu.RUnlock()

	if !ok {
		return nil, fmt.Errorf("no injector registered for target %q", exp.Target)
	}
	return factory()
}

// replaceFactoryForTest swaps a registration without the duplicate-registration panic.
//
// Production code must use Register, where a duplicate is a genuine programming error. The
// chaos tests each need their own fake, so they use this instead of weakening the invariant.
func replaceFactoryForTest(kind TargetKind, factory InjectorFactory) {
	factoriesMu.Lock()
	defer factoriesMu.Unlock()
	factories[kind] = factory
}
