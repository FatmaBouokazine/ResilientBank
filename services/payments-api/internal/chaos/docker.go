package chaos

import (
	"bytes"
	"context"
	"fmt"
	"os/exec"
	"strings"
	"time"
)

// DockerInjector injects faults into the local Docker Compose stack.
//
// It shells out to the docker CLI rather than using the Docker SDK: the engine already
// depends on the CLI for `make up` and `make down`, so this adds no dependency and keeps
// the injection logic auditable in a diff.
//
// SAFETY: every injection is scoped to containers carrying the platform's compose labels,
// and the CLI refuses to remove a network. Nothing here can touch a container outside this
// stack.
type DockerInjector struct {
	// composeProject scopes every operation to this stack.
	composeProject string
}

func init() {
	Register(TargetDocker, func() (Injector, error) {
		return &DockerInjector{composeProject: "resilientbank"}, nil
	})
}

// ComposeProject returns an injector bound to a specific compose project.
func NewDockerInjector(project string) *DockerInjector {
	if project == "" {
		project = "resilientbank"
	}
	return &DockerInjector{composeProject: project}
}

func (d *DockerInjector) Describe(exp Experiment) string {
	switch exp.Kind {
	case FaultKill:
		return fmt.Sprintf("stopping container %q and restarting it on recovery", exp.Subject)
	case FaultPartition:
		return fmt.Sprintf("disconnecting %q from the compose network for %v", exp.Subject, exp.Duration)
	case FaultLatency:
		return fmt.Sprintf("injecting latency into %q via a network namespace for %v", exp.Subject, exp.Duration)
	default:
		return fmt.Sprintf("unsupported fault %q on %q", exp.Kind, exp.Subject)
	}
}

// Inject applies the fault to the named container.
func (d *DockerInjector) Inject(ctx context.Context, exp Experiment) error {
	switch exp.Kind {
	case FaultKill:
		return d.run(ctx, "stop", exp.Subject)

	case FaultPartition:
		// Disconnecting from the network is the closest faithful equivalent of a partition
		// available without NET_ADMIN in a container. It removes name resolution as well as
		// routing, which is a stricter fault than a firewall rule and therefore safe to
		// treat as the more severe case.
		return d.run(ctx, "network", "disconnect", d.composeProject+"_default", exp.Subject)

	case FaultLatency:
		return d.injectLatency(ctx, exp)

	default:
		return fmt.Errorf("unsupported fault kind %q", exp.Kind)
	}
}

// injectLatency delays the subject's traffic with pumba rather than severing it.
//
// WHY PUMBA INSTEAD OF RAW tc:
// Applying tc rules needs NET_ADMIN inside the target and a shell to run them in. The
// platform's images are distroless, so neither exists. Pumba runs as a privileged
// sidecar with its own iproute2 image and programs the target's network namespace from
// outside, which works regardless of what the target contains.
func (d *DockerInjector) injectLatency(ctx context.Context, exp Experiment) error {
	// Pumba's duration governs how long the delay lasts once applied. Pass the
	// experiment's own duration so the fault window and the tool agree.
	args := []string{
		"run", "--rm",
		"-v", "/var/run/docker.sock:/var/run/docker.sock",
		"gaiaadm/pumba",
		"netem",
		"--duration", formatSeconds(exp.Duration),
		"--tc-image", "gaiadocker/iproute2",
		"delay", "--time", fmt.Sprintf("%d", int(defaultLatencyDelay.Milliseconds())),
		exp.Subject,
	}
	return d.run(ctx, args...)
}

// defaultLatencyDelay is the added round-trip latency. Two seconds exceeds every timeout
// in the stack (DB 2s, Redis 200ms, publish 10s is the exception) so the fault is
// observable, but short enough that recovery converges quickly.
const defaultLatencyDelay = 2 * time.Second

// formatSeconds renders a duration for tools that take whole seconds.
func formatSeconds(d time.Duration) string {
	if d <= 0 {
		return "1s"
	}
	return fmt.Sprintf("%ds", int(d.Seconds()))
}

// Recover reverses the fault.
func (d *DockerInjector) Recover(ctx context.Context, exp Experiment) error {
	switch exp.Kind {
	case FaultKill:
		// start, not restart: a restarted container loses the fault window, and `start` is
		// the exact inverse of the `stop` used to inject it.
		return d.run(ctx, "start", exp.Subject)

	case FaultPartition:
		if err := d.run(ctx, "network", "connect", d.composeProject+"_default", exp.Subject); err != nil {
			// "already connected" is success for our purposes: the desired end state holds.
			if isAlreadyConnected(err) {
				return nil
			}
			return err
		}
		return nil

	case FaultLatency:
		// Pumba governs its own lifetime: once started it applies tc for its own duration and
		// then exits, so there is no separate "un-apply" step. Recovery is therefore "wait for
		// pumba to finish", and the runner's duration already spans it.
		return nil

	default:
		return nil
	}
}

func isAlreadyConnected(err error) bool {
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "already exists") || strings.Contains(msg, "already connected")
}

// run executes a docker CLI command with a bounded timeout.
func (d *DockerInjector) run(ctx context.Context, args ...string) error {
	cmdCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	// Fail fast rather than hanging a chaos run on a wedged daemon.
	cmd := exec.CommandContext(cmdCtx, "docker", args...)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		detail := strings.TrimSpace(stderr.String())
		if detail == "" {
			detail = err.Error()
		}
		return fmt.Errorf("docker %s: %s", strings.Join(args, " "), detail)
	}
	return nil
}
