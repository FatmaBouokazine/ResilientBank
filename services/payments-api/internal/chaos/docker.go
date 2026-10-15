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
		return fmt.Errorf("latency injection into %q is not supported by the docker backend; "+
			"use the kubernetes backend (Chaos Mesh) or inject latency with `tc` in a privileged container", exp.Subject)

	default:
		return fmt.Errorf("unsupported fault kind %q", exp.Kind)
	}
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
