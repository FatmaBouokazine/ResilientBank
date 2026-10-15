package chaos

import (
	"bytes"
	"context"
	"fmt"
	"os/exec"
	"strings"
	"time"
)

// KubernetesInjector injects faults into cluster workloads.
//
// It shells out to kubectl rather than importing client-go: the engine's other backends
// already depend on CLIs (docker, and soon tc via pumba), so a uniform "shell out to the
// operator's tool" keeps every injection auditable and avoids embedding a second cluster
// client whose version could skew from the server's.
//
// The Subject for a FaultKill is "namespace/name" of a pod. For a FaultPartition it is
// "namespace/label=value", so the fault selects the workload rather than one pod replica,
// which is what a partition actually severs.
type KubernetesInjector struct {
	// Kubectl is the binary name, injectable so tests can point at a stub.
	Kubectl string
}

func init() {
	Register(TargetKubernetes, func() (Injector, error) {
		return &KubernetesInjector{Kubectl: "kubectl"}, nil
	})
}

func (k *KubernetesInjector) binary() string {
	if k.Kubectl != "" {
		return k.Kubectl
	}
	return "kubectl"
}

func (k *KubernetesInjector) Describe(exp Experiment) string {
	switch exp.Kind {
	case FaultKill:
		return fmt.Sprintf("deleting pod %q; its controller recreates it and recovery is the recreation", exp.Subject)
	case FaultPartition:
		return fmt.Sprintf("applying a deny-all NetworkPolicy to %q for %v, then deleting the policy", exp.Subject, exp.Duration)
	case FaultLatency:
		return fmt.Sprintf("latency on %q is not supported by the kubectl backend", exp.Subject)
	default:
		return fmt.Sprintf("unsupported fault %q on %q", exp.Kind, exp.Subject)
	}
}

// Inject applies the fault.
func (k *KubernetesInjector) Inject(ctx context.Context, exp Experiment) error {
	switch exp.Kind {
	case FaultKill:
		namespace, name, err := splitSubject(exp.Subject)
		if err != nil {
			return err
		}
		if err := k.run(ctx, "delete", "pod", "-n", namespace, name, "--ignore-not-found=false"); err != nil {
			return fmt.Errorf("delete pod %q: %w (if the pod finished on its own, the fault target vanished)", exp.Subject, err)
		}
		return nil

	case FaultPartition:
		// A deny-all policy is the closest faithful equivalent of a network partition
		// expressible with stock Kubernetes. It severs both ingress and egress without
		// touching the pod itself, which is what makes recovery a single delete.
		return k.applyDenyAll(ctx, exp.Subject)

	case FaultLatency:
		return fmt.Errorf("latency on %q needs traffic shaping (Chaos Mesh or tc), which the "+
			"kubectl backend does not apply; use the docker backend with pumba", exp.Subject)

	default:
		return fmt.Errorf("unsupported fault kind %q", exp.Kind)
	}
}

// Recover reverses the fault.
func (k *KubernetesInjector) Recover(ctx context.Context, exp Experiment) error {
	switch exp.Kind {
	case FaultKill:
		// The pod's controller recreates it. There is nothing to undo, but the subject must
		// have come back: a controller that failed to reschedule is a real finding.
		namespace, name, err := splitSubject(exp.Subject)
		if err != nil {
			return err
		}
		if err := k.waitForPod(ctx, namespace, name); err != nil {
			return fmt.Errorf("pod %q did not return after kill: %w", exp.Subject, err)
		}
		return nil

	case FaultPartition:
		if err := k.run(ctx, "delete", "networkpolicy", "-n", policyNamespace(exp.Subject), policyName(exp.Subject), "--ignore-not-found"); err != nil {
			// "not found" is success for our purposes: the desired end state is the policy
			// being absent.
			if isNotFound(err) {
				return nil
			}
			return err
		}
		return nil

	default:
		return nil
	}
}

// applyDenyAll creates a policy that denies all ingress and egress to the pods matching the
// subject's selector. It is deliberately narrow in selector scope and broad in denied
// traffic: the fault is "this workload is isolated", never "the cluster is down".
func (k *KubernetesInjector) applyDenyAll(ctx context.Context, subject string) error {
	namespace, selector, err := splitSelector(subject)
	if err != nil {
		return err
	}

	key, value, err := splitKeyValue(selector)
	if err != nil {
		return err
	}

	policy := fmt.Sprintf(`apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: %s
  namespace: %s
  labels:
    resilientbank.io/chaos: "true"
spec:
  podSelector:
    matchLabels:
      %s: %s
  policyTypes:
  - Ingress
  - Egress
`, policyName(subject), namespace, key, value)

	cmdCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	cmd := exec.CommandContext(cmdCtx, k.binary(), "apply", "-f", "-")
	cmd.Stdin = strings.NewReader(policy)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		detail := strings.TrimSpace(stderr.String())
		if detail == "" {
			detail = err.Error()
		}
		return fmt.Errorf("apply deny-all policy: %s", detail)
	}
	return nil
}

// waitForPod blocks until the named pod is Running again. It does not require the same
// pod UID: after a kill the replacement has a new name, so it waits for any ready pod that
// is not the deleted one. In practice the controller recreates within seconds.
func (k *KubernetesInjector) waitForPod(ctx context.Context, namespace, name string) error {
	// The replacement has a new name. Wait for any Running pod in the namespace whose name
	// shares the pod's generated prefix (everything before the last dash).
	prefix := name
	if idx := strings.LastIndex(name, "-"); idx > 0 {
		prefix = name[:idx]
	}

	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		out, err := k.output(ctx, "get", "pods", "-n", namespace, "-o", "name")
		if err == nil {
			for _, line := range strings.Split(strings.TrimSpace(out), "\n") {
				short := strings.TrimPrefix(strings.TrimSpace(line), "pod/")
				if strings.HasPrefix(short, prefix) && short != name {
					if ready, _ := k.output(ctx, "get", "pod", "-n", namespace, short,
						"-o", "jsonpath={.status.phase}"); strings.TrimSpace(ready) == "Running" {
						return nil
					}
				}
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
	return fmt.Errorf("no replacement Running for %q within 60s", name)
}

func (k *KubernetesInjector) run(ctx context.Context, args ...string) error {
	cmdCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	cmd := exec.CommandContext(cmdCtx, k.binary(), args...)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		detail := strings.TrimSpace(stderr.String())
		if detail == "" {
			detail = err.Error()
		}
		return fmt.Errorf("kubectl %s: %s", strings.Join(args, " "), detail)
	}
	return nil
}

func (k *KubernetesInjector) output(ctx context.Context, args ...string) (string, error) {
	cmdCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	cmd := exec.CommandContext(cmdCtx, k.binary(), args...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		detail := strings.TrimSpace(stderr.String())
		if detail == "" {
			detail = err.Error()
		}
		return "", fmt.Errorf("kubectl %s: %s", strings.Join(args, " "), detail)
	}
	return stdout.String(), nil
}

func isNotFound(err error) bool {
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "notfound") || strings.Contains(msg, "not found")
}

// policyName derives a deterministic policy name from the subject so Recover deletes
// exactly what Inject created. The hash keeps it unique when two subjects share a prefix.
func policyName(subject string) string {
	safe := strings.Map(func(r rune) rune {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9', r == '-':
			return r
		case r >= 'A' && r <= 'Z':
			return r - 'A' + 'a'
		default:
			return '-'
		}
	}, subject)
	if len(safe) > 48 {
		safe = safe[:48]
	}
	return "chaos-deny-" + strings.Trim(safe, "-")
}

func policyNamespace(subject string) string {
	if idx := strings.Index(subject, "/"); idx > 0 {
		return subject[:idx]
	}
	return "default"
}

// splitSubject parses "namespace/name" for pod operations.
func splitSubject(subject string) (namespace, name string, err error) {
	parts := strings.SplitN(subject, "/", 2)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return "", "", fmt.Errorf("subject %q must be namespace/name", subject)
	}
	return parts[0], parts[1], nil
}

// splitSelector parses "namespace/label=value" for selector operations.
func splitSelector(subject string) (namespace, selector string, err error) {
	parts := strings.SplitN(subject, "/", 2)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return "", "", fmt.Errorf("subject %q must be namespace/label=value", subject)
	}
	return parts[0], parts[1], nil
}

func splitKeyValue(selector string) (string, string, error) {
	parts := strings.SplitN(selector, "=", 2)
	if len(parts) != 2 || parts[0] == "" {
		return "", "", fmt.Errorf("selector %q must be label=value", selector)
	}
	return parts[0], parts[1], nil
}
