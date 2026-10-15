// Command chaos runs resilience experiments against a running stack and verifies that the
// platform's guarantees survive them.
//
// Usage:
//
//	go run ./cmd/chaos list
//	go run ./cmd/chaos run <experiment>
//	go run ./cmd/chaos run kafka-partition --duration 30s
//
// Exit codes are meaningful for CI:
//
//	0  the system behaved as designed (resilient, or degraded and recovered)
//	1  an invariant was violated, or the system failed to recover
//	2  the experiment could not be run (bad spec, no injector, no database)
//
// A fault that could not be injected exits 2 rather than 0. Reporting a pass for an
// experiment that never happened is the one failure mode a chaos tool must not have.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/YOUR_GITHUB_USER/resilientbank/services/payments-api/internal/chaos"
	"github.com/YOUR_GITHUB_USER/resilientbank/services/payments-api/internal/config"
)

func main() {
	if err := run(); err != nil {
		var exit exitError
		if errors.As(err, &exit) {
			os.Exit(exit.code)
		}
		fmt.Fprintf(os.Stderr, "chaos: %v\n", err)
		os.Exit(2)
	}
}

// exitError carries a specific exit code out of the command.
type exitError struct {
	code int
	err  error
}

func (e exitError) Error() string { return e.err.Error() }
func (e exitError) Unwrap() error { return e.err }

func run() error {
	args := os.Args[1:]
	if len(args) == 0 {
		usage()
		return exitError{2, errors.New("no command given")}
	}

	switch args[0] {
	case "list":
		return listExperiments()
	case "run":
		return runExperiment(args[1:])
	case "-h", "--help", "help":
		usage()
		return nil
	default:
		usage()
		return exitError{2, fmt.Errorf("unknown command %q", args[0])}
	}
}

func usage() {
	fmt.Fprint(os.Stderr, `chaos - resilience experiment runner

Commands:
  list                  Show the built-in experiment catalogue
  run <name> [flags]    Run one experiment

Flags for run:
  --duration <dur>   Override the fault duration (default: the experiment's own)
  --database-url <u> Override the database used for SLO verification

Exit codes:
  0  behaved as designed
  1  invariant violated, or failed to recover
  2  the experiment could not be run
`)
}

// experiments is the catalogue. Keeping it in code rather than YAML means the experiments
// are type-checked, reviewable in a diff, and cannot drift from the fields the engine
// actually reads.
func experiments() map[string]chaos.Experiment {
	return map[string]chaos.Experiment{
		// The headline experiment. Kafka is where the outbox's guarantee is either real or
		// fictional: if a committed payment loses its event here, the design is wrong.
		"kafka-partition": {
			Name:       "kafka-partition",
			Kind:       chaos.FaultPartition,
			Target:     chaos.TargetDocker,
			Subject:    "rb-kafka",
			Duration:   20 * time.Second,
			SettleTime: 15 * time.Second,
		},
		// Killing the broker outright. Distinct from a partition: a stopped container
		// refuses connections immediately, while a partition lets connections hang until
		// they time out. Clients fail differently in the two cases.
		"kafka-kill": {
			Name:       "kafka-kill",
			Kind:       chaos.FaultKill,
			Target:     chaos.TargetDocker,
			Subject:    "rb-kafka",
			Duration:   20 * time.Second,
			SettleTime: 15 * time.Second,
		},
		// The relay is the component whose failure the outbox is designed to absorb.
		"relayer-kill": {
			Name:       "relayer-kill",
			Kind:       chaos.FaultKill,
			Target:     chaos.TargetLocal,
			Subject:    "outbox-relayer",
			Duration:   20 * time.Second,
			SettleTime: 20 * time.Second,
		},
		// If a committed payment can be lost here, the platform has failed its purpose.
		"postgres-kill": {
			Name:       "postgres-kill",
			Kind:       chaos.FaultKill,
			Target:     chaos.TargetDocker,
			Subject:    "rb-postgres",
			Duration:   15 * time.Second,
			SettleTime: 20 * time.Second,
		},
	}
}

func listExperiments() error {
	for name, exp := range experiments() {
		fmt.Printf("%-16s %-10s %-12s %v\n", name, exp.Kind, exp.Target, exp.Duration)
	}
	return nil
}

func runExperiment(args []string) error {
	if len(args) == 0 {
		return exitError{2, errors.New("run requires an experiment name")}
	}

	name := args[0]
	exp, ok := experiments()[name]
	if !ok {
		return exitError{2, fmt.Errorf("unknown experiment %q; run 'chaos list' to see the catalogue", name)}
	}

	var durationOverride time.Duration
	databaseURL := os.Getenv("DATABASE_URL")

	for i := 1; i < len(args); i++ {
		switch args[i] {
		case "--duration":
			if i+1 >= len(args) {
				return exitError{2, errors.New("--duration needs a value")}
			}
			d, err := time.ParseDuration(args[i+1])
			if err != nil {
				return exitError{2, fmt.Errorf("invalid --duration: %w", err)}
			}
			durationOverride = d
			i++
		case "--database-url":
			if i+1 >= len(args) {
				return exitError{2, errors.New("--database-url needs a value")}
			}
			databaseURL = args[i+1]
			i++
		default:
			return exitError{2, fmt.Errorf("unknown flag %q", args[i])}
		}
	}

	if durationOverride > 0 {
		exp.Duration = durationOverride
	}

	if err := exp.Validate(); err != nil {
		return exitError{2, err}
	}

	if databaseURL == "" {
		databaseURL = config.Load().DatabaseURL
	}

	ctx := context.Background()

	poolCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	pool, err := pgxpool.New(poolCtx, databaseURL)
	cancel()
	if err != nil {
		return exitError{2, err}
	}
	defer pool.Close()

	pingCtx, pingCancel := context.WithTimeout(ctx, 5*time.Second)
	defer pingCancel()
	if err := pool.Ping(pingCtx); err != nil {
		return exitError{2, fmt.Errorf("cannot reach the database for SLO verification: %w", err)}
	}

	observer := chaos.NewOutboxObserver(pool, 2*time.Minute, 10_000)
	runner := &chaos.Runner{
		Observer:        observer,
		SampleInterval:  2 * time.Second,
		RecoveryTimeout: 90 * time.Second,
		Logger:          consoleLogger{},
	}

	fmt.Printf("running experiment %q: %s/%s on %s for %v\n\n",
		exp.Name, exp.Kind, exp.Target, exp.Subject, exp.Duration)

	report, runErr := runner.Run(ctx, exp)

	printReport(report)

	if runErr != nil {
		return exitError{1, runErr}
	}
	if !report.Passed() {
		return exitError{1, fmt.Errorf("experiment %q failed: %s", exp.Name, report.Verdict)}
	}
	return nil
}

type consoleLogger struct{}

func (consoleLogger) Infof(format string, args ...any) {
	fmt.Printf("  %s\n", strings.TrimRight(fmt.Sprintf(format, args...), "\n"))
}

func (consoleLogger) Errorf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "  %s\n", strings.TrimRight(fmt.Sprintf(format, args...), "\n"))
}

func printReport(r chaos.Report) {
	fmt.Printf("\nexperiment : %s\n", r.Experiment.Name)
	fmt.Printf("verdict    : %s\n", r.Verdict)
	fmt.Printf("duration   : %v\n", r.EndedAt.Sub(r.StartedAt).Truncate(time.Millisecond))

	if r.RecoveryObservations > 0 {
		fmt.Printf("recovery   : healthy after %v (%d observation(s))\n",
			r.RecoveryTime.Truncate(time.Millisecond), r.RecoveryObservations)
	} else {
		fmt.Println("recovery   : NOT OBSERVED")
	}

	fmt.Println("\nobjectives:")
	for _, obj := range r.ObjectiveBreakdown {
		state := "HELD"
		if !obj.HeldDuringFault {
			state = "DEGRADED"
		}
		recovered := "no"
		if obj.Recovered {
			recovered = "yes"
		}
		fmt.Printf("  %-20s %-9s samples=%-3d recovered=%s\n",
			obj.SLOName, state, obj.SamplesDuringFault, recovered)
	}

	if len(r.Notes) > 0 {
		fmt.Println("\nnotes:")
		for _, n := range r.Notes {
			fmt.Printf("  - %s\n", n)
		}
	}

	fmt.Println("\n" + r.Summary())
}

// printJSON renders the report as JSON for CI ingestion.
func printJSON(r chaos.Report) {
	b, _ := json.MarshalIndent(r, "", "  ")
	fmt.Println(string(b))
}
