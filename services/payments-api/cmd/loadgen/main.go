// Command loadgen drives sustained CreatePayment traffic against a running API and
// reports the latency and error profile against explicit SLOs.
//
// It exists because the resilience claims are only meaningful if the normal path is fast
// and stays fast. A soak run distinguishes a service that is correct under light load from
// one that degrades once the outbox, Redis and PostgreSQL are actually busy.
//
// The tool is deliberately dependency-free: it uses the generated client and the standard
// library. Duplicate idempotency keys are replayed on purpose so the Redis fast path is
// exercised under the same load as the write path.
package main

import (
	"context"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"time"

	paymentsv1 "github.com/YOUR_GITHUB_USER/resilientbank/services/payments-api/gen/payments/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

func main() {
	var (
		addr      = flag.String("addr", "localhost:50051", "gRPC endpoint of the payment API")
		duration  = flag.Duration("duration", 30*time.Second, "measured load duration (after warmup)")
		warmup    = flag.Duration("warmup", 3*time.Second, "traffic sent but excluded from stats")
		workers   = flag.Int("workers", 16, "concurrent client goroutines")
		rate      = flag.Int("rate", 200, "target total requests per second (0 = unthrottled)")
		from      = flag.String("from", "acc-bank", "source account id")
		to        = flag.String("to", "acc-alice", "destination account id")
		amount    = flag.Int64("amount", 100, "amount in minor units")
		currency  = flag.String("currency", "EUR", "ISO 4217 currency code")
		dupRatio  = flag.Float64("dup-ratio", 0.1, "fraction of requests that replay a recent idempotency key")
		timeout   = flag.Duration("timeout", 2*time.Second, "per-request deadline")
		sloP95    = flag.Duration("slo-p95", 150*time.Millisecond, "p95 latency SLO")
		sloP99    = flag.Duration("slo-p99", 400*time.Millisecond, "p99 latency SLO")
		sloErrors = flag.Float64("slo-error-rate", 0.001, "error-rate SLO as a fraction")
	)
	flag.Parse()

	if *workers < 1 {
		fmt.Fprintln(os.Stderr, "workers must be >= 1")
		os.Exit(2)
	}
	if *duration <= 0 {
		fmt.Fprintln(os.Stderr, "duration must be > 0")
		os.Exit(2)
	}
	if *dupRatio < 0 || *dupRatio > 1 {
		fmt.Fprintln(os.Stderr, "dup-ratio must be between 0 and 1")
		os.Exit(2)
	}

	conn, err := grpc.NewClient(*addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Fprintf(os.Stderr, "dial %s: %v\n", *addr, err)
		os.Exit(1)
	}
	defer conn.Close()

	if err := preflight(conn); err != nil {
		fmt.Fprintf(os.Stderr, "preflight health check failed: %v\n", err)
		fmt.Fprintln(os.Stderr, "is the stack running? try: make up-all")
		os.Exit(1)
	}

	fmt.Printf("loadgen -> %s  duration=%s  warmup=%s  workers=%d  rate=%d/s\n",
		*addr, *duration, *warmup, *workers, *rate)

	hist := newHistogram(100*time.Microsecond, 100_000) // 100us buckets, 10s range
	var (
		success  atomic.Int64
		failed   atomic.Int64
		dupSent  atomic.Int64
		dupOK    atomic.Int64
		measured atomic.Bool
		lastErr  atomic.Value
	)

	runStart := time.Now()
	totalRun := *warmup + *duration
	ctx, cancel := context.WithTimeout(context.Background(), totalRun)
	defer cancel()

	// Warmup traffic is identical to measured traffic; only the accounting differs.
	go func() {
		select {
		case <-time.After(*warmup):
			measured.Store(true)
		case <-ctx.Done():
		}
	}()

	client := paymentsv1.NewPaymentServiceClient(conn)
	var wg sync.WaitGroup
	runID := runStart.UnixNano()

	var interval time.Duration
	if *rate > 0 {
		interval = time.Duration(float64(time.Second) * float64(*workers) / float64(*rate))
	}

	for w := 0; w < *workers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			// A small ring of recent keys per worker. Replaying one of these must return
			// the original result without applying the transfer twice.
			ring := make([]string, 0, 256)
			var seq int64
			next := time.Now()

			for ctx.Err() == nil {
				if interval > 0 {
					next = next.Add(interval)
					if d := time.Until(next); d > 0 {
						select {
						case <-time.After(d):
						case <-ctx.Done():
							return
						}
					}
				}

				dup := len(ring) > 0 && rand.Float64() < *dupRatio
				var key string
				if dup {
					key = ring[rand.Intn(len(ring))]
					dupSent.Add(1)
				} else {
					seq++
					key = fmt.Sprintf("%d-%d-%d", runID, workerID, seq)
					if len(ring) < cap(ring) {
						ring = append(ring, key)
					} else {
						ring[rand.Intn(len(ring))] = key
					}
				}

				rctx, rcancel := context.WithTimeout(ctx, *timeout)
				start := time.Now()
				resp, err := client.CreatePayment(rctx, &paymentsv1.CreatePaymentRequest{
					IdempotencyKey: key,
					FromAccount:    *from,
					ToAccount:      *to,
					AmountCents:    *amount,
					Currency:       *currency,
				})
				elapsed := time.Since(start)
				rcancel()

				if measured.Load() {
					if err != nil {
						failed.Add(1)
						lastErr.Store(err.Error())
					} else {
						success.Add(1)
						if dup && resp.GetStatus() == "ACCEPTED" {
							dupOK.Add(1)
						}
						hist.record(elapsed)
					}
				}
			}
		}(w)
	}

	wg.Wait()
	measuredAt := time.Now()

	ok := success.Load()
	bad := failed.Load()
	total := ok + bad
	errRate := 0.0
	if total > 0 {
		errRate = float64(bad) / float64(total)
	}
	statsDuration := measuredAt.Sub(runStart.Add(*warmup))
	if statsDuration <= 0 {
		statsDuration = *duration
	}
	rps := float64(total) / statsDuration.Seconds()
	p50, p95, p99, max, mean := hist.percentiles()

	fmt.Println()
	fmt.Printf("measured window : %s\n", statsDuration.Round(time.Millisecond))
	fmt.Printf("requests        : %d  (ok=%d errors=%d)\n", total, ok, bad)
	fmt.Printf("throughput      : %.0f req/s\n", rps)
	fmt.Printf("error rate      : %.4f%%\n", errRate*100)
	fmt.Printf("duplicate sends : %d (%d accepted)\n", dupSent.Load(), dupOK.Load())
	fmt.Printf("latency         : p50=%s p95=%s p99=%s max=%s mean=%s\n",
		p50.Round(time.Microsecond), p95.Round(time.Microsecond),
		p99.Round(time.Microsecond), max.Round(time.Microsecond), mean.Round(time.Microsecond))
	if e, _ := lastErr.Load().(string); e != "" {
		fmt.Printf("last error      : %s\n", e)
	}

	var violations []string
	if errRate > *sloErrors {
		violations = append(violations, fmt.Sprintf("error rate %.4f%% > SLO %.4f%%", errRate*100, *sloErrors*100))
	}
	if p95 > *sloP95 {
		violations = append(violations, fmt.Sprintf("p95 %s > SLO %s", p95.Round(time.Microsecond), *sloP95))
	}
	if p99 > *sloP99 {
		violations = append(violations, fmt.Sprintf("p99 %s > SLO %s", p99.Round(time.Microsecond), *sloP99))
	}
	if total == 0 {
		violations = append(violations, "no measured requests")
	}

	fmt.Println()
	if len(violations) > 0 {
		fmt.Println("SLO RESULT: FAIL")
		for _, v := range violations {
			fmt.Printf("  - %s\n", v)
		}
		os.Exit(1)
	}
	fmt.Println("SLO RESULT: PASS")
}

func preflight(conn *grpc.ClientConn) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := healthpb.NewHealthClient(conn).Check(ctx, &healthpb.HealthCheckRequest{})
	return err
}

// histogram is a fixed-memory latency histogram. Percentiles are reported at its bucket
// resolution (100us by default), which is far finer than the SLOs it is compared against
// and avoids holding one sample per request during a long soak.
type histogram struct {
	resolution time.Duration
	mu         sync.Mutex
	buckets    []int64
	overflow   int64
	count      int64
	sum        time.Duration
	max        time.Duration
}

func newHistogram(resolution time.Duration, size int) *histogram {
	return &histogram{resolution: resolution, buckets: make([]int64, size)}
}

func (h *histogram) record(d time.Duration) {
	if d < 0 {
		d = 0
	}
	idx := int(d / h.resolution)
	h.mu.Lock()
	if idx >= len(h.buckets) {
		h.overflow++
	} else {
		h.buckets[idx]++
	}
	h.count++
	h.sum += d
	if d > h.max {
		h.max = d
	}
	h.mu.Unlock()
}

func (h *histogram) percentiles() (p50, p95, p99, max, mean time.Duration) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.count == 0 {
		return 0, 0, 0, 0, 0
	}
	mean = h.sum / time.Duration(h.count)
	max = h.max

	pick := func(p float64) time.Duration {
		target := int64(float64(h.count) * p)
		if target < 1 {
			target = 1
		}
		var seen int64
		for i, c := range h.buckets {
			seen += c
			if seen >= target {
				return time.Duration(i+1) * h.resolution
			}
		}
		return max
	}
	return pick(0.50), pick(0.95), pick(0.99), max, mean
}
