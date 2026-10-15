// Command outbox-relayer drains the transactional outbox to Kafka.
//
// It runs separately from payments-api so that broker unavailability, producer stalls, and
// partition backpressure can never reach the gRPC ingestion path. Payments are committed to
// PostgreSQL regardless of whether this process is running, which is the property that
// makes the system tolerate a total Kafka outage without losing a transfer.
//
// Usage:
//
//	go run ./cmd/outbox-relayer
//
// Configuration comes from environment variables; see internal/config.
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/YOUR_GITHUB_USER/resilientbank/services/payments-api/internal/config"
	"github.com/YOUR_GITHUB_USER/resilientbank/services/payments-api/internal/kafka"
	"github.com/YOUR_GITHUB_USER/resilientbank/services/payments-api/internal/metrics"
	"github.com/YOUR_GITHUB_USER/resilientbank/services/payments-api/internal/outbox"
)

// shutdownGrace is how long the relayer may take to finish its current batch after a
// signal. It must exceed the configured publish timeout, otherwise a slow broker response
// would be cut off mid-publish and force a duplicate redelivery.
const shutdownGrace = 30 * time.Second

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	if err := run(logger); err != nil {
		logger.Error("relayer exited with an error", "error", err)
		os.Exit(1)
	}
}

func run(logger *slog.Logger) error {
	cfg := config.Load()

	logger.Info("starting outbox-relayer",
		"kafka_brokers", cfg.Kafka.Brokers,
		"kafka_enabled", cfg.Kafka.Enabled,
		"topic", cfg.Kafka.TopicEvents,
		"dlq_topic", cfg.Kafka.TopicDLQ,
		"batch_size", cfg.Kafka.PollBatchSize,
		"poll_interval", cfg.Kafka.PollInterval,
		"max_attempts", cfg.Kafka.MaxAttempts,
	)

	// PostgreSQL is a hard dependency. Without it there is nothing to relay, so failing
	// fast is correct here, unlike the payment API which cannot proceed either.
	poolCtx, poolCancel := context.WithTimeout(context.Background(), 10*time.Second)
	pool, err := pgxpool.New(poolCtx, cfg.DatabaseURL)
	poolCancel()
	if err != nil {
		return err
	}
	defer pool.Close()

	pingCtx, pingCancel := context.WithTimeout(context.Background(), 5*time.Second)
	if err := pool.Ping(pingCtx); err != nil {
		pingCancel()
		return errors.New("failed to connect to PostgreSQL (is `make up` running?): " + err.Error())
	}
	pingCancel()
	logger.Info("connected to PostgreSQL")

	publisher, err := kafka.NewProducer(
		cfg.Kafka.Brokers,
		cfg.Kafka.ClientID,
		cfg.Kafka.Enabled,
		cfg.Kafka.PublishTimeout,
	)
	if err != nil {
		// A broker that is merely unreachable is not a startup failure: NewProducer only
		// fails on misconfiguration. The relayer must still start and drain the backlog
		// once the broker returns.
		return err
	}
	defer publisher.Close()

	if publisher.Enabled() {
		logger.Info("kafka producer ready", "brokers", cfg.Kafka.Brokers)
	} else {
		logger.Warn("kafka publishing is DISABLED; events will accumulate in the outbox")
	}

	relayer := outbox.NewRelayer(outbox.NewStore(pool), publisher, outboxConfig(cfg), logger)

	// Publish the outbox series at zero before serving metrics. Without this, a labelled
	// counter such as the dead-letter metric has no series until its first event, so an
	// operator cannot tell "nothing has failed yet" apart from "nothing is instrumented".
	metrics.InitOutboxMetrics()

	// Metrics run in their own server so a scrape never competes with the publish loop,
	// and so backlog depth stays observable even when the broker is down.
	metricsServer := &http.Server{
		Addr:              cfg.MetricsAddr,
		Handler:           metricsHandler(),
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		logger.Info("serving relayer metrics", "addr", cfg.MetricsAddr)
		if err := metricsServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("metrics server error", "error", err)
		}
	}()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	relayErr := relayer.Run(ctx)

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), shutdownGrace)
	defer shutdownCancel()
	if err := metricsServer.Shutdown(shutdownCtx); err != nil {
		logger.Warn("metrics server shutdown error", "error", err)
	}

	logger.Info("shutdown complete")
	return relayErr
}

// outboxConfig maps the shared configuration onto the relayer's behaviour.
func outboxConfig(cfg *config.Config) outbox.RelayerConfig {
	relay := outbox.DefaultRelayerConfig()
	relay.Topic = cfg.Kafka.TopicEvents
	relay.TopicDLQ = cfg.Kafka.TopicDLQ
	relay.BatchSize = cfg.Kafka.PollBatchSize
	relay.PollInterval = cfg.Kafka.PollInterval
	relay.MaxAttempts = cfg.Kafka.MaxAttempts
	relay.Backoff = outbox.BackoffPolicy{
		Base:           cfg.Kafka.BackoffBase,
		Max:            cfg.Kafka.BackoffMax,
		JitterFraction: 0.2,
	}
	// The lease must outlast a publish attempt, otherwise a slow but healthy broker would
	// let a second relayer claim the same row and duplicate the event.
	relay.Lease = cfg.Kafka.PublishTimeout * 2
	return relay
}
