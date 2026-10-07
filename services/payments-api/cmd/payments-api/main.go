package main

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	grpc_health_v1 "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"

	paymentsv1 "github.com/YOUR_GITHUB_USER/resilientbank/services/payments-api/gen/payments/v1"
	"github.com/YOUR_GITHUB_USER/resilientbank/services/payments-api/internal/config"
	"github.com/YOUR_GITHUB_USER/resilientbank/services/payments-api/internal/payments"
)

func main() {
	// Initialize structured JSON logging
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	slog.SetDefault(logger)

	cfg := config.Load()
	logger.Info("starting payments-api service",
		"grpc_addr", cfg.GRPCAddr,
		"metrics_addr", cfg.MetricsAddr,
		"redis_addr", cfg.RedisAddr,
	)

	// Configure and connect to PostgreSQL
	poolCtx, poolCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer poolCancel()

	pgConfig, err := pgxpool.ParseConfig(cfg.DatabaseURL)
	if err != nil {
		logger.Error("invalid DATABASE_URL", "error", err)
		os.Exit(1)
	}

	dbPool, err := pgxpool.NewWithConfig(poolCtx, pgConfig)
	if err != nil {
		logger.Error("failed to create postgres connection pool (is docker compose running?)", "error", err)
		os.Exit(1)
	}

	// Fail fast if PostgreSQL is unreachable
	pingCtx, pingCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer pingCancel()
	if err := dbPool.Ping(pingCtx); err != nil {
		logger.Error("failed to connect to PostgreSQL (is docker compose running?)", "error", err)
		os.Exit(1)
	}
	logger.Info("connected to PostgreSQL successfully")

	// Connect to Redis with tight resilience timeouts (never blocks payments)
	redisClient := redis.NewClient(&redis.Options{
		Addr:         cfg.RedisAddr,
		DialTimeout:  200 * time.Millisecond,
		ReadTimeout:  200 * time.Millisecond,
		WriteTimeout: 200 * time.Millisecond,
		MaxRetries:   1,
	})

	redisPingCtx, redisPingCancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer redisPingCancel()
	if err := redisClient.Ping(redisPingCtx).Err(); err != nil {
		logger.Warn("redis is unreachable at startup; continuing with postgres only", "error", err)
	} else {
		logger.Info("connected to Redis successfully")
	}

	// Metrics HTTP server exposing standard Go/process and custom metrics
	metricsMux := http.NewServeMux()
	metricsMux.Handle("/metrics", promhttp.Handler())
	metricsServer := &http.Server{
		Addr:    cfg.MetricsAddr,
		Handler: metricsMux,
	}

	go func() {
		logger.Info("serving prometheus metrics", "addr", cfg.MetricsAddr)
		if err := metricsServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("metrics HTTP server error", "error", err)
		}
	}()

	// Listen on gRPC port
	lis, err := net.Listen("tcp", cfg.GRPCAddr)
	if err != nil {
		logger.Error("failed to bind gRPC listener", "addr", cfg.GRPCAddr, "error", err)
		os.Exit(1)
	}

	grpcServer := grpc.NewServer()

	// Register payments service handler
	paymentService := payments.NewService(dbPool, redisClient, cfg, logger)
	paymentsv1.RegisterPaymentServiceServer(grpcServer, paymentService)

	// Register gRPC Health Checking Protocol
	healthServer := health.NewServer()
	grpc_health_v1.RegisterHealthServer(grpcServer, healthServer)
	healthServer.SetServingStatus("", grpc_health_v1.HealthCheckResponse_SERVING)
	healthServer.SetServingStatus("payments.v1.PaymentService", grpc_health_v1.HealthCheckResponse_SERVING)

	// Register gRPC Server Reflection for grpcurl / cli tools
	reflection.Register(grpcServer)

	go func() {
		logger.Info("serving gRPC", "addr", cfg.GRPCAddr)
		if err := grpcServer.Serve(lis); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			logger.Error("gRPC server error", "error", err)
		}
	}()

	// Handle graceful shutdown on OS signals (SIGINT, SIGTERM)
	stopSig := make(chan os.Signal, 1)
	signal.Notify(stopSig, syscall.SIGINT, syscall.SIGTERM)
	sig := <-stopSig
	logger.Info("shutdown signal received; terminating gracefully", "signal", sig.String())

	// 1. Mark health status NOT_SERVING to stop new incoming traffic
	healthServer.SetServingStatus("", grpc_health_v1.HealthCheckResponse_NOT_SERVING)
	healthServer.SetServingStatus("payments.v1.PaymentService", grpc_health_v1.HealthCheckResponse_NOT_SERVING)

	// 2. GracefulStop with a 10s fallback timeout
	stopped := make(chan struct{})
	go func() {
		grpcServer.GracefulStop()
		close(stopped)
	}()

	select {
	case <-stopped:
		logger.Info("gRPC server gracefully stopped")
	case <-time.After(10 * time.Second):
		logger.Warn("gRPC server graceful stop timed out after 10s; forcing Stop")
		grpcServer.Stop()
	}

	// 3. Terminate metrics server
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer shutdownCancel()
	if err := metricsServer.Shutdown(shutdownCtx); err != nil {
		logger.Error("metrics server shutdown error", "error", err)
	} else {
		logger.Info("metrics HTTP server gracefully stopped")
	}

	// 4. Clean up connection pool and Redis client
	dbPool.Close()
	logger.Info("postgresql pool closed")

	if err := redisClient.Close(); err != nil {
		logger.Warn("redis client close error", "error", err)
	} else {
		logger.Info("redis client closed")
	}

	logger.Info("shutdown complete")
}
