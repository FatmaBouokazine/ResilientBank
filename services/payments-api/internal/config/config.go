package config

import (
	"os"
	"time"
)

// Config holds runtime configuration options for the payment service.
type Config struct {
	GRPCAddr       string
	MetricsAddr    string
	DatabaseURL    string
	RedisAddr      string
	IdempotencyTTL time.Duration
	DBTimeout      time.Duration
}

// Load populates configuration from environment variables with sensible production defaults.
func Load() *Config {
	return &Config{
		GRPCAddr:       getEnv("GRPC_ADDR", ":50051"),
		MetricsAddr:    getEnv("METRICS_ADDR", ":2112"),
		DatabaseURL:    getEnv("DATABASE_URL", "postgres://resilient:resilient@localhost:5432/payments?sslmode=disable"),
		RedisAddr:      getEnv("REDIS_ADDR", "localhost:6379"),
		IdempotencyTTL: getEnvDuration("IDEMPOTENCY_TTL", 24*time.Hour),
		DBTimeout:      getEnvDuration("DB_TIMEOUT", 2*time.Second),
	}
}

func getEnv(key, defaultVal string) string {
	if val, ok := os.LookupEnv(key); ok && val != "" {
		return val
	}
	return defaultVal
}

func getEnvDuration(key string, defaultVal time.Duration) time.Duration {
	if val, ok := os.LookupEnv(key); ok && val != "" {
		if d, err := time.ParseDuration(val); err == nil {
			return d
		}
	}
	return defaultVal
}
