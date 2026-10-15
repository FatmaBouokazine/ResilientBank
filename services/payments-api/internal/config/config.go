package config

import (
	"os"
	"strconv"
	"strings"
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

	// Kafka settings for the outbox relayer and its producer.
	Kafka KafkaConfig
}

// KafkaConfig holds everything needed to publish outbox events.
//
// Brokers defaults to the host-facing listener. A containerised client must instead use
// the broker's INTERNAL advertised address (kafka:29092), because the EXTERNAL listener
// advertises localhost, which from inside another container resolves to that container.
type KafkaConfig struct {
	// Enabled gates all Kafka publishing. When false the relayer still claims and marks
	// rows, so a broker outage or a deliberately disabled stack never blocks payments:
	// events accumulate in the outbox and drain when Kafka returns.
	Enabled bool

	Brokers []string

	// ClientID identifies this producer in broker logs and metrics.
	ClientID string

	// TopicEvents receives payment events. Must be keyed by payment id so all events for
	// one payment retain their relative order.
	TopicEvents string

	// TopicDLQ receives events that cannot be published and must not be retried.
	TopicDLQ string

	// PublishTimeout bounds a single produce call, including broker delivery
	// acknowledgement.
	PublishTimeout time.Duration

	// Outbox polling.
	PollInterval  time.Duration
	PollBatchSize int

	// Retry policy for transient publish failures.
	MaxAttempts int
	BackoffBase time.Duration
	BackoffMax  time.Duration
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
		Kafka:          loadKafka(),
	}
}

func loadKafka() KafkaConfig {
	return KafkaConfig{
		// Defaults to ON. Operators disable it explicitly to run without a broker.
		Enabled:  getEnvBool("KAFKA_ENABLED", true),
		Brokers:  getEnvList("KAFKA_BROKERS", []string{"localhost:9092"}),
		ClientID: getEnv("KAFKA_CLIENT_ID", "resilientbank-outbox-relayer"),

		TopicEvents: getEnv("KAFKA_TOPIC_EVENTS", "payments.events.v1"),
		TopicDLQ:    getEnv("KAFKA_TOPIC_DLQ", "payments.events.v1.dlq"),

		PublishTimeout: getEnvDuration("KAFKA_PUBLISH_TIMEOUT", 10*time.Second),

		PollInterval:  getEnvDuration("OUTBOX_POLL_INTERVAL", 500*time.Millisecond),
		PollBatchSize: getEnvInt("OUTBOX_BATCH_SIZE", 100),

		MaxAttempts: getEnvInt("OUTBOX_MAX_ATTEMPTS", 10),
		BackoffBase: getEnvDuration("OUTBOX_BACKOFF_BASE", time.Second),
		BackoffMax:  getEnvDuration("OUTBOX_BACKOFF_MAX", 5*time.Minute),
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

func getEnvInt(key string, defaultVal int) int {
	if val, ok := os.LookupEnv(key); ok && val != "" {
		if i, err := strconv.Atoi(strings.TrimSpace(val)); err == nil {
			return i
		}
	}
	return defaultVal
}

func getEnvBool(key string, defaultVal bool) bool {
	if val, ok := os.LookupEnv(key); ok && val != "" {
		if b, err := strconv.ParseBool(strings.TrimSpace(val)); err == nil {
			return b
		}
	}
	return defaultVal
}

// getEnvList parses a comma-separated environment variable.
func getEnvList(key string, defaultVal []string) []string {
	if val, ok := os.LookupEnv(key); ok && val != "" {
		parts := strings.Split(val, ",")
		out := make([]string, 0, len(parts))
		for _, p := range parts {
			if trimmed := strings.TrimSpace(p); trimmed != "" {
				out = append(out, trimmed)
			}
		}
		if len(out) > 0 {
			return out
		}
	}
	return defaultVal
}
