package payments

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	paymentsv1 "github.com/YOUR_GITHUB_USER/resilientbank/services/payments-api/gen/payments/v1"
	"github.com/YOUR_GITHUB_USER/resilientbank/services/payments-api/internal/config"
	"github.com/YOUR_GITHUB_USER/resilientbank/services/payments-api/internal/metrics"
)

// IdempotencyRecord represents the serialized payload cached in Redis.
type IdempotencyRecord struct {
	PaymentID   string `json:"payment_id"`
	RequestHash string `json:"request_hash"`
}

// Service implements paymentsv1.PaymentServiceServer.
type Service struct {
	paymentsv1.UnimplementedPaymentServiceServer
	db     *pgxpool.Pool
	redis  *redis.Client
	cfg    *config.Config
	logger *slog.Logger
}

// NewService constructs a new payment service handler.
func NewService(db *pgxpool.Pool, redisClient *redis.Client, cfg *config.Config, logger *slog.Logger) *Service {
	return &Service{
		db:     db,
		redis:  redisClient,
		cfg:    cfg,
		logger: logger,
	}
}

// CreatePayment executes payment ingestion with two-layer idempotency.
//
// WHY TWO LAYERS:
// 1. Redis is the fast path (performance layer): It absorbs duplicate requests in
//    sub-millisecond time directly from memory, preventing unnecessary database load
//    during rapid client retries or network rebroadcasts.
// 2. PostgreSQL UNIQUE constraint is the real guarantee (durability/correctness layer):
//    If Redis is down, partitioned, degraded, restarting, or has evicted keys, or if two
//    identical requests arrive concurrently at the exact same millisecond, the PostgreSQL
//    UNIQUE constraint on idempotency_key prevents double-inserts and ensures ACID consistency.
//    A slow or unavailable Redis must never block payment processing.
func (s *Service) CreatePayment(ctx context.Context, req *paymentsv1.CreatePaymentRequest) (*paymentsv1.CreatePaymentResponse, error) {
	start := time.Now()
	defer func() {
		metrics.PaymentDuration.Observe(time.Since(start).Seconds())
	}()

	// 1. Validate request
	if err := ValidateCreatePaymentRequest(req); err != nil {
		metrics.PaymentsTotal.WithLabelValues("invalid").Inc()
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	// 2. Compute request fingerprint
	requestHash := ComputeRequestHash(req.FromAccount, req.ToAccount, req.AmountCents, req.Currency)
	redisKey := fmt.Sprintf("idem:%s", req.IdempotencyKey)

	// 3. Redis fast path
	val, err := s.redis.Get(ctx, redisKey).Result()
	if err == nil {
		var record IdempotencyRecord
		if unmarshalErr := json.Unmarshal([]byte(val), &record); unmarshalErr == nil {
			if record.RequestHash == requestHash {
				// Cache hit with matching hash: duplicate request
				metrics.PaymentsTotal.WithLabelValues("duplicate").Inc()
				return &paymentsv1.CreatePaymentResponse{
					PaymentId: record.PaymentID,
					Status:    "ACCEPTED",
				}, nil
			}
			// Cache hit with different hash: key reuse conflict
			metrics.PaymentsTotal.WithLabelValues("conflict").Inc()
			return nil, status.Error(codes.AlreadyExists, "idempotency key reused with different parameters")
		}
	} else if !errors.Is(err, redis.Nil) {
		// ANY Redis error (including network drop/timeout) must NOT fail the request.
		// Log warning, increment dependency error metric, and proceed to PostgreSQL.
		s.logger.WarnContext(ctx, "redis cache lookup failed; continuing to postgres",
			"idempotency_key", req.IdempotencyKey,
			"error", err,
		)
		metrics.DependencyErrorsTotal.WithLabelValues("redis").Inc()
	}

	// 4. Insert into PostgreSQL with per-request DB_TIMEOUT
	dbCtx, cancel := context.WithTimeout(ctx, s.cfg.DBTimeout)
	defer cancel()

	newID := uuid.New().String()
	insertQuery := `
		INSERT INTO payments (id, idempotency_key, request_hash, from_account, to_account, amount_cents, currency, status, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, now())
		ON CONFLICT (idempotency_key) DO NOTHING
	`

	tag, err := s.db.Exec(dbCtx, insertQuery,
		newID, req.IdempotencyKey, requestHash, req.FromAccount, req.ToAccount, req.AmountCents, req.Currency, "ACCEPTED",
	)
	if err != nil {
		s.logger.ErrorContext(ctx, "postgres insert failed",
			"idempotency_key", req.IdempotencyKey,
			"error", err,
		)
		metrics.DependencyErrorsTotal.WithLabelValues("postgres").Inc()
		metrics.PaymentsTotal.WithLabelValues("error").Inc()
		return nil, status.Error(codes.Unavailable, "database unavailable")
	}

	var finalPaymentID string
	if tag.RowsAffected() == 1 {
		// Freshly created payment
		finalPaymentID = newID
		metrics.PaymentsTotal.WithLabelValues("created").Inc()
	} else {
		// 0 rows affected: conflict on idempotency_key, inspect stored record
		selectQuery := `SELECT id, request_hash FROM payments WHERE idempotency_key = $1`
		var existingID, existingHash string
		if queryErr := s.db.QueryRow(dbCtx, selectQuery, req.IdempotencyKey).Scan(&existingID, &existingHash); queryErr != nil {
			s.logger.ErrorContext(ctx, "postgres existing record query failed",
				"idempotency_key", req.IdempotencyKey,
				"error", queryErr,
			)
			metrics.DependencyErrorsTotal.WithLabelValues("postgres").Inc()
			metrics.PaymentsTotal.WithLabelValues("error").Inc()
			return nil, status.Error(codes.Unavailable, "database unavailable")
		}

		if existingHash == requestHash {
			// Matching parameters: duplicate
			finalPaymentID = existingID
			metrics.PaymentsTotal.WithLabelValues("duplicate").Inc()
		} else {
			// Different parameters: conflict
			metrics.PaymentsTotal.WithLabelValues("conflict").Inc()
			return nil, status.Error(codes.AlreadyExists, "idempotency key reused with different parameters")
		}
	}

	// 5. Write-through cache to Redis
	recordBytes, err := json.Marshal(IdempotencyRecord{
		PaymentID:   finalPaymentID,
		RequestHash: requestHash,
	})
	if err == nil {
		if cacheErr := s.redis.Set(ctx, redisKey, recordBytes, s.cfg.IdempotencyTTL).Err(); cacheErr != nil {
			s.logger.WarnContext(ctx, "failed to update redis cache; continuing",
				"idempotency_key", req.IdempotencyKey,
				"error", cacheErr,
			)
			metrics.DependencyErrorsTotal.WithLabelValues("redis").Inc()
		}
	}

	// 6. Return response
	return &paymentsv1.CreatePaymentResponse{
		PaymentId: finalPaymentID,
		Status:    "ACCEPTED",
	}, nil
}

// GetPayment retrieves a payment record by its unique ID.
func (s *Service) GetPayment(ctx context.Context, req *paymentsv1.GetPaymentRequest) (*paymentsv1.GetPaymentResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request cannot be nil")
	}

	// Validate UUID format
	if _, err := uuid.Parse(req.PaymentId); err != nil {
		return nil, status.Error(codes.InvalidArgument, "malformed payment_id: must be a valid UUID")
	}

	dbCtx, cancel := context.WithTimeout(ctx, s.cfg.DBTimeout)
	defer cancel()

	query := `
		SELECT id, from_account, to_account, amount_cents, currency, status, created_at
		FROM payments
		WHERE id = $1
	`

	var (
		id, fromAccount, toAccount, currency, statusStr string
		amountCents                                      int64
		createdAt                                       time.Time
	)

	err := s.db.QueryRow(dbCtx, query, req.PaymentId).Scan(
		&id, &fromAccount, &toAccount, &amountCents, &currency, &statusStr, &createdAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, status.Error(codes.NotFound, "payment not found")
		}
		s.logger.ErrorContext(ctx, "postgres get payment failed",
			"payment_id", req.PaymentId,
			"error", err,
		)
		metrics.DependencyErrorsTotal.WithLabelValues("postgres").Inc()
		return nil, status.Error(codes.Unavailable, "database unavailable")
	}

	return &paymentsv1.GetPaymentResponse{
		PaymentId:   id,
		Status:      statusStr,
		FromAccount: fromAccount,
		ToAccount:   toAccount,
		AmountCents: amountCents,
		Currency:    currency,
		CreatedAt:   timestamppb.New(createdAt),
	}, nil
}
