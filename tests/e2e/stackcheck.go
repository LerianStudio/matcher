//go:build e2e

package e2e

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"strings"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/redis/go-redis/v9"
)

// StackCheckResult holds the result of checking a single service.
type StackCheckResult struct {
	Service string
	OK      bool
	Error   error
	Latency time.Duration
}

// StackChecker verifies that all required services are running and healthy.
type StackChecker struct {
	cfg *E2EConfig
}

// NewStackChecker creates a new stack checker with the given configuration.
func NewStackChecker(cfg *E2EConfig) *StackChecker {
	return &StackChecker{cfg: cfg}
}

// CheckAll verifies all services are available. Returns error if any service is down.
func (sc *StackChecker) CheckAll(ctx context.Context) ([]StackCheckResult, error) {
	results := make([]StackCheckResult, 0, 4)
	var failures []string

	// Check App health endpoint
	appResult := sc.checkApp(ctx)
	results = append(results, appResult)
	if !appResult.OK {
		failures = append(failures, fmt.Sprintf("app: %v", appResult.Error))
	}

	// Check PostgreSQL
	pgResult := sc.checkPostgres(ctx)
	results = append(results, pgResult)
	if !pgResult.OK {
		failures = append(failures, fmt.Sprintf("postgres: %v", pgResult.Error))
	}

	// Check Redis
	redisResult := sc.checkRedis(ctx)
	results = append(results, redisResult)
	if !redisResult.OK {
		failures = append(failures, fmt.Sprintf("redis: %v", redisResult.Error))
	}

	// Check RabbitMQ
	rabbitResult := sc.checkRabbitMQ(ctx)
	results = append(results, rabbitResult)
	if !rabbitResult.OK {
		failures = append(failures, fmt.Sprintf("rabbitmq: %v", rabbitResult.Error))
	}

	if len(failures) > 0 {
		return results, fmt.Errorf("stack not ready: %s", strings.Join(failures, "; "))
	}

	return results, nil
}

func (sc *StackChecker) checkApp(ctx context.Context) StackCheckResult {
	result := StackCheckResult{Service: "app"}
	start := time.Now()

	client := &http.Client{Timeout: sc.cfg.StackCheckTimeout}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, sc.cfg.AppBaseURL+"/health", nil)
	if err != nil {
		result.Error = fmt.Errorf("failed to create request: %w", err)
		return result
	}

	resp, err := client.Do(req)
	if err != nil {
		result.Error = fmt.Errorf("health check failed: %w", err)
		return result
	}
	defer resp.Body.Close()

	result.Latency = time.Since(start)

	if resp.StatusCode != http.StatusOK {
		result.Error = fmt.Errorf("health check returned %d", resp.StatusCode)
		return result
	}

	result.OK = true
	return result
}

func (sc *StackChecker) checkPostgres(ctx context.Context) StackCheckResult {
	result := StackCheckResult{Service: "postgres"}
	start := time.Now()

	db, err := sql.Open("pgx", sc.cfg.PostgresDSN())
	if err != nil {
		result.Error = fmt.Errorf("failed to open connection: %w", err)
		return result
	}
	defer db.Close()

	if err := db.PingContext(ctx); err != nil {
		result.Error = fmt.Errorf("ping failed: %w", err)
		return result
	}

	result.Latency = time.Since(start)
	result.OK = true
	return result
}

func (sc *StackChecker) checkRedis(ctx context.Context) StackCheckResult {
	result := StackCheckResult{Service: "redis"}
	start := time.Now()

	client := sc.newRedisClient()
	defer client.Close()

	if err := client.Ping(ctx).Err(); err != nil {
		result.Error = fmt.Errorf("ping failed: %w", err)
		return result
	}

	result.Latency = time.Since(start)
	result.OK = true
	return result
}

func (sc *StackChecker) checkRabbitMQ(ctx context.Context) StackCheckResult {
	result := StackCheckResult{Service: "rabbitmq"}
	start := time.Now()

	conn, err := amqp.Dial(sc.cfg.RabbitMQURL())
	if err != nil {
		result.Error = fmt.Errorf("dial failed: %w", err)
		return result
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		result.Error = fmt.Errorf("channel open failed: %w", err)
		return result
	}
	defer ch.Close()

	result.Latency = time.Since(start)
	result.OK = true
	return result
}

func (sc *StackChecker) newRedisClient() *redis.Client {
	return redis.NewClient(&redis.Options{
		Addr: sc.cfg.RedisHost,
	})
}

// FlushRateLimitKeys removes all rate limit keys from Redis.
// This ensures e2e tests start with a clean rate limit state.
//
// Key layout: lib-commons' rate limiter stores counters under
// "{keyPrefix}:ratelimit:{tier}:{identity}" (see ratelimit.RateLimiter.buildKey).
// Matcher configures the prefix as "matcher" via NewLibRateLimiter, so the
// pattern that actually matches on a running stack is "matcher:ratelimit:*".
// We also pre-scan unprefixed "ratelimit:*" so this helper stays correct if
// a future refactor drops the prefix or if a test stack wires a different
// one; both branches are deletion-only, so the extra scan is safe.
func (sc *StackChecker) FlushRateLimitKeys(ctx context.Context) error {
	client := sc.newRedisClient()
	defer client.Close()

	const scanBatchSize = 100

	patterns := []string{
		"matcher:ratelimit:*",
		"ratelimit:*",
	}

	for _, pattern := range patterns {
		var cursor uint64

		for {
			keys, nextCursor, err := client.Scan(ctx, cursor, pattern, scanBatchSize).Result()
			if err != nil {
				return fmt.Errorf("redis scan %q: %w", pattern, err)
			}

			if len(keys) > 0 {
				if err := client.Del(ctx, keys...).Err(); err != nil {
					return fmt.Errorf("redis batch delete: %w", err)
				}
			}

			cursor = nextCursor
			if cursor == 0 {
				break
			}
		}
	}

	return nil
}

// CleanStaleOutboxEvents removes all outbox events from previous test runs.
// This prevents test failures caused by backlogged or stuck events blocking
// the dispatcher from processing new events in a timely manner.
func (sc *StackChecker) CleanStaleOutboxEvents(ctx context.Context) (int64, error) {
	db, err := sql.Open("pgx", sc.cfg.PostgresDSN())
	if err != nil {
		return 0, fmt.Errorf("open postgres: %w", err)
	}
	defer db.Close()

	result, err := db.ExecContext(ctx, `DELETE FROM outbox_events`)
	if err != nil {
		return 0, fmt.Errorf("delete outbox events: %w", err)
	}

	deleted, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("rows affected: %w", err)
	}

	return deleted, nil
}

// CleanStaleAuditState removes audit logs and chain state from previous test runs.
// The audit log uses a hash chain where each record references the previous record's hash.
// If audit_log_chain_state retains sequence counters from a prior run but the corresponding
// audit_logs rows are gone, the consumer fails with "previous record not found" when
// computing the hash chain, causing outbox events to be marked as failed.
func (sc *StackChecker) CleanStaleAuditState(ctx context.Context) error {
	db, err := sql.Open("pgx", sc.cfg.PostgresDSN())
	if err != nil {
		return fmt.Errorf("open postgres: %w", err)
	}
	defer db.Close()

	if _, err := db.ExecContext(ctx, `DELETE FROM audit_logs`); err != nil {
		return fmt.Errorf("delete audit logs: %w", err)
	}

	if _, err := db.ExecContext(ctx, `DELETE FROM audit_log_chain_state`); err != nil {
		return fmt.Errorf("delete audit log chain state: %w", err)
	}

	return nil
}

// FormatResults returns a human-readable summary of check results.
func FormatResults(results []StackCheckResult) string {
	var sb strings.Builder
	sb.WriteString("Stack Health Check:\n")
	for _, r := range results {
		status := "✓"
		if !r.OK {
			status = "✗"
		}
		sb.WriteString(fmt.Sprintf("  %s %s", status, r.Service))
		if r.OK {
			sb.WriteString(fmt.Sprintf(" (%v)\n", r.Latency.Round(time.Millisecond)))
		} else {
			sb.WriteString(fmt.Sprintf(": %v\n", r.Error))
		}
	}
	return sb.String()
}
