// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package bootstrap

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"time"
)

func resolvePostgresCheck(deps *HealthDependencies) (HealthCheckFunc, bool) {
	if deps == nil {
		return nil, false
	}

	if deps.PostgresCheck != nil {
		return deps.PostgresCheck, true
	}

	if deps.Postgres == nil {
		return nil, false
	}

	return func(ctx context.Context) error {
		db, err := deps.Postgres.Primary()
		if err != nil {
			return fmt.Errorf("postgres health check: get primary db failed: %w", err)
		}

		if db == nil {
			return errPostgresPrimaryNil
		}

		if err := db.PingContext(ctx); err != nil {
			return fmt.Errorf("postgres health check: ping failed: %w", err)
		}

		return nil
	}, true
}

func resolvePostgresReplicaCheck(deps *HealthDependencies) (HealthCheckFunc, bool) {
	if deps == nil {
		return nil, false
	}

	if deps.PostgresReplicaCheck != nil {
		return deps.PostgresReplicaCheck, true
	}

	if deps.PostgresReplica == nil {
		return nil, false
	}

	return func(ctx context.Context) error {
		resolver, err := deps.PostgresReplica.Resolver(ctx)
		if err != nil {
			return fmt.Errorf("postgres replica health check: get resolver failed: %w", err)
		}

		if resolver == nil {
			return errReplicaResolverNil
		}

		replicas := resolver.ReplicaDBs()
		if len(replicas) == 0 {
			return errNoReplicasConfigured
		}

		// Probe ONLY the first non-nil replica. Iterating every replica under
		// a single per-check timeout means one hanging replica burns the whole
		// budget before reaching the next. Since callers get a load-balanced
		// view via the resolver, checking one replica is representative — a
		// degraded replica pool surfaces as probe failures over subsequent
		// /readyz hits as the resolver rotates connections.
		for _, replica := range replicas {
			if replica == nil {
				continue
			}

			if err := replica.PingContext(ctx); err != nil {
				return fmt.Errorf("postgres replica health check: ping replica failed: %w", err)
			}

			return nil
		}

		return errNoNonNilReplicas
	}, true
}

func resolveRedisCheck(deps *HealthDependencies) (HealthCheckFunc, bool) {
	if deps == nil {
		return nil, false
	}

	if deps.RedisCheck != nil {
		return deps.RedisCheck, true
	}

	if deps.Redis == nil {
		return nil, false
	}

	return func(ctx context.Context) error {
		client, err := deps.Redis.GetClient(ctx)
		if err != nil {
			return fmt.Errorf("redis health check: get client failed: %w", err)
		}

		if client == nil {
			return errRedisClientNil
		}

		if err := client.Ping(ctx).Err(); err != nil {
			return fmt.Errorf("redis health check: ping failed: %w", err)
		}

		return nil
	}, true
}

func resolveRabbitMQCheck(deps *HealthDependencies) (HealthCheckFunc, bool) {
	if deps == nil {
		return nil, false
	}

	if deps.RabbitMQCheck != nil {
		return deps.RabbitMQCheck, true
	}

	if deps.RabbitMQ == nil {
		return nil, false
	}

	return func(ctx context.Context) error {
		if deps.RabbitMQ.HealthCheckURL != "" &&
			(deps.RabbitMQ.AllowInsecureHealthCheck || !isInsecureHTTPHealthCheckURL(deps.RabbitMQ.HealthCheckURL)) {
			if err := checkRabbitMQHTTPHealth(ctx, deps.RabbitMQ.HealthCheckURL); err == nil {
				return nil
			}
		}

		return deps.RabbitMQ.EnsureChannel()
	}, true
}

func resolveObjectStorageCheck(deps *HealthDependencies) (HealthCheckFunc, bool) {
	if deps == nil {
		return nil, false
	}

	if deps.ObjectStorageCheck != nil {
		return deps.ObjectStorageCheck, true
	}

	if deps.ObjectStorage == nil {
		return nil, false
	}

	return func(ctx context.Context) error {
		// We just check that we can reach the storage by checking for a non-existent key.
		// The Exists call will return false if the key doesn't exist (expected),
		// but will error if the storage is unreachable.
		_, err := deps.ObjectStorage.Exists(ctx, ".health-check")
		if err != nil {
			return fmt.Errorf("object storage health check: %w", err)
		}

		return nil
	}, true
}

// rabbitMQHTTPClientTimeout is an outer belt-and-suspenders cap on the
// shared RabbitMQ health-check client. Per-request deadlines still flow via
// http.NewRequestWithContext; this only fires if a bug drops the request ctx.
const rabbitMQHTTPClientTimeout = 10 * time.Second

// rabbitMQHTTPClient is a reusable HTTP client for RabbitMQ health checks.
// http.Client is safe for concurrent use, so a single package-level instance
// avoids per-call allocations and connection pool churn. Per-check timeouts
// are enforced by the request context from applyReadinessCheckResult; the
// client-level Timeout is belt-and-suspenders for bugs that lose the request
// ctx.
var rabbitMQHTTPClient = &http.Client{Timeout: rabbitMQHTTPClientTimeout}

func checkRabbitMQHTTPHealth(ctx context.Context, healthURL string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, healthURL, http.NoBody)
	if err != nil {
		return fmt.Errorf("rabbitmq health check: create request: %w", err)
	}

	resp, err := rabbitMQHTTPClient.Do(req) // #nosec G704 -- internal RabbitMQ health check, URL is from trusted application config
	if err != nil {
		return fmt.Errorf("rabbitmq health check: request failed: %w", err)
	}

	defer func() {
		// Drain before close so the underlying TCP connection can be reused by
		// the keep-alive pool.
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("%w: %d", errRabbitMQUnhealthy, resp.StatusCode)
	}

	return nil
}
