// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package bootstrap

import (
	"context"
	"testing"

	"github.com/gofiber/fiber/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"
)

// TestHealthCheckEvaluator_NilDependenciesFailClosed verifies the
// fail-closed behaviour across every dep. With nil deps, the evaluator cannot
// resolve any check and must aggregate down to unhealthy (503).
//
// Per-dep semantics when the dep has no resolved check:
//
//	required (postgres, redis, rabbitmq)        → status=down, error token
//	optional (postgres_replica, object_storage) → status=skipped, reason
//
// Redis is required per NewHealthDependencies (RedisOptional defaults false
// only when wired via createHealthDependencies; the raw struct default is
// true). This test uses the raw evaluator path — deps=nil — so every dep's
// spec.optional is computed against a nil HealthDependencies, which returns
// false. All 5 deps therefore fail as required.
func TestHealthCheckEvaluator_NilDependenciesFailClosed(t *testing.T) {
	t.Parallel()

	httpStatus, checks, healthy := evaluateReadinessChecks(
		context.Background(),
		nil,
		nil,
		&libLog.NopLogger{},
		0,
	)

	assert.Equal(t, fiber.StatusServiceUnavailable, httpStatus)
	assert.False(t, healthy)
	require.Len(t, checks, readyzDepCount)

	// Every dep must be present and in status=down. With nil deps, spec.optional
	// evaluates to false for all, so every missing check is a required failure.
	for _, name := range []string{"postgres", "postgres_replica", "redis", "rabbitmq", "object_storage"} {
		entry, ok := checks[name]
		require.True(t, ok, "missing check entry for %s", name)
		assert.Equal(t, checkStatusDown, entry.Status, "dep %s must be down", name)
		assert.Equal(t, "check not configured", entry.Error, "dep %s must carry the bounded error token", name)
	}
}

// TestHealthCheckEvaluator_OptionalUnresolvedReportsSkipped verifies that
// optional deps with no registered check surface as "skipped" with a
// human-readable reason, not as "down". This is the contract's optional-dep
// carve-out: optional deps must be honest about being unconfigured without
// poisoning the top-level aggregation.
func TestHealthCheckEvaluator_OptionalUnresolvedReportsSkipped(t *testing.T) {
	t.Parallel()

	// All optional; no checks registered.
	deps := &HealthDependencies{
		PostgresOptional:        true,
		PostgresReplicaOptional: true,
		RedisOptional:           true,
		RabbitMQOptional:        true,
		ObjectStorageOptional:   true,
	}

	httpStatus, checks, healthy := evaluateReadinessChecks(
		context.Background(),
		nil,
		deps,
		&libLog.NopLogger{},
		0,
	)

	assert.Equal(t, fiber.StatusOK, httpStatus, "all-optional + unresolved must aggregate healthy")
	assert.True(t, healthy)
	require.Len(t, checks, readyzDepCount)

	for _, name := range []string{"postgres", "postgres_replica", "redis", "rabbitmq", "object_storage"} {
		entry, ok := checks[name]
		require.True(t, ok, "missing check entry for %s", name)
		assert.Equal(t, checkStatusSkipped, entry.Status, "dep %s must be skipped", name)
		assert.Contains(t, entry.Reason, "not configured", "dep %s reason must explain why", name)
	}
}
