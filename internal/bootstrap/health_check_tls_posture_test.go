// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package bootstrap

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"
)

func TestHealthCheckTLSPosture_CategoriseProbeErrorTimeout(t *testing.T) {
	t.Parallel()

	assert.Equal(t, "timeout", categoriseProbeError(context.DeadlineExceeded))
}

func TestHealthCheckTLSPosture_RedisMalformedConfigReturnsUnknownReason(t *testing.T) {
	t.Parallel()

	cfg := &Config{Redis: RedisConfig{Host: "[::1", TLS: true}}

	deps := &HealthDependencies{
		RedisCheck:              func(context.Context) error { return nil },
		PostgresOptional:        true,
		PostgresReplicaOptional: true,
		RedisOptional:           true,
		RabbitMQOptional:        true,
		ObjectStorageOptional:   true,
	}

	_, checks, healthy := evaluateReadinessChecks(context.Background(), cfg, deps, &libLog.NopLogger{}, 0)

	assert.True(t, healthy)
	assert.Nil(t, checks["redis"].TLS)
	assert.Contains(t, checks["redis"].Reason, "TLS posture unknown")
}
