// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package bootstrap

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	libRabbitmq "github.com/LerianStudio/lib-commons/v5/commons/rabbitmq"
	"github.com/stretchr/testify/assert"
)

func TestResolvePostgresCheck_NilDeps(t *testing.T) {
	t.Parallel()

	fn, ok := resolvePostgresCheck(nil)
	assert.Nil(t, fn)
	assert.False(t, ok)
}

func TestResolvePostgresCheck_NilPostgresClient(t *testing.T) {
	t.Parallel()

	deps := &HealthDependencies{}

	fn, ok := resolvePostgresCheck(deps)
	assert.Nil(t, fn)
	assert.False(t, ok)
}

func TestResolvePostgresCheck_CustomCheckUsed(t *testing.T) {
	t.Parallel()

	deps := &HealthDependencies{
		PostgresCheck: func(_ context.Context) error {
			return nil
		},
	}

	fn, ok := resolvePostgresCheck(deps)
	assert.NotNil(t, fn)
	assert.True(t, ok)
}

func TestResolvePostgresReplicaCheck_NilDeps(t *testing.T) {
	t.Parallel()

	fn, ok := resolvePostgresReplicaCheck(nil)
	assert.Nil(t, fn)
	assert.False(t, ok)
}

func TestResolvePostgresReplicaCheck_NilReplicaClient(t *testing.T) {
	t.Parallel()

	deps := &HealthDependencies{}

	fn, ok := resolvePostgresReplicaCheck(deps)
	assert.Nil(t, fn)
	assert.False(t, ok)
}

func TestResolveRedisCheck_NilDeps(t *testing.T) {
	t.Parallel()

	fn, ok := resolveRedisCheck(nil)
	assert.Nil(t, fn)
	assert.False(t, ok)
}

func TestResolveRedisCheck_NilRedisClient(t *testing.T) {
	t.Parallel()

	deps := &HealthDependencies{}

	fn, ok := resolveRedisCheck(deps)
	assert.Nil(t, fn)
	assert.False(t, ok)
}

func TestResolveRedisCheck_CustomCheckUsed(t *testing.T) {
	t.Parallel()

	deps := &HealthDependencies{
		RedisCheck: func(_ context.Context) error {
			return nil
		},
	}

	fn, ok := resolveRedisCheck(deps)
	assert.NotNil(t, fn)
	assert.True(t, ok)
}

func TestResolveRabbitMQCheck_NilDeps(t *testing.T) {
	t.Parallel()

	fn, ok := resolveRabbitMQCheck(nil)
	assert.Nil(t, fn)
	assert.False(t, ok)
}

func TestResolveRabbitMQCheck_NilRabbitMQConn(t *testing.T) {
	t.Parallel()

	deps := &HealthDependencies{}

	fn, ok := resolveRabbitMQCheck(deps)
	assert.Nil(t, fn)
	assert.False(t, ok)
}

func TestResolveRabbitMQCheck_CustomCheckUsed(t *testing.T) {
	t.Parallel()

	deps := &HealthDependencies{
		RabbitMQCheck: func(_ context.Context) error {
			return nil
		},
	}

	fn, ok := resolveRabbitMQCheck(deps)
	assert.NotNil(t, fn)
	assert.True(t, ok)
}

func TestResolveRabbitMQCheck_SkipsInsecureHTTPProbeWhenPolicyDisallowsIt(t *testing.T) {
	t.Parallel()

	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	deps := &HealthDependencies{
		RabbitMQ: &libRabbitmq.RabbitMQConnection{
			HealthCheckURL:           server.URL,
			AllowInsecureHealthCheck: false,
		},
	}

	fn, ok := resolveRabbitMQCheck(deps)
	assert.True(t, ok)
	assert.NotNil(t, fn)
	assert.Error(t, fn(context.Background()))
	assert.Equal(t, int32(0), requests.Load())
}

func TestResolveRabbitMQCheck_UsesHTTPProbeWhenAllowed(t *testing.T) {
	t.Parallel()

	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	deps := &HealthDependencies{
		RabbitMQ: &libRabbitmq.RabbitMQConnection{
			HealthCheckURL:           server.URL,
			AllowInsecureHealthCheck: true,
		},
	}

	fn, ok := resolveRabbitMQCheck(deps)
	assert.True(t, ok)
	assert.NotNil(t, fn)
	assert.NoError(t, fn(context.Background()))
	assert.Equal(t, int32(1), requests.Load())
}

func TestResolveObjectStorageCheck_NilDeps(t *testing.T) {
	t.Parallel()

	fn, ok := resolveObjectStorageCheck(nil)
	assert.Nil(t, fn)
	assert.False(t, ok)
}

func TestResolveObjectStorageCheck_NilStorage(t *testing.T) {
	t.Parallel()

	deps := &HealthDependencies{}

	fn, ok := resolveObjectStorageCheck(deps)
	assert.Nil(t, fn)
	assert.False(t, ok)
}

func TestResolveObjectStorageCheck_CustomCheckUsed(t *testing.T) {
	t.Parallel()

	deps := &HealthDependencies{
		ObjectStorageCheck: func(_ context.Context) error {
			return nil
		},
	}

	fn, ok := resolveObjectStorageCheck(deps)
	assert.NotNil(t, fn)
	assert.True(t, ok)
}
