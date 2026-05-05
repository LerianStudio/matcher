// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package bootstrap

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	libRabbitmq "github.com/LerianStudio/lib-commons/v5/commons/rabbitmq"
	streaming "github.com/LerianStudio/lib-streaming/v2"
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
	"github.com/gofiber/fiber/v2"
	"github.com/stretchr/testify/assert"
)

func TestResolvePostgresCheck_NilDeps(t *testing.T) {
	fn, ok := resolvePostgresCheck(nil)
	assert.Nil(t, fn)
	assert.False(t, ok)
}

func TestResolvePostgresCheck_NilPostgresClient(t *testing.T) {
	deps := &HealthDependencies{}

	fn, ok := resolvePostgresCheck(deps)
	assert.Nil(t, fn)
	assert.False(t, ok)
}

func TestResolvePostgresCheck_CustomCheckUsed(t *testing.T) {
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
	fn, ok := resolvePostgresReplicaCheck(nil)
	assert.Nil(t, fn)
	assert.False(t, ok)
}

func TestResolvePostgresReplicaCheck_NilReplicaClient(t *testing.T) {
	deps := &HealthDependencies{}

	fn, ok := resolvePostgresReplicaCheck(deps)
	assert.Nil(t, fn)
	assert.False(t, ok)
}

func TestResolveRedisCheck_NilDeps(t *testing.T) {
	fn, ok := resolveRedisCheck(nil)
	assert.Nil(t, fn)
	assert.False(t, ok)
}

func TestResolveRedisCheck_NilRedisClient(t *testing.T) {
	deps := &HealthDependencies{}

	fn, ok := resolveRedisCheck(deps)
	assert.Nil(t, fn)
	assert.False(t, ok)
}

func TestResolveRedisCheck_CustomCheckUsed(t *testing.T) {
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
	fn, ok := resolveRabbitMQCheck(nil)
	assert.Nil(t, fn)
	assert.False(t, ok)
}

func TestResolveRabbitMQCheck_NilRabbitMQConn(t *testing.T) {
	deps := &HealthDependencies{}

	fn, ok := resolveRabbitMQCheck(deps)
	assert.Nil(t, fn)
	assert.False(t, ok)
}

func TestResolveRabbitMQCheck_CustomCheckUsed(t *testing.T) {
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

func TestResolveFetcherCheck_DisabledFetcherIsSkipped(t *testing.T) {
	deps := &HealthDependencies{
		FetcherCheck: func(_ context.Context) error { return nil },
	}

	fn, ok := resolveFetcherCheck(&Config{}, deps)
	assert.Nil(t, fn)
	assert.False(t, ok)
}

func TestResolveFetcherCheck_CustomCheckUsedWhenEnabled(t *testing.T) {
	deps := &HealthDependencies{
		FetcherCheck: func(_ context.Context) error { return nil },
	}

	fn, ok := resolveFetcherCheck(&Config{Fetcher: FetcherConfig{Enabled: true, URL: "https://fetcher.internal"}}, deps)
	assert.NotNil(t, fn)
	assert.True(t, ok)
	assert.NoError(t, fn(context.Background()))
}

func TestResolveFetcherCheck_EnabledFetcherWithoutURLFails(t *testing.T) {
	deps := &HealthDependencies{
		FetcherCheck: func(_ context.Context) error { return nil },
	}

	fn, ok := resolveFetcherCheck(&Config{Fetcher: FetcherConfig{Enabled: true}}, deps)
	assert.NotNil(t, fn)
	assert.True(t, ok)
	assert.ErrorIs(t, fn(context.Background()), errFetcherURLRequired)
}

func TestResolveFetcherCheck_UsesFetcherClientHealth(t *testing.T) {
	deps := &HealthDependencies{
		Fetcher: stubFetcherClient{healthy: true},
	}

	fn, ok := resolveFetcherCheck(&Config{Fetcher: FetcherConfig{Enabled: true, URL: "https://fetcher.internal"}}, deps)
	assert.NotNil(t, fn)
	assert.True(t, ok)
	assert.NoError(t, fn(context.Background()))

	deps.Fetcher = stubFetcherClient{healthy: false}
	fn, ok = resolveFetcherCheck(&Config{Fetcher: FetcherConfig{Enabled: true, URL: "https://fetcher.internal"}}, deps)
	assert.NotNil(t, fn)
	assert.True(t, ok)
	assert.ErrorIs(t, fn(context.Background()), sharedPorts.ErrFetcherUnavailable)
}

func TestResolveFetcherCheck_TypedNilFetcherClientIsUnavailable(t *testing.T) {
	var fetcher *nilFetcherClient
	deps := &HealthDependencies{Fetcher: fetcher}

	fn, ok := resolveFetcherCheck(&Config{Fetcher: FetcherConfig{Enabled: true, URL: "https://fetcher.internal"}}, deps)
	assert.Nil(t, fn)
	assert.False(t, ok)
}

func TestResolveObjectStorageCheck_NilDeps(t *testing.T) {
	fn, ok := resolveObjectStorageCheck(nil)
	assert.Nil(t, fn)
	assert.False(t, ok)
}

type stubFetcherClient struct {
	healthy bool
}

type nilFetcherClient struct{}

func (client stubFetcherClient) IsHealthy(context.Context) bool { return client.healthy }

func (client stubFetcherClient) ListConnections(context.Context, string) ([]*sharedPorts.FetcherConnection, error) {
	return nil, nil
}

func (client stubFetcherClient) GetSchema(context.Context, string) (*sharedPorts.FetcherSchema, error) {
	return nil, nil
}

func (client stubFetcherClient) TestConnection(context.Context, string) (*sharedPorts.FetcherTestResult, error) {
	return nil, nil
}

func (client stubFetcherClient) SubmitExtractionJob(context.Context, sharedPorts.ExtractionJobInput) (string, error) {
	return "", nil
}

func (client stubFetcherClient) GetExtractionJobStatus(context.Context, string) (*sharedPorts.ExtractionJobStatus, error) {
	return nil, nil
}

func (client *nilFetcherClient) IsHealthy(context.Context) bool {
	panic("typed nil fetcher client used")
}

func (client *nilFetcherClient) ListConnections(context.Context, string) ([]*sharedPorts.FetcherConnection, error) {
	return nil, nil
}

func (client *nilFetcherClient) GetSchema(context.Context, string) (*sharedPorts.FetcherSchema, error) {
	return nil, nil
}

func (client *nilFetcherClient) TestConnection(context.Context, string) (*sharedPorts.FetcherTestResult, error) {
	return nil, nil
}

func (client *nilFetcherClient) SubmitExtractionJob(context.Context, sharedPorts.ExtractionJobInput) (string, error) {
	return "", nil
}

func (client *nilFetcherClient) GetExtractionJobStatus(context.Context, string) (*sharedPorts.ExtractionJobStatus, error) {
	return nil, nil
}

func TestResolveObjectStorageCheck_NilStorage(t *testing.T) {
	deps := &HealthDependencies{}

	fn, ok := resolveObjectStorageCheck(deps)
	assert.Nil(t, fn)
	assert.False(t, ok)
}

type typedNilObjectStorage struct{}

func (*typedNilObjectStorage) Exists(context.Context, string) (bool, error) {
	panic("typed nil object storage used")
}

func TestResolveObjectStorageCheck_TypedNilStorageIsUnavailable(t *testing.T) {
	var storage *typedNilObjectStorage
	deps := &HealthDependencies{ObjectStorage: storage}

	fn, ok := resolveObjectStorageCheck(deps)

	assert.Nil(t, fn)
	assert.False(t, ok)
}

func TestResolveObjectStorageCheck_CustomCheckUsed(t *testing.T) {
	deps := &HealthDependencies{
		ObjectStorageCheck: func(_ context.Context) error {
			return nil
		},
	}

	fn, ok := resolveObjectStorageCheck(deps)
	assert.NotNil(t, fn)
	assert.True(t, ok)
}

func TestResolveStreamingCheck_DisabledStreamingIsSkipped(t *testing.T) {
	deps := &HealthDependencies{
		StreamingCheck: func(_ context.Context) error { return nil },
	}

	fn, ok := resolveStreamingCheck(deps)
	assert.Nil(t, fn)
	assert.False(t, ok)
}

func TestResolveStreamingCheck_CustomCheckUsedWhenEnabled(t *testing.T) {
	deps := &HealthDependencies{
		StreamingEnabled: true,
		StreamingCheck:   func(_ context.Context) error { return nil },
	}

	fn, ok := resolveStreamingCheck(deps)
	assert.NotNil(t, fn)
	assert.True(t, ok)
	assert.NoError(t, fn(context.Background()))
}

func TestResolveStreamingCheck_DegradedHealthIsReadinessSafe(t *testing.T) {
	deps := &HealthDependencies{
		StreamingEnabled: true,
		StreamingCheck: func(context.Context) error {
			return streaming.NewHealthError(streaming.Degraded, errors.New("broker unavailable with outbox fallback"))
		},
	}

	fn, ok := resolveStreamingCheck(deps)

	assert.NotNil(t, fn)
	assert.True(t, ok)
	assert.NoError(t, fn(context.Background()))
}

func TestResolveStreamingCheck_TypedNilEmitterIsSkipped(t *testing.T) {
	var emitter *typedNilStreamingEmitter
	deps := &HealthDependencies{
		StreamingEnabled: true,
		Streaming:        emitter,
	}

	fn, ok := resolveStreamingCheck(deps)

	assert.Nil(t, fn)
	assert.False(t, ok)
}

type typedNilStreamingEmitter struct{}

func (*typedNilStreamingEmitter) Emit(context.Context, streaming.EmitRequest) error { return nil }

func (*typedNilStreamingEmitter) Close() error { return nil }

func (*typedNilStreamingEmitter) Healthy(context.Context) error { return nil }

func TestEvaluateReadinessChecks_DisabledStreamingSkippedHealthy(t *testing.T) {
	status, checks, healthy := evaluateReadinessChecks(context.Background(), &Config{}, &HealthDependencies{
		PostgresOptional:        true,
		PostgresReplicaOptional: true,
		RedisOptional:           true,
		RabbitMQOptional:        true,
		ObjectStorageOptional:   true,
	}, &testLogger{}, 0)

	assert.Equal(t, fiber.StatusOK, status)
	assert.True(t, healthy)
	assert.Equal(t, checkStatusSkipped, checks["streaming"].Status)
}

func TestEvaluateReadinessChecks_EnabledStreamingFailureIsRequired(t *testing.T) {
	streamingErr := errors.New("streaming broker unavailable")
	status, checks, healthy := evaluateReadinessChecks(context.Background(), &Config{}, &HealthDependencies{
		PostgresOptional:        true,
		PostgresReplicaOptional: true,
		RedisOptional:           true,
		RabbitMQOptional:        true,
		ObjectStorageOptional:   true,
		StreamingEnabled:        true,
		StreamingCheck:          func(context.Context) error { return streamingErr },
	}, &testLogger{}, 0)

	assert.Equal(t, fiber.StatusServiceUnavailable, status)
	assert.False(t, healthy)
	assert.Equal(t, checkStatusDown, checks["streaming"].Status)
}

func TestEvaluateReadinessChecks_ReturnsWhenProbeIgnoresContext(t *testing.T) {
	block := make(chan struct{})
	defer close(block)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	startedAt := time.Now()
	status, checks, healthy := evaluateReadinessChecks(ctx, &Config{}, &HealthDependencies{
		PostgresOptional:        true,
		PostgresReplicaOptional: true,
		RedisCheck: func(context.Context) error {
			<-block
			return nil
		},
		RabbitMQOptional:      true,
		ObjectStorageOptional: true,
	}, &testLogger{}, 0)

	assert.Less(t, time.Since(startedAt), 200*time.Millisecond)
	assert.Equal(t, fiber.StatusServiceUnavailable, status)
	assert.False(t, healthy)
	assert.Equal(t, checkStatusDown, checks["redis"].Status)
}
