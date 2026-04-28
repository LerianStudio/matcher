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
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
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

func TestResolveFetcherCheck_DisabledFetcherIsSkipped(t *testing.T) {
	t.Parallel()

	deps := &HealthDependencies{
		FetcherCheck: func(_ context.Context) error { return nil },
	}

	fn, ok := resolveFetcherCheck(&Config{}, deps)
	assert.Nil(t, fn)
	assert.False(t, ok)
}

func TestResolveFetcherCheck_CustomCheckUsedWhenEnabled(t *testing.T) {
	t.Parallel()

	deps := &HealthDependencies{
		FetcherCheck: func(_ context.Context) error { return nil },
	}

	fn, ok := resolveFetcherCheck(&Config{Fetcher: FetcherConfig{Enabled: true, URL: "https://fetcher.internal"}}, deps)
	assert.NotNil(t, fn)
	assert.True(t, ok)
	assert.NoError(t, fn(context.Background()))
}

func TestResolveFetcherCheck_EnabledFetcherWithoutURLFails(t *testing.T) {
	t.Parallel()

	deps := &HealthDependencies{
		FetcherCheck: func(_ context.Context) error { return nil },
	}

	fn, ok := resolveFetcherCheck(&Config{Fetcher: FetcherConfig{Enabled: true}}, deps)
	assert.NotNil(t, fn)
	assert.True(t, ok)
	assert.ErrorIs(t, fn(context.Background()), errFetcherURLRequired)
}

func TestResolveFetcherCheck_UsesFetcherClientHealth(t *testing.T) {
	t.Parallel()

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
	t.Parallel()

	var fetcher *nilFetcherClient
	deps := &HealthDependencies{Fetcher: fetcher}

	fn, ok := resolveFetcherCheck(&Config{Fetcher: FetcherConfig{Enabled: true, URL: "https://fetcher.internal"}}, deps)
	assert.Nil(t, fn)
	assert.False(t, ok)
}

func TestResolveObjectStorageCheck_NilDeps(t *testing.T) {
	t.Parallel()

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
