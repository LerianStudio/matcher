// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package bootstrap

import (
	"context"
	"fmt"
	"sync"

	"github.com/LerianStudio/lib-commons/v4/commons/circuitbreaker"
	libLog "github.com/LerianStudio/lib-commons/v4/commons/log"

	discoveryFetcher "github.com/LerianStudio/matcher/internal/discovery/adapters/fetcher"
	discoveryPorts "github.com/LerianStudio/matcher/internal/discovery/ports"
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

type dynamicFetcherClient struct {
	initialCfg     *Config
	configGetter   func() *Config
	mu             sync.Mutex
	activeKey      string
	activeClient   sharedPorts.FetcherClient
	breaker        circuitbreaker.Manager
	m2mProvider    sharedPorts.M2MProvider       // nil in single-tenant mode
	tokenExchanger discoveryPorts.TokenExchanger // nil in single-tenant mode
}

func newDynamicFetcherClient(initialCfg *Config, configGetter func() *Config, logger libLog.Logger, m2mProvider ...sharedPorts.M2MProvider) sharedPorts.FetcherClient {
	var breaker circuitbreaker.Manager

	if mgr, err := circuitbreaker.NewManager(logger); err == nil {
		breaker = mgr
	}

	client := &dynamicFetcherClient{
		initialCfg:   initialCfg,
		configGetter: configGetter,
		breaker:      breaker,
	}

	if len(m2mProvider) > 0 && m2mProvider[0] != nil {
		client.m2mProvider = m2mProvider[0]
	}

	return client
}

// IsHealthy reports whether the active Fetcher client is healthy.
func (client *dynamicFetcherClient) IsHealthy(ctx context.Context) bool {
	delegate, err := client.current()
	if err != nil {
		return false
	}

	return delegate.IsHealthy(ctx)
}

// ListConnections delegates connection listing to the active Fetcher client.
func (client *dynamicFetcherClient) ListConnections(ctx context.Context, productName string) ([]*sharedPorts.FetcherConnection, error) {
	delegate, err := client.current()
	if err != nil {
		return nil, fmt.Errorf("resolve fetcher client for list connections: %w", err)
	}

	connections, err := delegate.ListConnections(ctx, productName)
	if err != nil {
		return nil, fmt.Errorf("list fetcher connections: %w", err)
	}

	return connections, nil
}

// GetSchema delegates schema retrieval to the active Fetcher client.
func (client *dynamicFetcherClient) GetSchema(ctx context.Context, connectionID string) (*sharedPorts.FetcherSchema, error) {
	delegate, err := client.current()
	if err != nil {
		return nil, fmt.Errorf("resolve fetcher client for get schema: %w", err)
	}

	schema, err := delegate.GetSchema(ctx, connectionID)
	if err != nil {
		return nil, fmt.Errorf("get fetcher schema: %w", err)
	}

	return schema, nil
}

// TestConnection delegates connection testing to the active Fetcher client.
func (client *dynamicFetcherClient) TestConnection(ctx context.Context, connectionID string) (*sharedPorts.FetcherTestResult, error) {
	delegate, err := client.current()
	if err != nil {
		return nil, fmt.Errorf("resolve fetcher client for test connection: %w", err)
	}

	result, err := delegate.TestConnection(ctx, connectionID)
	if err != nil {
		return nil, fmt.Errorf("test fetcher connection: %w", err)
	}

	return result, nil
}

// SubmitExtractionJob delegates extraction job submission to the active Fetcher client.
func (client *dynamicFetcherClient) SubmitExtractionJob(ctx context.Context, input sharedPorts.ExtractionJobInput) (string, error) {
	delegate, err := client.current()
	if err != nil {
		return "", fmt.Errorf("resolve fetcher client for submit extraction job: %w", err)
	}

	jobID, err := delegate.SubmitExtractionJob(ctx, input)
	if err != nil {
		return "", fmt.Errorf("submit extraction job: %w", err)
	}

	return jobID, nil
}

// GetExtractionJobStatus delegates extraction job status retrieval to the active Fetcher client.
func (client *dynamicFetcherClient) GetExtractionJobStatus(ctx context.Context, jobID string) (*sharedPorts.ExtractionJobStatus, error) {
	delegate, err := client.current()
	if err != nil {
		return nil, fmt.Errorf("resolve fetcher client for extraction job status: %w", err)
	}

	status, err := delegate.GetExtractionJobStatus(ctx, jobID)
	if err != nil {
		return nil, fmt.Errorf("get extraction job status: %w", err)
	}

	return status, nil
}

func (client *dynamicFetcherClient) current() (sharedPorts.FetcherClient, error) {
	client.mu.Lock()
	defer client.mu.Unlock()

	cfg := client.initialCfg
	if client.configGetter != nil {
		if runtimeCfg := client.configGetter(); runtimeCfg != nil {
			cfg = runtimeCfg
		}
	}

	if cfg == nil || !cfg.Fetcher.Enabled {
		return nil, sharedPorts.ErrFetcherUnavailable
	}

	key := fmt.Sprintf("%s|%t|%s|%s", cfg.Fetcher.URL, cfg.Fetcher.AllowPrivateIPs, cfg.FetcherHealthTimeout(), cfg.FetcherRequestTimeout())
	if client.activeClient != nil && client.activeKey == key {
		return client.activeClient, nil
	}

	fetcherClient, err := discoveryFetcher.NewHTTPFetcherClient(fetcherHTTPClientConfig(cfg), client.breaker)
	if err != nil {
		return nil, fmt.Errorf("%w: create fetcher client: %w", sharedPorts.ErrFetcherUnavailable, err)
	}

	// Inject M2M provider for multi-tenant credential injection.
	if client.m2mProvider != nil {
		fetcherClient.SetM2MProvider(client.m2mProvider)
	}

	// Inject token exchanger for Bearer authentication.
	if client.tokenExchanger != nil {
		fetcherClient.SetTokenExchanger(client.tokenExchanger)
	}

	client.activeKey = key
	client.activeClient = fetcherClient

	return client.activeClient, nil
}
