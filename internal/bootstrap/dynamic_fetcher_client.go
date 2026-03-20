// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package bootstrap

import (
	"context"
	"fmt"
	"sync"

	discoveryFetcher "github.com/LerianStudio/matcher/internal/discovery/adapters/fetcher"
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

type dynamicFetcherClient struct {
	initialCfg   *Config
	configGetter func() *Config
	mu           sync.Mutex
	activeKey    string
	activeClient sharedPorts.FetcherClient
}

func newDynamicFetcherClient(initialCfg *Config, configGetter func() *Config) sharedPorts.FetcherClient {
	return &dynamicFetcherClient{initialCfg: initialCfg, configGetter: configGetter}
}

func (client *dynamicFetcherClient) IsHealthy(ctx context.Context) bool {
	delegate, err := client.current()
	if err != nil {
		return false
	}

	return delegate.IsHealthy(ctx)
}

func (client *dynamicFetcherClient) ListConnections(ctx context.Context, orgID string) ([]*sharedPorts.FetcherConnection, error) {
	delegate, err := client.current()
	if err != nil {
		return nil, err
	}

	return delegate.ListConnections(ctx, orgID)
}

func (client *dynamicFetcherClient) GetSchema(ctx context.Context, connectionID string) (*sharedPorts.FetcherSchema, error) {
	delegate, err := client.current()
	if err != nil {
		return nil, err
	}

	return delegate.GetSchema(ctx, connectionID)
}

func (client *dynamicFetcherClient) TestConnection(ctx context.Context, connectionID string) (*sharedPorts.FetcherTestResult, error) {
	delegate, err := client.current()
	if err != nil {
		return nil, err
	}

	return delegate.TestConnection(ctx, connectionID)
}

func (client *dynamicFetcherClient) SubmitExtractionJob(ctx context.Context, input sharedPorts.ExtractionJobInput) (string, error) {
	delegate, err := client.current()
	if err != nil {
		return "", err
	}

	return delegate.SubmitExtractionJob(ctx, input)
}

func (client *dynamicFetcherClient) GetExtractionJobStatus(ctx context.Context, jobID string) (*sharedPorts.ExtractionJobStatus, error) {
	delegate, err := client.current()
	if err != nil {
		return nil, err
	}

	return delegate.GetExtractionJobStatus(ctx, jobID)
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

	fetcherClient, err := discoveryFetcher.NewHTTPFetcherClient(fetcherHTTPClientConfig(cfg))
	if err != nil {
		return nil, sharedPorts.ErrFetcherUnavailable
	}

	client.activeKey = key
	client.activeClient = fetcherClient

	return client.activeClient, nil
}
