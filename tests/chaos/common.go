//go:build unit || chaos

// Package chaos provides shared helpers for chaos-specific harnesses and unit tests.
package chaos

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	libPostgres "github.com/LerianStudio/lib-commons/v4/commons/postgres"
	toxiproxy "github.com/Shopify/toxiproxy/v2/client"
	"github.com/google/uuid"
	"github.com/testcontainers/testcontainers-go"
)

// SeedData contains pre-created entities for chaos tests.
type SeedData struct {
	TenantID  uuid.UUID
	ContextID uuid.UUID
	SourceID  uuid.UUID
}

// ChaosHarness provides the complete chaos testing infrastructure.
type ChaosHarness struct {
	Network           *testcontainers.DockerNetwork
	PostgresContainer testcontainers.Container
	RedisContainer    testcontainers.Container
	RabbitMQContainer testcontainers.Container
	ToxiContainer     testcontainers.Container

	ToxiClient  *toxiproxy.Client
	PGProxy     *toxiproxy.Proxy
	RedisProxy  *toxiproxy.Proxy
	RabbitProxy *toxiproxy.Proxy

	ProxiedPostgresDSN string
	ProxiedRedisAddr   string
	ProxiedRabbitHost  string
	ProxiedRabbitPort  string

	DirectPostgresDSN string
	DirectRedisAddr   string
	DirectRabbitHost  string
	DirectRabbitPort  string
	RabbitMQHealthURL string

	Connection *libPostgres.Client
	Seed       SeedData
	closeDBs   func() error

	testMu sync.Mutex
}

var (
	sharedChaos     *ChaosHarness
	sharedChaosOnce sync.Once
	sharedChaosErr  error
)

// GetSharedChaos returns the initialized shared chaos harness.
func GetSharedChaos() *ChaosHarness {
	return sharedChaos
}

// CleanupSharedChaos terminates all containers and the network.
func CleanupSharedChaos(ctx context.Context) error {
	if sharedChaos == nil {
		return nil
	}

	return sharedChaos.Cleanup(ctx)
}

// Cleanup terminates all containers and removes the network.
func (h *ChaosHarness) Cleanup(ctx context.Context) error {
	var errs []error

	if h == nil {
		return nil
	}

	if h.closeDBs != nil {
		if err := h.closeDBs(); err != nil {
			errs = append(errs, fmt.Errorf("database: %w", err))
		}
	}

	if h.ToxiContainer != nil {
		if err := h.ToxiContainer.Terminate(ctx); err != nil {
			errs = append(errs, fmt.Errorf("toxiproxy: %w", err))
		}
	}

	if h.RabbitMQContainer != nil {
		if err := h.RabbitMQContainer.Terminate(ctx); err != nil {
			errs = append(errs, fmt.Errorf("rabbitmq: %w", err))
		}
	}

	if h.RedisContainer != nil {
		if err := h.RedisContainer.Terminate(ctx); err != nil {
			errs = append(errs, fmt.Errorf("redis: %w", err))
		}
	}

	if h.PostgresContainer != nil {
		if err := h.PostgresContainer.Terminate(ctx); err != nil {
			errs = append(errs, fmt.Errorf("postgres: %w", err))
		}
	}

	if h.Network != nil {
		if err := h.Network.Remove(ctx); err != nil {
			errs = append(errs, fmt.Errorf("network: %w", err))
		}
	}

	return errors.Join(errs...)
}

// OutboxStats summarizes outbox event states for diagnostic reporting.
type OutboxStats struct {
	Pending    int
	Processing int
	Published  int
	Failed     int
	Invalid    int
	Total      int
}

// String provides a human-readable summary.
func (s OutboxStats) String() string {
	return fmt.Sprintf(
		"outbox[total=%d pending=%d processing=%d published=%d failed=%d invalid=%d]",
		s.Total, s.Pending, s.Processing, s.Published, s.Failed, s.Invalid,
	)
}

func dispatchOutboxUntilEmpty(maxIterations int, sleep func(time.Duration), dispatch func() int) int {
	total := 0
	for range maxIterations {
		processed := dispatch()
		if processed == 0 {
			break
		}
		total += processed
		if sleep != nil {
			sleep(50 * time.Millisecond)
		}
	}
	return total
}

// BuildCSVContent generates a simple CSV for chaos testing.
func BuildCSVContent(rows int) string {
	csv := "external_id,date,amount,currency\n"
	for i := range rows {
		csv += fmt.Sprintf("CHAOS-%05d,2025-01-15,%d.00,USD\n", i, (i+1)*100)
	}
	return csv
}

type proxyController interface {
	Name() string
	ListToxicNames() ([]string, error)
	RemoveToxicByName(name string) error
	SetEnabled(enabled bool) error
}

func removeAllToxicsFromProxies(proxies []proxyController) error {
	var cleanupErr error
	for _, proxy := range proxies {
		if proxy == nil {
			continue
		}
		toxics, err := proxy.ListToxicNames()
		if err != nil {
			cleanupErr = errors.Join(cleanupErr, fmt.Errorf("list toxics for %s: %w", proxy.Name(), err))
			continue
		}
		for _, toxic := range toxics {
			if err := proxy.RemoveToxicByName(toxic); err != nil {
				cleanupErr = errors.Join(cleanupErr, fmt.Errorf("remove toxic %s from %s: %w", toxic, proxy.Name(), err))
			}
		}
	}
	return cleanupErr
}

func enableAllProxyControllers(proxies []proxyController) error {
	var enableErr error
	for _, proxy := range proxies {
		if proxy == nil {
			continue
		}
		if err := proxy.SetEnabled(true); err != nil {
			enableErr = errors.Join(enableErr, fmt.Errorf("re-enable proxy %s: %w", proxy.Name(), err))
		}
	}
	return enableErr
}

func isolateServiceProxy(service string, proxies map[string]proxyController) (func() error, error) {
	proxy, ok := proxies[service]
	if !ok {
		return nil, fmt.Errorf("unknown service: %s (expected: postgres, redis, rabbitmq)", service)
	}
	if proxy == nil {
		return nil, fmt.Errorf("%s proxy is nil", service)
	}
	if err := proxy.SetEnabled(false); err != nil {
		return nil, fmt.Errorf("isolate %s: %w", service, err)
	}
	return func() error {
		return proxy.SetEnabled(true)
	}, nil
}
