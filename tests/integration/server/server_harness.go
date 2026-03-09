//go:build integration

// Package server provides a full-stack integration test harness with HTTP server.
package server

import (
	"context"
	"fmt"
	"testing"

	libLog "github.com/LerianStudio/lib-commons/v4/commons/log"

	"github.com/LerianStudio/matcher/internal/bootstrap"
	outboxServices "github.com/LerianStudio/matcher/internal/outbox/services"
	"github.com/LerianStudio/matcher/tests/integration"
)

// RabbitMQ event routing keys (must match production publishers).
const (
	ExchangeName                 = "matcher.events"
	RoutingKeyIngestionCompleted = "ingestion.completed"
	RoutingKeyIngestionFailed    = "ingestion.failed"
	RoutingKeyMatchConfirmed     = "matching.match_confirmed"
)

// ServerHarness provides a full-stack integration test environment with
// the Fiber HTTP server, real database connections, and RabbitMQ.
type ServerHarness struct {
	*integration.TestHarness
	serverHarnessBase

	Service *bootstrap.Service
}

// NewServerHarness creates a full-stack integration harness with HTTP server.
func NewServerHarness(ctx context.Context, t *testing.T) (*ServerHarness, error) {
	t.Helper()

	// Create base harness with containers
	baseHarness, err := integration.NewTestHarness(ctx, t)
	if err != nil {
		return nil, fmt.Errorf("failed to create base harness: %w", err)
	}

	// Initialize database
	if err := baseHarness.InitDatabase(t); err != nil {
		if cleanupErr := baseHarness.Cleanup(ctx); cleanupErr != nil {
			t.Logf("cleanup error after InitDatabase failure: %v", cleanupErr)
		}
		return nil, fmt.Errorf("failed to initialize database: %w", err)
	}

	sh := &ServerHarness{
		TestHarness: baseHarness,
	}
	sh.serverHarnessBase = serverHarnessBase{
		t:                 t,
		PostgresDSN:       baseHarness.PostgresDSN,
		RedisAddr:         baseHarness.RedisAddr,
		RabbitMQHost:      baseHarness.RabbitMQHost,
		RabbitMQPort:      baseHarness.RabbitMQPort,
		RabbitMQHealthURL: baseHarness.RabbitMQHealthURL,
		Seed:              baseHarness.Seed,
	}

	// Set environment variables for bootstrap
	if err := sh.setEnvFromContainers(t); err != nil {
		if cleanupErr := baseHarness.Cleanup(ctx); cleanupErr != nil {
			t.Logf("cleanup error after env setup failure: %v", cleanupErr)
		}
		return nil, fmt.Errorf("failed to set environment: %w", err)
	}

	// Initialize the service
	svc, err := bootstrap.InitServersWithOptions(&bootstrap.Options{
		Logger: &libLog.NopLogger{},
	})
	if err != nil {
		if cleanupErr := baseHarness.Cleanup(ctx); cleanupErr != nil {
			t.Logf("cleanup error after service init failure: %v", cleanupErr)
		}
		return nil, fmt.Errorf("failed to initialize service: %w", err)
	}

	sh.Service = svc
	sh.App = svc.GetApp()

	// Extract outbox dispatcher for controlled dispatch
	if dispatcher, ok := extractOutboxDispatcher(svc); ok {
		sh.OutboxDispatcher = dispatcher
	}

	// Setup RabbitMQ consumer spy
	if err := sh.setupRabbitSpy(t); err != nil {
		if cleanupErr := baseHarness.Cleanup(ctx); cleanupErr != nil {
			t.Logf("cleanup error after rabbit spy setup failure: %v", cleanupErr)
		}
		return nil, fmt.Errorf("failed to setup rabbit spy: %w", err)
	}

	return sh, nil
}

// Cleanup releases all resources.
func (sh *ServerHarness) Cleanup(ctx context.Context) error {
	var errs []error

	// Close RabbitMQ spy
	if sh.rabbitCh != nil {
		if err := sh.rabbitCh.Close(); err != nil {
			errs = append(errs, fmt.Errorf("rabbit channel: %w", err))
		}
	}
	if sh.rabbitConn != nil {
		if err := sh.rabbitConn.Close(); err != nil {
			errs = append(errs, fmt.Errorf("rabbit connection: %w", err))
		}
	}

	// Shutdown Fiber app
	if sh.App != nil {
		if err := sh.App.Shutdown(); err != nil {
			errs = append(errs, fmt.Errorf("fiber shutdown: %w", err))
		}
	}

	// Cleanup base harness
	if err := sh.TestHarness.Cleanup(ctx); err != nil {
		errs = append(errs, err)
	}

	if len(errs) > 0 {
		return fmt.Errorf("cleanup errors: %v", errs)
	}
	return nil
}

// RunWithServer runs a test with the full server harness.
// If shared infrastructure is available (initialized via TestMain), it uses that.
// Otherwise, it falls back to creating per-test containers.
func RunWithServer(t *testing.T, testFn func(t *testing.T, sh *ServerHarness)) {
	// Check if shared infrastructure is available
	if integration.GetSharedInfra() != nil {
		// Use shared server harness
		RunWithSharedServer(t, func(t *testing.T, ssh *SharedServerHarness) {
			// Convert SharedServerHarness to ServerHarness-like interface
			// by wrapping it in a compatibility adapter
			legacyHarness := ssh.ToLegacyServerHarness()
			testFn(t, legacyHarness)
		})
		return
	}

	// Fallback to per-test containers (legacy behavior)
	ctx := context.Background()

	harness, err := NewServerHarness(ctx, t)
	if err != nil {
		t.Fatalf("failed to create server harness: %v", err)
	}

	t.Cleanup(func() {
		if err := harness.Cleanup(ctx); err != nil {
			t.Logf("failed to cleanup server harness: %v", err)
		}
	})

	testFn(t, harness)
}

// extractOutboxDispatcher attempts to extract the outbox dispatcher from the service.
func extractOutboxDispatcher(svc *bootstrap.Service) (*outboxServices.Dispatcher, bool) {
	if svc == nil {
		return nil, false
	}

	runner := svc.GetOutboxRunner()
	if runner == nil {
		return nil, false
	}

	dispatcher, ok := runner.(*outboxServices.Dispatcher)
	return dispatcher, ok
}
