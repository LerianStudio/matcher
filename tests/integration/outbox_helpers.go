//go:build integration

// Package integration provides shared test infrastructure for integration tests.
package integration

import (
	"context"
	"fmt"
	"testing"

	"github.com/LerianStudio/lib-commons/v5/commons/outbox"
	outboxpg "github.com/LerianStudio/lib-commons/v5/commons/outbox/postgres"
	libPostgres "github.com/LerianStudio/lib-commons/v5/commons/postgres"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/auth"
)

// testDefaultTenantDiscoverer mirrors the production defaultTenantDiscoverer
// in internal/bootstrap/outbox_wiring.go without importing the bootstrap
// package (which would create an import cycle: some integration tests under
// internal/... import this package, and bootstrap transitively imports those
// packages). Duplicating the ~20 lines of logic is preferable to a cycle,
// and the behaviour is pinned by the regression test below.
type testDefaultTenantDiscoverer struct {
	inner outbox.TenantDiscoverer
}

func (d *testDefaultTenantDiscoverer) DiscoverTenants(ctx context.Context) ([]string, error) {
	if d == nil || d.inner == nil {
		return nil, fmt.Errorf("test default tenant discoverer: nil inner")
	}

	tenants, err := d.inner.DiscoverTenants(ctx)
	if err != nil {
		return nil, fmt.Errorf("discover tenants: %w", err)
	}

	defaultTenantID := auth.GetDefaultTenantID()
	if defaultTenantID == "" {
		return tenants, nil
	}

	for _, t := range tenants {
		if t == defaultTenantID {
			return tenants, nil
		}
	}

	return append(tenants, defaultTenantID), nil
}

// NewTestOutboxRepository creates a canonical outbox repository suitable for
// integration tests. It mirrors production wiring (internal/bootstrap) by:
//   - using WithAllowEmptyTenant so single-tenant harnesses (public schema)
//     work without a UUID tenant context,
//   - passing WithDefaultTenantID so the canonical layer knows which tenant
//     maps to the public schema, and
//   - wrapping the SchemaResolver in a test-local default-tenant discoverer
//     so DiscoverTenants always includes the default tenant — matching how
//     matcher boots outside tests.
func NewTestOutboxRepository(t *testing.T, conn *libPostgres.Client) outbox.OutboxRepository {
	t.Helper()

	resolver, err := outboxpg.NewSchemaResolver(
		conn,
		outboxpg.WithAllowEmptyTenant(),
		outboxpg.WithDefaultTenantID(auth.GetDefaultTenantID()),
	)
	require.NoError(t, err, "create outbox schema resolver for test")

	tenantDiscoverer := &testDefaultTenantDiscoverer{inner: resolver}

	repo, err := outboxpg.NewRepository(conn, resolver, tenantDiscoverer)
	require.NoError(t, err, "create outbox repository for test")

	return repo
}
