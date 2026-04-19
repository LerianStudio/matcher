//go:build integration

// Package integration provides shared test infrastructure for integration tests.
package integration

import (
	"testing"

	"github.com/LerianStudio/lib-commons/v5/commons/outbox"
	outboxpg "github.com/LerianStudio/lib-commons/v5/commons/outbox/postgres"
	libPostgres "github.com/LerianStudio/lib-commons/v5/commons/postgres"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/auth"
	"github.com/LerianStudio/matcher/internal/bootstrap"
)

// NewTestOutboxRepository creates a canonical outbox repository suitable for
// integration tests. It mirrors production wiring by:
//   - using WithAllowEmptyTenant so single-tenant harnesses (public schema)
//     work without a UUID tenant context,
//   - passing WithDefaultTenantID so the canonical layer knows which tenant
//     maps to the public schema, and
//   - wrapping the SchemaResolver in bootstrap.NewDefaultTenantDiscoverer so
//     DiscoverTenants always includes the default tenant — matching how
//     matcher boots outside tests.
func NewTestOutboxRepository(t *testing.T, conn *libPostgres.Client) outbox.OutboxRepository {
	t.Helper()

	resolver, err := outboxpg.NewSchemaResolver(
		conn,
		outboxpg.WithAllowEmptyTenant(),
		outboxpg.WithDefaultTenantID(auth.GetDefaultTenantID()),
	)
	require.NoError(t, err, "create outbox schema resolver for test")

	tenantDiscoverer := bootstrap.NewDefaultTenantDiscoverer(resolver)

	repo, err := outboxpg.NewRepository(conn, resolver, tenantDiscoverer)
	require.NoError(t, err, "create outbox repository for test")

	return repo
}
