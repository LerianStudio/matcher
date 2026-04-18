//go:build integration

// Package integration provides shared test infrastructure for integration tests.
package integration

import (
	"testing"

	"github.com/LerianStudio/lib-commons/v5/commons/outbox"
	outboxpg "github.com/LerianStudio/lib-commons/v5/commons/outbox/postgres"
	libPostgres "github.com/LerianStudio/lib-commons/v5/commons/postgres"
	"github.com/stretchr/testify/require"
)

// NewTestOutboxRepository creates a canonical outbox repository suitable for
// integration tests. It uses WithAllowEmptyTenant so that single-tenant test
// harnesses (which run in the public schema) work without a UUID tenant context.
func NewTestOutboxRepository(t *testing.T, conn *libPostgres.Client) outbox.OutboxRepository {
	t.Helper()

	resolver, err := outboxpg.NewSchemaResolver(conn, outboxpg.WithAllowEmptyTenant())
	require.NoError(t, err, "create outbox schema resolver for test")

	repo, err := outboxpg.NewRepository(conn, resolver, resolver)
	require.NoError(t, err, "create outbox repository for test")

	return repo
}
