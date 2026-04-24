//go:build integration

package integration

import (
	"context"
	"database/sql"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/auth"
	pgcommon "github.com/LerianStudio/matcher/internal/shared/adapters/postgres/common"
)

func TestIntegration_Flow_ReadReplicaRouting(t *testing.T) {
	RunWithDatabase(t, func(t *testing.T, h *TestHarness) {
		ctx := h.Ctx()
		provider := h.Provider()

		t.Run("WithTenantReadQuery executes on replica connection", func(t *testing.T) {
			result, err := pgcommon.WithTenantReadQuery(
				ctx,
				provider,
				func(q pgcommon.QueryExecutor) (int, error) {
					var count int
					err := q.QueryRowContext(ctx, "SELECT 1").Scan(&count)
					return count, err
				},
			)

			require.NoError(t, err)
			assert.Equal(t, 1, result)
		})

		t.Run("WithTenantRead applies tenant schema isolation", func(t *testing.T) {
			result, err := pgcommon.WithTenantRead(
				ctx,
				provider,
				func(conn *sql.Conn) (string, error) {
					var searchPath string
					err := conn.QueryRowContext(ctx, "SHOW search_path").Scan(&searchPath)
					return searchPath, err
				},
			)

			require.NoError(t, err)
			assert.Contains(t, result, "public")
		})

		t.Run("WithTenantReadQuery can read existing data", func(t *testing.T) {
			result, err := pgcommon.WithTenantReadQuery(
				ctx,
				provider,
				func(q pgcommon.QueryExecutor) (bool, error) {
					var exists bool
					err := q.QueryRowContext(ctx,
						"SELECT EXISTS(SELECT 1 FROM reconciliation_contexts WHERE id = $1)",
						h.Seed.ContextID,
					).Scan(&exists)
					return exists, err
				},
			)

			require.NoError(t, err)
			assert.True(t, result, "should find seeded context via replica")
		})

		t.Run("WithTenantTxProvider still uses primary for writes", func(t *testing.T) {
			_, err := pgcommon.WithTenantTxProvider(
				ctx,
				provider,
				func(tx *sql.Tx) (struct{}, error) {
					_, execErr := tx.ExecContext(ctx, "SELECT 1")
					return struct{}{}, execErr
				},
			)

			require.NoError(t, err)
		})
	})
}

func TestIntegration_Flow_ReplicaFallbackToPrimary(t *testing.T) {
	RunWithDatabase(t, func(t *testing.T, h *TestHarness) {
		ctx := h.Ctx()
		provider := h.Provider()

		t.Run("GetReplicaDB returns connection when no replica configured", func(t *testing.T) {
			dbLease, err := provider.GetReplicaDB(ctx)
			require.NoError(t, err)
			require.NotNil(t, dbLease)
			defer dbLease.Release()

			err = dbLease.DB().PingContext(ctx)
			require.NoError(t, err)
		})

		t.Run("read operations work when falling back to primary", func(t *testing.T) {
			result, err := pgcommon.WithTenantReadQuery(
				ctx,
				provider,
				func(q pgcommon.QueryExecutor) (int64, error) {
					var count int64
					err := q.QueryRowContext(ctx, "SELECT COUNT(*) FROM reconciliation_contexts").
						Scan(&count)
					return count, err
				},
			)

			require.NoError(t, err)
			assert.GreaterOrEqual(
				t,
				result,
				int64(1),
				"should have at least one context from seed data",
			)
		})
	})
}

func TestIntegration_Flow_ReadReplicaTenantIsolation(t *testing.T) {
	RunWithDatabase(t, func(t *testing.T, h *TestHarness) {
		provider := h.Provider()

		t.Run("default tenant context works on replica", func(t *testing.T) {
			ctx := context.WithValue(context.Background(), auth.TenantIDKey, auth.DefaultTenantID)

			result, err := pgcommon.WithTenantReadQuery(
				ctx,
				provider,
				func(q pgcommon.QueryExecutor) (int, error) {
					var count int
					err := q.QueryRowContext(ctx, "SELECT 1").Scan(&count)
					return count, err
				},
			)

			require.NoError(t, err)
			assert.Equal(t, 1, result)
		})

		t.Run("empty tenant context defaults correctly", func(t *testing.T) {
			ctx := context.Background()

			result, err := pgcommon.WithTenantReadQuery(
				ctx,
				provider,
				func(q pgcommon.QueryExecutor) (int, error) {
					var count int
					err := q.QueryRowContext(ctx, "SELECT 1").Scan(&count)
					return count, err
				},
			)

			require.NoError(t, err)
			assert.Equal(t, 1, result)
		})

		t.Run("different tenants see isolated data", func(t *testing.T) {
			tenantAID := "tenant-a-" + h.Seed.ContextID.String()[:8]
			tenantBID := "tenant-b-" + h.Seed.ContextID.String()[:8]

			ctxTenantA := context.WithValue(context.Background(), auth.TenantIDKey, tenantAID)
			ctxTenantB := context.WithValue(context.Background(), auth.TenantIDKey, tenantBID)
			ctxDefault := context.WithValue(
				context.Background(),
				auth.TenantIDKey,
				auth.DefaultTenantID,
			)

			testTableName := "test_tenant_isolation_" + h.Seed.ContextID.String()[:8]

			_, err := pgcommon.WithTenantTxProvider(
				ctxDefault,
				provider,
				func(tx *sql.Tx) (struct{}, error) {
					_, execErr := tx.ExecContext(
						ctxDefault,
						"CREATE TABLE IF NOT EXISTS "+testTableName+" (id TEXT PRIMARY KEY, tenant_id TEXT NOT NULL)",
					)
					return struct{}{}, execErr
				},
			)
			require.NoError(t, err)

			defer func() {
				_, _ = pgcommon.WithTenantTxProvider(
					ctxDefault,
					provider,
					func(tx *sql.Tx) (struct{}, error) {
						_, _ = tx.ExecContext(ctxDefault, "DROP TABLE IF EXISTS "+testTableName)
						return struct{}{}, nil
					},
				)
			}()

			_, err = pgcommon.WithTenantTxProvider(
				ctxDefault,
				provider,
				func(tx *sql.Tx) (struct{}, error) {
					_, execErr := tx.ExecContext(
						ctxDefault,
						"INSERT INTO "+testTableName+" (id, tenant_id) VALUES ($1, $2)",
						"row-tenant-a",
						tenantAID,
					)
					return struct{}{}, execErr
				},
			)
			require.NoError(t, err)

			countA, err := pgcommon.WithTenantReadQuery(
				ctxDefault,
				provider,
				func(q pgcommon.QueryExecutor) (int, error) {
					var count int
					scanErr := q.QueryRowContext(ctxTenantA, "SELECT COUNT(*) FROM "+testTableName+" WHERE tenant_id = $1", tenantAID).
						Scan(&count)
					return count, scanErr
				},
			)
			require.NoError(t, err)
			assert.Equal(t, 1, countA, "tenant A should see its own row")

			countB, err := pgcommon.WithTenantReadQuery(
				ctxDefault,
				provider,
				func(q pgcommon.QueryExecutor) (int, error) {
					var count int
					scanErr := q.QueryRowContext(ctxTenantB, "SELECT COUNT(*) FROM "+testTableName+" WHERE tenant_id = $1", tenantBID).
						Scan(&count)
					return count, scanErr
				},
			)
			require.NoError(t, err)
			assert.Equal(t, 0, countB, "tenant B should not see tenant A's row")
		})
	})
}
