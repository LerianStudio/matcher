//go:build integration

package auth_test

import (
	"context"
	"database/sql"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/auth"
	configContextRepo "github.com/LerianStudio/matcher/internal/configuration/adapters/postgres/context"
	configEntities "github.com/LerianStudio/matcher/internal/configuration/domain/entities"
	pgcommon "github.com/LerianStudio/matcher/internal/shared/adapters/postgres/common"
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
	"github.com/LerianStudio/matcher/tests/integration"
)

// TestIntegration_Auth_CrossTenantIsolation_H03 verifies that PostgreSQL schema-based tenant isolation
// prevents one tenant from accessing another tenant's data.
//
// This test addresses H-03 from REVIEW_AUTH.md:
// "No E2E test proves tenant isolation prevents cross-tenant data access."
func TestIntegration_Auth_CrossTenantIsolation_H03(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		t.Run("tenant A cannot see tenant B data", func(t *testing.T) {
			tenantAID := uuid.New()
			tenantBID := uuid.New()

			ctxTenantA := context.WithValue(context.Background(), auth.TenantIDKey, tenantAID.String())
			ctxTenantB := context.WithValue(context.Background(), auth.TenantIDKey, tenantBID.String())

			err := createTenantSchema(t, h, tenantAID.String())
			require.NoError(t, err, "failed to create schema for tenant A")

			err = createTenantSchema(t, h, tenantBID.String())
			require.NoError(t, err, "failed to create schema for tenant B")

			contextRepo := configContextRepo.NewRepository(h.Provider())

			contextA, err := configEntities.NewReconciliationContext(
				ctxTenantA,
				tenantAID,
				configEntities.CreateReconciliationContextInput{
					Name:     "Tenant A Context",
					Type:     shared.ContextTypeOneToOne,
					Interval: "0 0 * * *",
				},
			)
			require.NoError(t, err)

			createdA, err := contextRepo.Create(ctxTenantA, contextA)
			require.NoError(t, err, "failed to create context for tenant A")

			contextB, err := configEntities.NewReconciliationContext(
				ctxTenantB,
				tenantBID,
				configEntities.CreateReconciliationContextInput{
					Name:     "Tenant B Context",
					Type:     shared.ContextTypeOneToMany,
					Interval: "0 0 * * *",
				},
			)
			require.NoError(t, err)

			createdB, err := contextRepo.Create(ctxTenantB, contextB)
			require.NoError(t, err, "failed to create context for tenant B")

			foundA, err := contextRepo.FindByID(ctxTenantA, createdA.ID)
			require.NoError(t, err, "tenant A should be able to read its own data")
			assert.Equal(t, "Tenant A Context", foundA.Name)

			foundB, err := contextRepo.FindByID(ctxTenantB, createdB.ID)
			require.NoError(t, err, "tenant B should be able to read its own data")
			assert.Equal(t, "Tenant B Context", foundB.Name)

			_, err = contextRepo.FindByID(ctxTenantA, createdB.ID)
			require.Error(t, err, "tenant A should NOT be able to read tenant B's data")

			_, err = contextRepo.FindByID(ctxTenantB, createdA.ID)
			require.Error(t, err, "tenant B should NOT be able to read tenant A's data")
		})

		t.Run("schema isolation enforced per transaction", func(t *testing.T) {
			tenantID := uuid.New()
			ctx := context.WithValue(context.Background(), auth.TenantIDKey, tenantID.String())

			err := createTenantSchema(t, h, tenantID.String())
			require.NoError(t, err)

			_, err = pgcommon.WithTenantTx(ctx, h.Connection, func(tx *sql.Tx) (struct{}, error) {
				var searchPath string
				row := tx.QueryRowContext(ctx, "SHOW search_path")

				if scanErr := row.Scan(&searchPath); scanErr != nil {
					return struct{}{}, scanErr
				}

				require.Contains(t, searchPath, tenantID.String(),
					"search_path should contain tenant ID within transaction")

				return struct{}{}, nil
			})
			require.NoError(t, err)

			var defaultSearchPath string
			resolver, err := h.Connection.Resolver(context.Background())
			require.NoError(t, err, "failed to get database connection")
			primaryDBs := resolver.PrimaryDBs()
			require.NotEmpty(t, primaryDBs, "no primary databases available")
			row := primaryDBs[0].QueryRowContext(context.Background(), "SHOW search_path")
			require.NoError(t, row.Scan(&defaultSearchPath))
			assert.NotContains(t, defaultSearchPath, tenantID.String(),
				"search_path should NOT contain tenant ID outside transaction")
		})

		t.Run("ApplyTenantSchema sets correct schema", func(t *testing.T) {
			tenantID := uuid.New()
			ctx := context.WithValue(context.Background(), auth.TenantIDKey, tenantID.String())

			err := createTenantSchema(t, h, tenantID.String())
			require.NoError(t, err)

			db, err := h.Connection.Resolver(context.Background())
			require.NoError(t, err, "failed to get database connection")

			// Use PrimaryDBs()[0] to get the actual *sql.DB, which returns *sql.Tx from BeginTx.
			// Calling BeginTx directly on dbresolver.DB returns *dbresolver.tx wrapper,
			// which ApplyTenantSchema doesn't accept (it requires *sql.Tx for security).
			primaryDBs := db.PrimaryDBs()
			require.NotEmpty(t, primaryDBs, "no primary database configured")

			tx, err := primaryDBs[0].BeginTx(ctx, nil)
			require.NoError(t, err)

			defer func() { _ = tx.Rollback() }()

			err = auth.ApplyTenantSchema(ctx, tx)
			require.NoError(t, err)

			var searchPath string
			err = tx.QueryRowContext(ctx, "SHOW search_path").Scan(&searchPath)
			require.NoError(t, err)

			expectedQuoted := "\"" + tenantID.String() + "\""
			assert.Contains(t, searchPath, expectedQuoted,
				"search_path should contain quoted tenant ID")
			assert.Contains(t, searchPath, "public",
				"search_path should include public schema as fallback")
		})
	})
}

func createTenantSchema(t *testing.T, h *integration.TestHarness, tenantID string) error {
	t.Helper()

	db, err := h.Connection.Resolver(context.Background())
	if err != nil {
		return err
	}

	quotedID := auth.QuoteIdentifier(tenantID)
	_, err = db.ExecContext(
		context.Background(),
		"CREATE SCHEMA IF NOT EXISTS "+quotedID,
	)
	if err != nil {
		return err
	}

	_, err = db.ExecContext(
		context.Background(),
		"SET search_path TO "+quotedID+", public",
	)
	if err != nil {
		return err
	}

	_, err = db.ExecContext(
		context.Background(),
		`CREATE TABLE IF NOT EXISTS `+quotedID+`.reconciliation_context (
			id UUID PRIMARY KEY,
			tenant_id UUID NOT NULL,
			name VARCHAR(255) NOT NULL,
			type VARCHAR(50) NOT NULL,
			interval VARCHAR(100),
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		)`,
	)
	if err != nil {
		return err
	}

	_, err = db.ExecContext(
		context.Background(),
		"SET search_path TO public",
	)

	return err
}
