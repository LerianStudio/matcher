//go:build integration

package matching

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"

	governancePostgres "github.com/LerianStudio/matcher/internal/governance/adapters/postgres"
	adjustmentRepo "github.com/LerianStudio/matcher/internal/matching/adapters/postgres/adjustment"
	matchingEntities "github.com/LerianStudio/matcher/internal/matching/domain/entities"
	matchingRepositories "github.com/LerianStudio/matcher/internal/matching/domain/repositories"
	matchingVO "github.com/LerianStudio/matcher/internal/matching/domain/value_objects"
	matchingCommand "github.com/LerianStudio/matcher/internal/matching/services/command"
	pgcommon "github.com/LerianStudio/matcher/internal/shared/adapters/postgres/common"
	sharedDomain "github.com/LerianStudio/matcher/internal/shared/domain"
	"github.com/LerianStudio/matcher/tests/integration"
)

func newAdjustmentAuditLog(t *testing.T, ctx context.Context, tenantID, adjustmentID uuid.UUID) *sharedDomain.AuditLog {
	t.Helper()

	changes := []byte(fmt.Sprintf(`{"entity_type":"adjustment","entity_id":"%s","action":"CREATE"}`, adjustmentID.String()))
	auditLog, err := sharedDomain.NewAuditLog(
		ctx,
		tenantID,
		"adjustment",
		adjustmentID,
		"CREATE",
		nil,
		changes,
	)
	require.NoError(t, err)

	return auditLog
}

// createTestAdjustment creates a persisted adjustment linked to the given match group.
// The suffix is appended to description/reason to ensure distinguishable records.
func createTestAdjustment(
	t *testing.T,
	ctx context.Context,
	repo *adjustmentRepo.Repository,
	contextID uuid.UUID,
	matchGroupID uuid.UUID,
	suffix string,
) *matchingEntities.Adjustment {
	t.Helper()

	entity, err := matchingEntities.NewAdjustment(
		ctx,
		contextID,
		&matchGroupID,
		nil,
		matchingEntities.AdjustmentTypeBankFee,
		matchingEntities.AdjustmentDirectionDebit,
		decimal.NewFromFloat(10.50),
		"USD",
		fmt.Sprintf("Integration test adjustment %s", suffix),
		fmt.Sprintf("Repo test reason %s", suffix),
		"integration-test-user",
	)
	require.NoError(t, err)

	created, err := repo.Create(ctx, entity)
	require.NoError(t, err)
	require.NotNil(t, created)

	return created
}

func TestAdjustmentRepository_Create(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctxBase := e4t9Ctx(t, h)
		ctx, cancel := context.WithTimeout(ctxBase, 90*time.Second)
		defer cancel()

		seed := seedE4T9Config(t, h)
		registerFailureDiagnostics(t, h, seed.ContextID)
		wired := wireE4T9UseCases(t, h)
		group := runMatchAndGetGroup(t, ctx, h, wired, seed)

		repo := adjustmentRepo.NewRepository(h.Provider(), nil)

		amount := decimal.NewFromFloat(7.25)
		entity, err := matchingEntities.NewAdjustment(
			ctx,
			seed.ContextID,
			&group.ID,
			nil,
			matchingEntities.AdjustmentTypeFXDifference,
			matchingEntities.AdjustmentDirectionCredit,
			amount,
			"EUR",
			"FX variance on cross-border settlement",
			"Exchange rate moved between capture and settlement",
			"repo-test-user",
		)
		require.NoError(t, err)

		created, err := repo.Create(ctx, entity)
		require.NoError(t, err)
		require.NotNil(t, created)

		// Verify every field round-trips correctly.
		require.NotEqual(t, uuid.Nil, created.ID)
		require.Equal(t, seed.ContextID, created.ContextID)
		require.NotNil(t, created.MatchGroupID)
		require.Equal(t, group.ID, *created.MatchGroupID)
		require.Nil(t, created.TransactionID)
		require.Equal(t, matchingEntities.AdjustmentTypeFXDifference, created.Type)
		require.Equal(t, matchingEntities.AdjustmentDirectionCredit, created.Direction)
		require.True(t, amount.Equal(created.Amount), "expected %s, got %s", amount, created.Amount)
		require.Equal(t, "EUR", created.Currency)
		require.Equal(t, "FX variance on cross-border settlement", created.Description)
		require.Equal(t, "Exchange rate moved between capture and settlement", created.Reason)
		require.Equal(t, "repo-test-user", created.CreatedBy)
		require.False(t, created.CreatedAt.IsZero())
		require.False(t, created.UpdatedAt.IsZero())

		// Verify row exists in the database.
		rowCount := countInt(t, ctx, h.Connection,
			"SELECT count(*) FROM adjustments WHERE id=$1",
			created.ID.String(),
		)
		require.Equal(t, 1, rowCount)
	})
}

func TestAdjustmentRepository_FindByID(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctxBase := e4t9Ctx(t, h)
		ctx, cancel := context.WithTimeout(ctxBase, 90*time.Second)
		defer cancel()

		seed := seedE4T9Config(t, h)
		registerFailureDiagnostics(t, h, seed.ContextID)
		wired := wireE4T9UseCases(t, h)
		group := runMatchAndGetGroup(t, ctx, h, wired, seed)

		repo := adjustmentRepo.NewRepository(h.Provider(), nil)
		created := createTestAdjustment(t, ctx, repo, seed.ContextID, group.ID, "find-by-id")

		found, err := repo.FindByID(ctx, seed.ContextID, created.ID)
		require.NoError(t, err)
		require.NotNil(t, found)

		require.Equal(t, created.ID, found.ID)
		require.Equal(t, created.ContextID, found.ContextID)
		require.NotNil(t, found.MatchGroupID)
		require.Equal(t, *created.MatchGroupID, *found.MatchGroupID)
		require.Equal(t, created.Type, found.Type)
		require.Equal(t, created.Direction, found.Direction)
		require.True(t, created.Amount.Equal(found.Amount))
		require.Equal(t, created.Currency, found.Currency)
		require.Equal(t, created.Description, found.Description)
		require.Equal(t, created.Reason, found.Reason)
		require.Equal(t, created.CreatedBy, found.CreatedBy)
	})
}

func TestAdjustmentRepository_FindByID_NotFound(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctxBase := e4t9Ctx(t, h)
		ctx, cancel := context.WithTimeout(ctxBase, 90*time.Second)
		defer cancel()

		seed := seedE4T9Config(t, h)

		repo := adjustmentRepo.NewRepository(h.Provider(), nil)

		_, err := repo.FindByID(ctx, seed.ContextID, uuid.New())
		require.ErrorIs(t, err, sql.ErrNoRows)
	})
}

func TestAdjustmentRepository_ListByContextID(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctxBase := e4t9Ctx(t, h)
		ctx, cancel := context.WithTimeout(ctxBase, 90*time.Second)
		defer cancel()

		seed := seedE4T9Config(t, h)
		registerFailureDiagnostics(t, h, seed.ContextID)
		wired := wireE4T9UseCases(t, h)
		group := runMatchAndGetGroup(t, ctx, h, wired, seed)

		repo := adjustmentRepo.NewRepository(h.Provider(), nil)

		const count = 3
		createdIDs := make(map[uuid.UUID]struct{}, count)

		for i := 0; i < count; i++ {
			adj := createTestAdjustment(t, ctx, repo, seed.ContextID, group.ID, fmt.Sprintf("list-%d", i))
			createdIDs[adj.ID] = struct{}{}
		}

		listed, _, err := repo.ListByContextID(
			ctx,
			seed.ContextID,
			matchingRepositories.CursorFilter{Limit: 20},
		)
		require.NoError(t, err)
		require.GreaterOrEqual(t, len(listed), count)

		// Verify all 3 created adjustments appear in the result set.
		foundCount := 0
		for _, adj := range listed {
			if _, ok := createdIDs[adj.ID]; ok {
				foundCount++
			}
		}
		require.Equal(t, count, foundCount, "all created adjustments must appear in list result")
	})
}

func TestAdjustmentRepository_ListByContextID_Pagination(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctxBase := e4t9Ctx(t, h)
		ctx, cancel := context.WithTimeout(ctxBase, 90*time.Second)
		defer cancel()

		seed := seedE4T9Config(t, h)
		registerFailureDiagnostics(t, h, seed.ContextID)
		wired := wireE4T9UseCases(t, h)
		group := runMatchAndGetGroup(t, ctx, h, wired, seed)

		repo := adjustmentRepo.NewRepository(h.Provider(), nil)

		const totalAdjustments = 5
		allIDs := make(map[uuid.UUID]struct{}, totalAdjustments)

		for i := 0; i < totalAdjustments; i++ {
			adj := createTestAdjustment(t, ctx, repo, seed.ContextID, group.ID, fmt.Sprintf("page-%d", i))
			allIDs[adj.ID] = struct{}{}
		}

		const pageSize = 2
		collected := make(map[uuid.UUID]struct{})
		cursor := ""
		pages := 0
		maxPages := 10 // safety guard to avoid infinite loops

		for pages < maxPages {
			page, pagination, err := repo.ListByContextID(
				ctx,
				seed.ContextID,
				matchingRepositories.CursorFilter{Limit: pageSize, Cursor: cursor},
			)
			require.NoError(t, err)

			for _, adj := range page {
				_, duplicate := collected[adj.ID]
				require.False(t, duplicate, "adjustment %s appeared on multiple pages", adj.ID)
				collected[adj.ID] = struct{}{}
			}

			pages++

			if pagination.Next == "" {
				break
			}

			cursor = pagination.Next
		}

		// Verify every adjustment we created was returned across all pages.
		for id := range allIDs {
			_, found := collected[id]
			require.True(t, found, "adjustment %s was not returned in any page", id)
		}
	})
}

func TestAdjustmentRepository_ListByMatchGroupID(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctxBase := e4t9Ctx(t, h)
		ctx, cancel := context.WithTimeout(ctxBase, 120*time.Second)
		defer cancel()

		seed := seedE4T9Config(t, h)
		registerFailureDiagnostics(t, h, seed.ContextID)
		wired := wireE4T9UseCases(t, h)

		// Create first match group via the standard helper.
		group1 := runMatchAndGetGroup(t, ctx, h, wired, seed)

		// Create second match group by ingesting a different pair of transactions
		// within the SAME configuration (avoids duplicate rule constraint from calling
		// seedE4T9Config twice on the same context).
		ledgerCSV2 := buildCSV("MATCH-GRP2-001", "350.00", "USD", "2026-02-20", "second-payment")
		_, err := wired.IngestionUC.StartIngestion(
			ctx, seed.ContextID, seed.LedgerSourceID,
			"ledger_grp2.csv", int64(len(ledgerCSV2)), "csv",
			strings.NewReader(ledgerCSV2),
		)
		require.NoError(t, err)

		bankCSV2 := buildCSV("match-grp2-001", "350.00", "USD", "2026-02-20", "second-payment")
		_, err = wired.IngestionUC.StartIngestion(
			ctx, seed.ContextID, seed.NonLedgerSourceID,
			"bank_grp2.csv", int64(len(bankCSV2)), "csv",
			strings.NewReader(bankCSV2),
		)
		require.NoError(t, err)

		_, groups2, err := wired.MatchingUC.RunMatch(ctx, matchingCommand.RunMatchInput{
			TenantID:  h.Seed.TenantID,
			ContextID: seed.ContextID,
			Mode:      matchingVO.MatchRunModeCommit,
		})
		require.NoError(t, err)
		require.NotEmpty(t, groups2, "expected at least one match group from second RunMatch")

		group2 := groups2[0]

		repo := adjustmentRepo.NewRepository(h.Provider(), nil)

		// Create 2 adjustments for group1.
		g1Adj1 := createTestAdjustment(t, ctx, repo, seed.ContextID, group1.ID, "g1-a")
		g1Adj2 := createTestAdjustment(t, ctx, repo, seed.ContextID, group1.ID, "g1-b")

		// Create 1 adjustment for group2.
		createTestAdjustment(t, ctx, repo, seed.ContextID, group2.ID, "g2-a")

		// Query by group1 — should return exactly 2.
		listed, err := repo.ListByMatchGroupID(ctx, seed.ContextID, group1.ID)
		require.NoError(t, err)
		require.Len(t, listed, 2)

		listedIDs := make(map[uuid.UUID]struct{}, len(listed))
		for _, adj := range listed {
			listedIDs[adj.ID] = struct{}{}
		}

		_, hasAdj1 := listedIDs[g1Adj1.ID]
		_, hasAdj2 := listedIDs[g1Adj2.ID]
		require.True(t, hasAdj1, "group1 adjustment 1 must be in result")
		require.True(t, hasAdj2, "group1 adjustment 2 must be in result")

		// Query by group2 — should return exactly 1.
		listed2, err := repo.ListByMatchGroupID(ctx, seed.ContextID, group2.ID)
		require.NoError(t, err)
		require.Len(t, listed2, 1)
	})
}

func TestAdjustmentRepository_CreateWithAuditLog(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctxBase := e4t9Ctx(t, h)
		ctx, cancel := context.WithTimeout(ctxBase, 90*time.Second)
		defer cancel()

		seed := seedE4T9Config(t, h)
		registerFailureDiagnostics(t, h, seed.ContextID)
		wired := wireE4T9UseCases(t, h)
		group := runMatchAndGetGroup(t, ctx, h, wired, seed)

		auditRepo := governancePostgres.NewRepository(h.Provider())
		repo := adjustmentRepo.NewRepository(h.Provider(), auditRepo)

		entity, err := matchingEntities.NewAdjustment(
			ctx,
			seed.ContextID,
			&group.ID,
			nil,
			matchingEntities.AdjustmentTypeBankFee,
			matchingEntities.AdjustmentDirectionDebit,
			decimal.NewFromFloat(13.37),
			"USD",
			"SOX integration test adjustment",
			"integration audit path",
			"integration-test-user",
		)
		require.NoError(t, err)

		auditLog := newAdjustmentAuditLog(t, ctx, h.Seed.TenantID, entity.ID)

		created, err := repo.CreateWithAuditLog(ctx, entity, auditLog)
		require.NoError(t, err)
		require.NotNil(t, created)

		adjustmentRows := countInt(
			t,
			ctx,
			h.Connection,
			"SELECT count(*) FROM adjustments WHERE id=$1",
			created.ID.String(),
		)
		require.Equal(t, 1, adjustmentRows)

		auditRows := countInt(
			t,
			ctx,
			h.Connection,
			"SELECT count(*) FROM audit_logs WHERE entity_type=$1 AND entity_id=$2",
			"adjustment",
			created.ID.String(),
		)
		require.Equal(t, 1, auditRows)
	})
}

func TestAdjustmentRepository_CreateWithAuditLogWithTx(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctxBase := e4t9Ctx(t, h)
		ctx, cancel := context.WithTimeout(ctxBase, 90*time.Second)
		defer cancel()

		seed := seedE4T9Config(t, h)
		registerFailureDiagnostics(t, h, seed.ContextID)
		wired := wireE4T9UseCases(t, h)
		group := runMatchAndGetGroup(t, ctx, h, wired, seed)

		auditRepo := governancePostgres.NewRepository(h.Provider())
		repo := adjustmentRepo.NewRepository(h.Provider(), auditRepo)

		entity, err := matchingEntities.NewAdjustment(
			ctx,
			seed.ContextID,
			&group.ID,
			nil,
			matchingEntities.AdjustmentTypeRounding,
			matchingEntities.AdjustmentDirectionCredit,
			decimal.NewFromFloat(0.02),
			"USD",
			"SOX tx integration test",
			"integration tx audit path",
			"integration-test-user",
		)
		require.NoError(t, err)

		auditLog := newAdjustmentAuditLog(t, ctx, h.Seed.TenantID, entity.ID)

		_, err = pgcommon.WithTenantTxProvider(ctx, h.Provider(), func(tx *sql.Tx) (*matchingEntities.Adjustment, error) {
			return repo.CreateWithAuditLogWithTx(ctx, tx, entity, auditLog)
		})
		require.NoError(t, err)

		adjustmentRows := countInt(
			t,
			ctx,
			h.Connection,
			"SELECT count(*) FROM adjustments WHERE id=$1",
			entity.ID.String(),
		)
		require.Equal(t, 1, adjustmentRows)

		auditRows := countInt(
			t,
			ctx,
			h.Connection,
			"SELECT count(*) FROM audit_logs WHERE entity_type=$1 AND entity_id=$2",
			"adjustment",
			entity.ID.String(),
		)
		require.Equal(t, 1, auditRows)
	})
}

func TestAdjustmentRepository_CreateWithAuditLog_RollbackWhenAuditMissing(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctxBase := e4t9Ctx(t, h)
		ctx, cancel := context.WithTimeout(ctxBase, 90*time.Second)
		defer cancel()

		seed := seedE4T9Config(t, h)
		registerFailureDiagnostics(t, h, seed.ContextID)
		wired := wireE4T9UseCases(t, h)
		group := runMatchAndGetGroup(t, ctx, h, wired, seed)

		auditRepo := governancePostgres.NewRepository(h.Provider())
		repo := adjustmentRepo.NewRepository(h.Provider(), auditRepo)

		entity, err := matchingEntities.NewAdjustment(
			ctx,
			seed.ContextID,
			&group.ID,
			nil,
			matchingEntities.AdjustmentTypeBankFee,
			matchingEntities.AdjustmentDirectionDebit,
			decimal.NewFromFloat(9.99),
			"USD",
			"rollback adjustment",
			"missing audit log must rollback",
			"integration-test-user",
		)
		require.NoError(t, err)

		_, err = repo.CreateWithAuditLog(ctx, entity, nil)
		require.ErrorIs(t, err, adjustmentRepo.ErrAuditLogRequired)

		adjustmentRows := countInt(
			t,
			ctx,
			h.Connection,
			"SELECT count(*) FROM adjustments WHERE id=$1",
			entity.ID.String(),
		)
		require.Equal(t, 0, adjustmentRows)
	})
}
