//go:build integration

// Package governance contains integration tests for the governance context's
// WithTx repository surface under multi-aggregate composition.
//
// Governance aggregates (AuditLog / ArchiveMetadata / ActorMapping) are the
// immutable spine of our compliance story. Audit logs form a hash chain and
// archive lifecycle transitions are observable to external auditors — partial
// persistence after a tx failure would make the chain inconsistent.
//
// Covers FINDING-042 (REFACTOR-051).
package governance

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	auditLogRepo "github.com/LerianStudio/matcher/internal/governance/adapters/postgres"
	actorMappingRepo "github.com/LerianStudio/matcher/internal/governance/adapters/postgres/actor_mapping"
	archiveRepo "github.com/LerianStudio/matcher/internal/governance/adapters/postgres/archive_metadata"
	governanceEntities "github.com/LerianStudio/matcher/internal/governance/domain/entities"
	pgcommon "github.com/LerianStudio/matcher/internal/shared/adapters/postgres/common"
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
	"github.com/LerianStudio/matcher/tests/integration"
)

// errDeliberateRollback forces the composition callback to abort, triggering
// tx rollback so the test can verify atomic undo across all repos involved.
var errDeliberateRollback = errors.New("deliberate rollback for composition test")

// newCompositionAuditLog builds a minimal valid AuditLog bound to the harness
// tenant. Each call produces a unique entity_id so concurrent tests don't
// collide on the hash chain sequence.
func newCompositionAuditLog(t *testing.T, ctx context.Context, tenantID uuid.UUID, action string) *shared.AuditLog {
	t.Helper()

	changes := json.RawMessage(`{"field":"value"}`)
	entry, err := shared.NewAuditLog(
		ctx,
		tenantID,
		"composition_test",
		uuid.New(),
		action,
		nil,
		changes,
	)
	require.NoError(t, err)

	return entry
}

// TestIntegration_Governance_WithTxComposition_AuditLogChain_Rollback
// asserts that two sequential AuditLog.CreateWithTx calls in a single tx
// roll back atomically. Because audit logs are hash-chained (each record's
// prev_hash references the previous record in the same tenant), a partial
// rollback would fork the chain and break hashchain verification.
func TestIntegration_Governance_WithTxComposition_AuditLogChain_Rollback(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctx := h.Ctx()
		repo := auditLogRepo.NewRepository(h.Provider())

		first := newCompositionAuditLog(t, ctx, h.Seed.TenantID, "composition.first")
		second := newCompositionAuditLog(t, ctx, h.Seed.TenantID, "composition.second")

		_, txErr := pgcommon.WithTenantTxProvider(ctx, h.Provider(), func(tx *sql.Tx) (struct{}, error) {
			if _, err := repo.CreateWithTx(ctx, tx, first); err != nil {
				return struct{}{}, err
			}

			if _, err := repo.CreateWithTx(ctx, tx, second); err != nil {
				return struct{}{}, err
			}

			return struct{}{}, errDeliberateRollback
		})
		require.ErrorIs(t, txErr, errDeliberateRollback)

		// Both log entries — AND the hash chain sequence advancement — must be
		// undone after rollback.
		_, err := repo.GetByID(ctx, first.ID)
		require.Error(t, err, "first AuditLog must not persist after rollback")

		_, err = repo.GetByID(ctx, second.ID)
		require.Error(t, err, "second AuditLog must not persist after rollback")
	})
}

// TestIntegration_Governance_WithTxComposition_AuditLogChain_Commit is the
// commit counterpart — both log entries visible, and the chain is consistent.
func TestIntegration_Governance_WithTxComposition_AuditLogChain_Commit(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctx := h.Ctx()
		repo := auditLogRepo.NewRepository(h.Provider())

		first := newCompositionAuditLog(t, ctx, h.Seed.TenantID, "composition.commit.first")
		second := newCompositionAuditLog(t, ctx, h.Seed.TenantID, "composition.commit.second")

		_, txErr := pgcommon.WithTenantTxProvider(ctx, h.Provider(), func(tx *sql.Tx) (struct{}, error) {
			if _, err := repo.CreateWithTx(ctx, tx, first); err != nil {
				return struct{}{}, err
			}

			if _, err := repo.CreateWithTx(ctx, tx, second); err != nil {
				return struct{}{}, err
			}

			return struct{}{}, nil
		})
		require.NoError(t, txErr)

		fetchedFirst, err := repo.GetByID(ctx, first.ID)
		require.NoError(t, err)
		require.NotNil(t, fetchedFirst)

		fetchedSecond, err := repo.GetByID(ctx, second.ID)
		require.NoError(t, err)
		require.NotNil(t, fetchedSecond)

		// Sequence advances monotonically within a tenant.
		require.Greater(t, fetchedSecond.TenantSeq, fetchedFirst.TenantSeq,
			"second log entry must have a higher sequence than first")
	})
}

// TestIntegration_Governance_WithTxComposition_ArchiveMetadataCreateAndUpdate_Rollback
// asserts atomic rollback across ArchiveMetadata.CreateWithTx + UpdateWithTx
// on the same aggregate. Archive lifecycle transitions (PENDING → EXPORTING →
// EXPORTED) are auditor-visible; a partial rollback mid-transition would make
// the archive appear to have completed a phase it hasn't.
func TestIntegration_Governance_WithTxComposition_ArchiveMetadataCreateAndUpdate_Rollback(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctx := h.Ctx()
		repo := archiveRepo.NewRepository(h.Provider())

		partition := "composition_y2024m01_" + uuid.New().String()[:8]
		entity, err := governanceEntities.NewArchiveMetadata(
			ctx,
			h.Seed.TenantID,
			partition,
			time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			time.Date(2024, 2, 1, 0, 0, 0, 0, time.UTC),
		)
		require.NoError(t, err)

		_, txErr := pgcommon.WithTenantTxProvider(ctx, h.Provider(), func(tx *sql.Tx) (struct{}, error) {
			if err := repo.CreateWithTx(ctx, tx, entity); err != nil {
				return struct{}{}, err
			}

			if err := entity.MarkExporting(); err != nil {
				return struct{}{}, err
			}

			if err := repo.UpdateWithTx(ctx, tx, entity); err != nil {
				return struct{}{}, err
			}

			return struct{}{}, errDeliberateRollback
		})
		require.ErrorIs(t, txErr, errDeliberateRollback)

		_, err = repo.GetByID(ctx, entity.ID)
		require.Error(t, err,
			"ArchiveMetadata must not persist after Create+Update+rollback")
	})
}

// TestIntegration_Governance_WithTxComposition_ArchiveMetadataCreateAndUpdate_Commit
// is the commit counterpart — archive visible in EXPORTING status.
func TestIntegration_Governance_WithTxComposition_ArchiveMetadataCreateAndUpdate_Commit(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctx := h.Ctx()
		repo := archiveRepo.NewRepository(h.Provider())

		partition := "composition_commit_y2024m01_" + uuid.New().String()[:8]
		entity, err := governanceEntities.NewArchiveMetadata(
			ctx,
			h.Seed.TenantID,
			partition,
			time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			time.Date(2024, 2, 1, 0, 0, 0, 0, time.UTC),
		)
		require.NoError(t, err)

		_, txErr := pgcommon.WithTenantTxProvider(ctx, h.Provider(), func(tx *sql.Tx) (struct{}, error) {
			if err := repo.CreateWithTx(ctx, tx, entity); err != nil {
				return struct{}{}, err
			}

			if err := entity.MarkExporting(); err != nil {
				return struct{}{}, err
			}

			if err := repo.UpdateWithTx(ctx, tx, entity); err != nil {
				return struct{}{}, err
			}

			return struct{}{}, nil
		})
		require.NoError(t, txErr)

		found, err := repo.GetByID(ctx, entity.ID)
		require.NoError(t, err)
		require.NotNil(t, found)
		require.Equal(t, governanceEntities.StatusExporting, found.Status)
	})
}

// TestIntegration_Governance_WithTxComposition_ActorMappingUpsertAndDelete_Rollback
// asserts that UpsertWithTx + DeleteWithTx on the same ActorMapping roll back
// atomically. GDPR workflows (pseudonymize + erase) compose these; a partial
// rollback could leak PII that the user requested erased.
func TestIntegration_Governance_WithTxComposition_ActorMappingUpsertAndDelete_Rollback(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctx := h.Ctx()
		repo := actorMappingRepo.NewRepository(h.Provider())

		// Pre-create a mapping outside the composition tx so Delete has a target.
		seedActorID := "actor-seed-" + uuid.New().String()
		seedDisplayName := "Seed User"
		seedEmail := "seed@example.com"
		seedMapping, err := governanceEntities.NewActorMapping(ctx, seedActorID, &seedDisplayName, &seedEmail)
		require.NoError(t, err)
		_, err = repo.Upsert(ctx, seedMapping)
		require.NoError(t, err)

		// Inside the composition: Upsert a new mapping AND Delete the pre-existing one.
		newActorID := "actor-new-" + uuid.New().String()
		newDisplayName := "New User"
		newEmail := "new@example.com"
		newMapping, err := governanceEntities.NewActorMapping(ctx, newActorID, &newDisplayName, &newEmail)
		require.NoError(t, err)

		_, txErr := pgcommon.WithTenantTxProvider(ctx, h.Provider(), func(tx *sql.Tx) (struct{}, error) {
			if _, err := repo.UpsertWithTx(ctx, tx, newMapping); err != nil {
				return struct{}{}, err
			}

			if err := repo.DeleteWithTx(ctx, tx, seedActorID); err != nil {
				return struct{}{}, err
			}

			return struct{}{}, errDeliberateRollback
		})
		require.ErrorIs(t, txErr, errDeliberateRollback)

		// The upserted mapping must not exist (Upsert rolled back).
		foundNew, err := repo.GetByActorID(ctx, newActorID)
		require.Error(t, err, "new ActorMapping must not persist after Upsert+rollback")
		require.Nil(t, foundNew)

		// The pre-existing mapping must still exist (Delete rolled back).
		foundSeed, err := repo.GetByActorID(ctx, seedActorID)
		require.NoError(t, err,
			"pre-existing ActorMapping must survive Delete+rollback")
		require.NotNil(t, foundSeed)
		require.Equal(t, seedActorID, foundSeed.ActorID)
	})
}

// TestIntegration_Governance_WithTxComposition_ActorMappingUpsertAndDelete_Commit
// is the commit counterpart — new mapping visible, old mapping gone.
func TestIntegration_Governance_WithTxComposition_ActorMappingUpsertAndDelete_Commit(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctx := h.Ctx()
		repo := actorMappingRepo.NewRepository(h.Provider())

		seedActorID := "actor-seed-commit-" + uuid.New().String()
		seedDisplayName := "Seed User Commit"
		seedEmail := "seed-commit@example.com"
		seedMapping, err := governanceEntities.NewActorMapping(ctx, seedActorID, &seedDisplayName, &seedEmail)
		require.NoError(t, err)
		_, err = repo.Upsert(ctx, seedMapping)
		require.NoError(t, err)

		newActorID := "actor-new-commit-" + uuid.New().String()
		newDisplayName := "New User Commit"
		newEmail := "new-commit@example.com"
		newMapping, err := governanceEntities.NewActorMapping(ctx, newActorID, &newDisplayName, &newEmail)
		require.NoError(t, err)

		_, txErr := pgcommon.WithTenantTxProvider(ctx, h.Provider(), func(tx *sql.Tx) (struct{}, error) {
			if _, err := repo.UpsertWithTx(ctx, tx, newMapping); err != nil {
				return struct{}{}, err
			}

			if err := repo.DeleteWithTx(ctx, tx, seedActorID); err != nil {
				return struct{}{}, err
			}

			return struct{}{}, nil
		})
		require.NoError(t, txErr)

		foundNew, err := repo.GetByActorID(ctx, newActorID)
		require.NoError(t, err)
		require.NotNil(t, foundNew)

		_, err = repo.GetByActorID(ctx, seedActorID)
		require.Error(t, err,
			"pre-existing ActorMapping must be deleted after commit")
	})
}
