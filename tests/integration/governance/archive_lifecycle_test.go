//go:build integration

package governance

import (
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	archiveRepo "github.com/LerianStudio/matcher/internal/governance/adapters/postgres/archive_metadata"
	"github.com/LerianStudio/matcher/internal/governance/domain/entities"
	"github.com/LerianStudio/matcher/tests/integration"
)

// newTestArchiveMetadata creates a valid ArchiveMetadata entity in PENDING status
// bound to the harness tenant. Each call produces a unique partition name to avoid
// collisions between tests sharing the same database.
func newTestArchiveMetadata(t *testing.T, h *integration.TestHarness) *entities.ArchiveMetadata {
	t.Helper()

	ctx := h.Ctx()
	partitionName := "audit_log_y2024m01_" + uuid.New().String()[:8]
	rangeStart := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	rangeEnd := time.Date(2024, 2, 1, 0, 0, 0, 0, time.UTC)

	am, err := entities.NewArchiveMetadata(ctx, h.Seed.TenantID, partitionName, rangeStart, rangeEnd)
	require.NoError(t, err)

	return am
}

func TestArchiveMetadata_CreateAndFindByID(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		repo := archiveRepo.NewRepository(h.Provider())
		ctx := h.Ctx()

		am := newTestArchiveMetadata(t, h)

		// Persist
		err := repo.Create(ctx, am)
		require.NoError(t, err)

		// Retrieve
		fetched, err := repo.GetByID(ctx, am.ID)
		require.NoError(t, err)

		// Verify identity & tenant
		require.Equal(t, am.ID, fetched.ID)
		require.Equal(t, am.TenantID, fetched.TenantID)

		// Verify domain fields
		require.Equal(t, am.PartitionName, fetched.PartitionName)
		require.True(t, am.DateRangeStart.Equal(fetched.DateRangeStart),
			"DateRangeStart mismatch: want %v, got %v", am.DateRangeStart, fetched.DateRangeStart)
		require.True(t, am.DateRangeEnd.Equal(fetched.DateRangeEnd),
			"DateRangeEnd mismatch: want %v, got %v", am.DateRangeEnd, fetched.DateRangeEnd)

		// Initial state defaults
		require.Equal(t, entities.StatusPending, fetched.Status)
		require.Zero(t, fetched.RowCount)
		require.Empty(t, fetched.ArchiveKey)
		require.Empty(t, fetched.Checksum)
		require.Zero(t, fetched.CompressedSizeBytes)
		require.Empty(t, fetched.StorageClass)
		require.Empty(t, fetched.ErrorMessage)
		require.Nil(t, fetched.ArchivedAt)

		// Timestamps
		require.False(t, fetched.CreatedAt.IsZero())
		require.False(t, fetched.UpdatedAt.IsZero())
	})
}

func TestArchiveMetadata_StatusTransitions(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		repo := archiveRepo.NewRepository(h.Provider())
		ctx := h.Ctx()

		am := newTestArchiveMetadata(t, h)

		err := repo.Create(ctx, am)
		require.NoError(t, err)

		// Helper that transitions in-memory, persists via Update, then verifies via GetByID.
		verifyTransition := func(transitionFn func() error, expectedStatus entities.ArchiveStatus) {
			t.Helper()

			require.NoError(t, transitionFn())
			require.NoError(t, repo.Update(ctx, am))

			fetched, err := repo.GetByID(ctx, am.ID)
			require.NoError(t, err)
			require.Equal(t, expectedStatus, fetched.Status)
		}

		// PENDING -> EXPORTING
		verifyTransition(am.MarkExporting, entities.StatusExporting)

		// EXPORTING -> EXPORTED (with row count)
		verifyTransition(func() error {
			return am.MarkExported(42_000)
		}, entities.StatusExported)

		// Verify row count was persisted
		fetched, err := repo.GetByID(ctx, am.ID)
		require.NoError(t, err)
		require.Equal(t, int64(42_000), fetched.RowCount)

		// EXPORTED -> UPLOADING
		verifyTransition(am.MarkUploading, entities.StatusUploading)

		// UPLOADING -> UPLOADED (with archive details)
		archiveKey := "s3://bucket/archive/2024-01.gz"
		checksum := "sha256:deadbeef"
		compressedSize := int64(2 * 1024 * 1024)
		storageClass := "GLACIER"

		verifyTransition(func() error {
			return am.MarkUploaded(archiveKey, checksum, compressedSize, storageClass)
		}, entities.StatusUploaded)

		// Verify archive details were persisted
		fetched, err = repo.GetByID(ctx, am.ID)
		require.NoError(t, err)
		require.Equal(t, archiveKey, fetched.ArchiveKey)
		require.Equal(t, checksum, fetched.Checksum)
		require.Equal(t, compressedSize, fetched.CompressedSizeBytes)
		require.Equal(t, storageClass, fetched.StorageClass)

		// UPLOADED -> VERIFYING
		verifyTransition(am.MarkVerifying, entities.StatusVerifying)

		// VERIFYING -> VERIFIED
		verifyTransition(am.MarkVerified, entities.StatusVerified)

		// VERIFIED -> DETACHING
		verifyTransition(am.MarkDetaching, entities.StatusDetaching)

		// DETACHING -> COMPLETE
		verifyTransition(am.MarkComplete, entities.StatusComplete)

		// Verify ArchivedAt is set upon completion
		fetched, err = repo.GetByID(ctx, am.ID)
		require.NoError(t, err)
		require.NotNil(t, fetched.ArchivedAt, "ArchivedAt must be set when status reaches COMPLETE")
		require.Equal(t, entities.StatusComplete, fetched.Status)
	})
}

func TestArchiveMetadata_ListByStatus(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		repo := archiveRepo.NewRepository(h.Provider())
		ctx := h.Ctx()

		// Create 3 archives: one PENDING, one EXPORTING, one EXPORTED.
		pending := newTestArchiveMetadata(t, h)
		require.NoError(t, repo.Create(ctx, pending))

		exporting := newTestArchiveMetadata(t, h)
		require.NoError(t, repo.Create(ctx, exporting))
		require.NoError(t, exporting.MarkExporting())
		require.NoError(t, repo.Update(ctx, exporting))

		exported := newTestArchiveMetadata(t, h)
		require.NoError(t, repo.Create(ctx, exported))
		require.NoError(t, exported.MarkExporting())
		require.NoError(t, exported.MarkExported(100))
		require.NoError(t, repo.Update(ctx, exported))

		// List by PENDING — expect exactly 1
		pendingList, err := repo.ListByTenant(ctx, h.Seed.TenantID, entities.StatusPending, nil, nil, 100, 0)
		require.NoError(t, err)
		require.Len(t, pendingList, 1)
		require.Equal(t, pending.ID, pendingList[0].ID)

		// List by EXPORTING — expect exactly 1
		exportingList, err := repo.ListByTenant(ctx, h.Seed.TenantID, entities.StatusExporting, nil, nil, 100, 0)
		require.NoError(t, err)
		require.Len(t, exportingList, 1)
		require.Equal(t, exporting.ID, exportingList[0].ID)

		// List by EXPORTED — expect exactly 1
		exportedList, err := repo.ListByTenant(ctx, h.Seed.TenantID, entities.StatusExported, nil, nil, 100, 0)
		require.NoError(t, err)
		require.Len(t, exportedList, 1)
		require.Equal(t, exported.ID, exportedList[0].ID)

		// List ALL (no status filter) — expect 3
		allList, err := repo.ListByTenant(ctx, h.Seed.TenantID, "", nil, nil, 100, 0)
		require.NoError(t, err)
		require.Len(t, allList, 3)

		// List by a status that has no records — expect empty
		completeList, err := repo.ListByTenant(ctx, h.Seed.TenantID, entities.StatusComplete, nil, nil, 100, 0)
		require.NoError(t, err)
		require.Empty(t, completeList)
	})
}

func TestArchiveMetadata_InvalidTransition(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		am := newTestArchiveMetadata(t, h)
		require.Equal(t, entities.StatusPending, am.Status)

		// PENDING -> COMPLETE (skip all intermediate states): must fail
		err := am.MarkComplete()
		require.Error(t, err)
		require.True(t, errors.Is(err, entities.ErrInvalidStateTransition),
			"expected ErrInvalidStateTransition, got: %v", err)
		require.Equal(t, entities.StatusPending, am.Status, "status must remain PENDING after rejected transition")

		// PENDING -> UPLOADED (skip EXPORTING, EXPORTED, UPLOADING): must fail
		err = am.MarkUploaded("key", "checksum", 100, "STANDARD")
		require.Error(t, err)
		require.True(t, errors.Is(err, entities.ErrInvalidStateTransition),
			"expected ErrInvalidStateTransition, got: %v", err)

		// PENDING -> VERIFIED: must fail
		err = am.MarkVerified()
		require.Error(t, err)
		require.True(t, errors.Is(err, entities.ErrInvalidStateTransition),
			"expected ErrInvalidStateTransition, got: %v", err)

		// PENDING -> DETACHING: must fail
		err = am.MarkDetaching()
		require.Error(t, err)
		require.True(t, errors.Is(err, entities.ErrInvalidStateTransition),
			"expected ErrInvalidStateTransition, got: %v", err)

		// Valid transition to EXPORTING, then attempt to skip to VERIFIED
		require.NoError(t, am.MarkExporting())
		err = am.MarkVerified()
		require.Error(t, err)
		require.True(t, errors.Is(err, entities.ErrInvalidStateTransition),
			"expected ErrInvalidStateTransition from EXPORTING -> VERIFIED, got: %v", err)
		require.Equal(t, entities.StatusExporting, am.Status, "status must remain EXPORTING after rejected transition")
	})
}

func TestArchiveMetadata_UpdateMetadata(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		repo := archiveRepo.NewRepository(h.Provider())
		ctx := h.Ctx()

		am := newTestArchiveMetadata(t, h)

		require.NoError(t, repo.Create(ctx, am))

		// Transition through to UPLOADED so we can set archive details
		require.NoError(t, am.MarkExporting())
		require.NoError(t, repo.Update(ctx, am))

		require.NoError(t, am.MarkExported(10_000))
		require.NoError(t, repo.Update(ctx, am))

		require.NoError(t, am.MarkUploading())
		require.NoError(t, repo.Update(ctx, am))

		archiveKey := "s3://my-bucket/archives/tenant/2024-01.tar.gz"
		checksum := "sha256:e3b0c44298fc1c149afbf4c8996fb924"
		compressedSize := int64(5 * 1024 * 1024)
		storageClass := "STANDARD"

		require.NoError(t, am.MarkUploaded(archiveKey, checksum, compressedSize, storageClass))
		require.NoError(t, repo.Update(ctx, am))

		// Verify persisted archive details
		fetched, err := repo.GetByID(ctx, am.ID)
		require.NoError(t, err)
		require.Equal(t, entities.StatusUploaded, fetched.Status)
		require.Equal(t, archiveKey, fetched.ArchiveKey)
		require.Equal(t, checksum, fetched.Checksum)
		require.Equal(t, compressedSize, fetched.CompressedSizeBytes)
		require.Equal(t, storageClass, fetched.StorageClass)
		require.Equal(t, int64(10_000), fetched.RowCount)

		// Record an error message and verify persistence
		am.MarkError("upload verification failed: checksum mismatch")
		require.NoError(t, repo.Update(ctx, am))

		fetched, err = repo.GetByID(ctx, am.ID)
		require.NoError(t, err)
		require.Equal(t, "upload verification failed: checksum mismatch", fetched.ErrorMessage)
		// Status remains UPLOADED — MarkError doesn't change status
		require.Equal(t, entities.StatusUploaded, fetched.Status)

		// Continue the lifecycle to COMPLETE and verify all fields persist
		transitionToComplete := func() {
			require.NoError(t, am.MarkVerifying())
			require.NoError(t, repo.Update(ctx, am))
			require.NoError(t, am.MarkVerified())
			require.NoError(t, repo.Update(ctx, am))
			require.NoError(t, am.MarkDetaching())
			require.NoError(t, repo.Update(ctx, am))
			require.NoError(t, am.MarkComplete())
			require.NoError(t, repo.Update(ctx, am))
		}
		transitionToComplete()

		final, err := repo.GetByID(ctx, am.ID)
		require.NoError(t, err)
		require.Equal(t, entities.StatusComplete, final.Status)
		require.NotNil(t, final.ArchivedAt)
		require.Equal(t, archiveKey, final.ArchiveKey)
		require.Equal(t, checksum, final.Checksum)
		require.Equal(t, compressedSize, final.CompressedSizeBytes)
		require.Equal(t, storageClass, final.StorageClass)
		require.Equal(t, int64(10_000), final.RowCount)
	})
}
