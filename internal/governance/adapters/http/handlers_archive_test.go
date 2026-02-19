//go:build unit

package http

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	sharedhttp "github.com/LerianStudio/lib-uncommons/v2/uncommons/net/http"
	"github.com/LerianStudio/matcher/internal/auth"
	archivePostgres "github.com/LerianStudio/matcher/internal/governance/adapters/postgres/archive_metadata"
	"github.com/LerianStudio/matcher/internal/governance/domain/entities"
	repoMocks "github.com/LerianStudio/matcher/internal/governance/domain/repositories/mocks"
	storageMocks "github.com/LerianStudio/matcher/internal/reporting/ports/mocks"
)

var (
	errTestStorageFailed = errors.New("storage connection failed")
	errTestRepoFailed    = errors.New("repository connection failed")
	testPresignExpiry    = 15 * time.Minute
	testPresignedURL     = "https://s3.amazonaws.com/bucket/archive.gz?X-Amz-Signature=abc123"
)

func TestNewArchiveHandler(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	repo := repoMocks.NewMockArchiveMetadataRepository(ctrl)
	storage := storageMocks.NewMockObjectStorageClient(ctrl)

	t.Run("success", func(t *testing.T) {
		t.Parallel()

		handler, err := NewArchiveHandler(repo, storage, testPresignExpiry)
		require.NoError(t, err)
		require.NotNil(t, handler)
	})

	t.Run("nil repository", func(t *testing.T) {
		t.Parallel()

		handler, err := NewArchiveHandler(nil, storage, testPresignExpiry)
		require.ErrorIs(t, err, ErrArchiveRepoRequired)
		require.Nil(t, handler)
	})

	t.Run("nil storage client", func(t *testing.T) {
		t.Parallel()

		handler, err := NewArchiveHandler(repo, nil, testPresignExpiry)
		require.ErrorIs(t, err, ErrStorageClientRequired)
		require.Nil(t, handler)
	})

	t.Run("zero presign expiry", func(t *testing.T) {
		t.Parallel()

		handler, err := NewArchiveHandler(repo, storage, 0)
		require.ErrorIs(t, err, ErrPresignExpiryRequired)
		require.Nil(t, handler)
	})

	t.Run("negative presign expiry", func(t *testing.T) {
		t.Parallel()

		handler, err := NewArchiveHandler(repo, storage, -time.Minute)
		require.ErrorIs(t, err, ErrPresignExpiryRequired)
		require.Nil(t, handler)
	})
}

func TestListArchives(t *testing.T) {
	t.Parallel()

	t.Run("success returns paginated results", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		tenantID := uuid.New()
		now := time.Now().UTC()

		archives := createTestArchives(tenantID, 2, now)

		repo := repoMocks.NewMockArchiveMetadataRepository(ctrl)
		repo.EXPECT().
			ListByTenant(gomock.Any(), tenantID, entities.StatusComplete, (*time.Time)(nil), (*time.Time)(nil), sharedhttp.DefaultLimit+1, sharedhttp.DefaultOffset).
			Return(archives, nil)

		storage := storageMocks.NewMockObjectStorageClient(ctrl)

		handler, err := NewArchiveHandler(repo, storage, testPresignExpiry)
		require.NoError(t, err)

		ctx := createTestContextWithTenant(tenantID)
		resp := testListArchivesRequest(ctx, t, handler, "")

		defer resp.Body.Close()

		require.Equal(t, fiber.StatusOK, resp.StatusCode)

		var response ListArchivesResponse
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&response))
		require.Len(t, response.Items, 2)
		require.Equal(t, sharedhttp.DefaultLimit, response.Limit)
		assert.Equal(t, archives[0].ID.String(), response.Items[0].ID)
		assert.Equal(t, "COMPLETE", response.Items[0].Status)
		assert.False(t, response.HasMore)
	})

	t.Run("success with date filters", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		tenantID := uuid.New()

		repo := repoMocks.NewMockArchiveMetadataRepository(ctrl)
		repo.EXPECT().
			ListByTenant(gomock.Any(), tenantID, entities.StatusComplete, gomock.Not(gomock.Nil()), gomock.Not(gomock.Nil()), sharedhttp.DefaultLimit+1, sharedhttp.DefaultOffset).
			Return([]*entities.ArchiveMetadata{}, nil)

		storage := storageMocks.NewMockObjectStorageClient(ctrl)

		handler, err := NewArchiveHandler(repo, storage, testPresignExpiry)
		require.NoError(t, err)

		ctx := createTestContextWithTenant(tenantID)
		resp := testListArchivesRequest(ctx, t, handler, "from=2024-01-01&to=2024-03-31")

		defer resp.Body.Close()

		require.Equal(t, fiber.StatusOK, resp.StatusCode)

		var response ListArchivesResponse
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&response))
		require.Empty(t, response.Items)
		require.False(t, response.HasMore)
	})

	t.Run("success with RFC3339 date filters", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		tenantID := uuid.New()

		repo := repoMocks.NewMockArchiveMetadataRepository(ctrl)
		repo.EXPECT().
			ListByTenant(gomock.Any(), tenantID, entities.StatusComplete, gomock.Not(gomock.Nil()), gomock.Not(gomock.Nil()), sharedhttp.DefaultLimit+1, sharedhttp.DefaultOffset).
			Return([]*entities.ArchiveMetadata{}, nil)

		storage := storageMocks.NewMockObjectStorageClient(ctrl)

		handler, err := NewArchiveHandler(repo, storage, testPresignExpiry)
		require.NoError(t, err)

		ctx := createTestContextWithTenant(tenantID)
		resp := testListArchivesRequest(ctx, t, handler, "from=2024-01-01T00:00:00Z&to=2024-03-31T23:59:59Z")

		defer resp.Body.Close()

		require.Equal(t, fiber.StatusOK, resp.StatusCode)
	})

	t.Run("invalid from date", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		tenantID := uuid.New()

		repo := repoMocks.NewMockArchiveMetadataRepository(ctrl)
		storage := storageMocks.NewMockObjectStorageClient(ctrl)

		handler, err := NewArchiveHandler(repo, storage, testPresignExpiry)
		require.NoError(t, err)

		ctx := createTestContextWithTenant(tenantID)
		resp := testListArchivesRequest(ctx, t, handler, "from=not-a-date")

		defer resp.Body.Close()

		require.Equal(t, fiber.StatusBadRequest, resp.StatusCode)

		var errResp sharedhttp.ErrorResponse
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&errResp))
		require.Contains(t, errResp.Message, "from")
	})

	t.Run("invalid to date", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		tenantID := uuid.New()

		repo := repoMocks.NewMockArchiveMetadataRepository(ctrl)
		storage := storageMocks.NewMockObjectStorageClient(ctrl)

		handler, err := NewArchiveHandler(repo, storage, testPresignExpiry)
		require.NoError(t, err)

		ctx := createTestContextWithTenant(tenantID)
		resp := testListArchivesRequest(ctx, t, handler, "to=invalid")

		defer resp.Body.Close()

		require.Equal(t, fiber.StatusBadRequest, resp.StatusCode)

		var errResp sharedhttp.ErrorResponse
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&errResp))
		require.Contains(t, errResp.Message, "to")
	})

	t.Run("limit capped at max", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		tenantID := uuid.New()

		repo := repoMocks.NewMockArchiveMetadataRepository(ctrl)
		repo.EXPECT().
			ListByTenant(gomock.Any(), tenantID, entities.StatusComplete, (*time.Time)(nil), (*time.Time)(nil), sharedhttp.MaxLimit+1, sharedhttp.DefaultOffset).
			Return([]*entities.ArchiveMetadata{}, nil)

		storage := storageMocks.NewMockObjectStorageClient(ctrl)

		handler, err := NewArchiveHandler(repo, storage, testPresignExpiry)
		require.NoError(t, err)

		ctx := createTestContextWithTenant(tenantID)
		resp := testListArchivesRequest(ctx, t, handler, "limit=500")

		defer resp.Body.Close()

		require.Equal(t, fiber.StatusOK, resp.StatusCode)

		var response ListArchivesResponse
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&response))
		require.Equal(t, sharedhttp.MaxLimit, response.Limit)
	})

	t.Run("internal error", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		tenantID := uuid.New()

		repo := repoMocks.NewMockArchiveMetadataRepository(ctrl)
		repo.EXPECT().
			ListByTenant(gomock.Any(), tenantID, entities.StatusComplete, (*time.Time)(nil), (*time.Time)(nil), sharedhttp.DefaultLimit+1, sharedhttp.DefaultOffset).
			Return(nil, errTestRepoFailed)

		storage := storageMocks.NewMockObjectStorageClient(ctrl)

		handler, err := NewArchiveHandler(repo, storage, testPresignExpiry)
		require.NoError(t, err)

		ctx := createTestContextWithTenant(tenantID)
		resp := testListArchivesRequest(ctx, t, handler, "")

		defer resp.Body.Close()

		require.Equal(t, fiber.StatusInternalServerError, resp.StatusCode)
	})

	t.Run("empty result", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		tenantID := uuid.New()

		repo := repoMocks.NewMockArchiveMetadataRepository(ctrl)
		repo.EXPECT().
			ListByTenant(gomock.Any(), tenantID, entities.StatusComplete, (*time.Time)(nil), (*time.Time)(nil), sharedhttp.DefaultLimit+1, sharedhttp.DefaultOffset).
			Return([]*entities.ArchiveMetadata{}, nil)

		storage := storageMocks.NewMockObjectStorageClient(ctrl)

		handler, err := NewArchiveHandler(repo, storage, testPresignExpiry)
		require.NoError(t, err)

		ctx := createTestContextWithTenant(tenantID)
		resp := testListArchivesRequest(ctx, t, handler, "")

		defer resp.Body.Close()

		require.Equal(t, fiber.StatusOK, resp.StatusCode)

		var response ListArchivesResponse
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&response))
		require.Empty(t, response.Items)
		require.False(t, response.HasMore)
	})

	t.Run("has more when extra item returned", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		tenantID := uuid.New()
		now := time.Now().UTC()

		// Return 6 items for limit=5 (limit+1) to trigger hasMore=true
		archives := createTestArchives(tenantID, 6, now)

		repo := repoMocks.NewMockArchiveMetadataRepository(ctrl)
		repo.EXPECT().
			ListByTenant(gomock.Any(), tenantID, entities.StatusComplete, (*time.Time)(nil), (*time.Time)(nil), 6, sharedhttp.DefaultOffset).
			Return(archives, nil)

		storage := storageMocks.NewMockObjectStorageClient(ctrl)

		handler, err := NewArchiveHandler(repo, storage, testPresignExpiry)
		require.NoError(t, err)

		ctx := createTestContextWithTenant(tenantID)
		resp := testListArchivesRequest(ctx, t, handler, "limit=5")

		defer resp.Body.Close()

		require.Equal(t, fiber.StatusOK, resp.StatusCode)

		var response ListArchivesResponse
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&response))
		require.Len(t, response.Items, 5)
		require.True(t, response.HasMore)
	})

	t.Run("no more when fewer than limit returned", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		tenantID := uuid.New()
		now := time.Now().UTC()

		// Return exactly 5 items for limit=5 (fetched limit+1=6, got 5) => hasMore=false
		archives := createTestArchives(tenantID, 5, now)

		repo := repoMocks.NewMockArchiveMetadataRepository(ctrl)
		repo.EXPECT().
			ListByTenant(gomock.Any(), tenantID, entities.StatusComplete, (*time.Time)(nil), (*time.Time)(nil), 6, sharedhttp.DefaultOffset).
			Return(archives, nil)

		storage := storageMocks.NewMockObjectStorageClient(ctrl)

		handler, err := NewArchiveHandler(repo, storage, testPresignExpiry)
		require.NoError(t, err)

		ctx := createTestContextWithTenant(tenantID)
		resp := testListArchivesRequest(ctx, t, handler, "limit=5")

		defer resp.Body.Close()

		require.Equal(t, fiber.StatusOK, resp.StatusCode)

		var response ListArchivesResponse
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&response))
		require.Len(t, response.Items, 5)
		require.False(t, response.HasMore)
	})
}

func TestDownloadArchive(t *testing.T) {
	t.Parallel()

	t.Run("success returns presigned url", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		tenantID := uuid.New()
		archiveID := uuid.New()
		now := time.Now().UTC()

		archive := createTestArchiveMetadata(tenantID, archiveID, now)

		repo := repoMocks.NewMockArchiveMetadataRepository(ctrl)
		repo.EXPECT().GetByID(gomock.Any(), archiveID).Return(archive, nil)

		storage := storageMocks.NewMockObjectStorageClient(ctrl)
		storage.EXPECT().
			GeneratePresignedURL(gomock.Any(), archive.ArchiveKey, testPresignExpiry).
			Return(testPresignedURL, nil)

		handler, err := NewArchiveHandler(repo, storage, testPresignExpiry)
		require.NoError(t, err)

		ctx := createTestContextWithTenant(tenantID)
		resp := testDownloadArchiveRequest(ctx, t, handler, archiveID.String())

		defer resp.Body.Close()

		require.Equal(t, fiber.StatusOK, resp.StatusCode)

		var response ArchiveDownloadResponse
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&response))
		assert.Equal(t, testPresignedURL, response.DownloadURL)
		assert.Equal(t, archive.Checksum, response.Checksum)
		assert.NotEmpty(t, response.ExpiresAt)
	})

	t.Run("missing archive id", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		tenantID := uuid.New()

		repo := repoMocks.NewMockArchiveMetadataRepository(ctrl)
		storage := storageMocks.NewMockObjectStorageClient(ctrl)

		handler, err := NewArchiveHandler(repo, storage, testPresignExpiry)
		require.NoError(t, err)

		ctx := createTestContextWithTenant(tenantID)
		// Send request without ID -- Fiber returns 404 for missing path param
		resp := testDownloadArchiveRequest(ctx, t, handler, "")

		defer resp.Body.Close()

		require.Equal(t, fiber.StatusNotFound, resp.StatusCode)
	})

	t.Run("invalid uuid", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		tenantID := uuid.New()

		repo := repoMocks.NewMockArchiveMetadataRepository(ctrl)
		storage := storageMocks.NewMockObjectStorageClient(ctrl)

		handler, err := NewArchiveHandler(repo, storage, testPresignExpiry)
		require.NoError(t, err)

		ctx := createTestContextWithTenant(tenantID)
		resp := testDownloadArchiveRequest(ctx, t, handler, "not-a-uuid")

		defer resp.Body.Close()

		require.Equal(t, fiber.StatusBadRequest, resp.StatusCode)

		var errResp sharedhttp.ErrorResponse
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&errResp))
		require.Equal(t, "invalid archive id", errResp.Message)
	})

	t.Run("archive not found - error", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		tenantID := uuid.New()
		archiveID := uuid.New()

		repo := repoMocks.NewMockArchiveMetadataRepository(ctrl)
		repo.EXPECT().GetByID(gomock.Any(), archiveID).Return(nil, archivePostgres.ErrMetadataNotFound)

		storage := storageMocks.NewMockObjectStorageClient(ctrl)

		handler, err := NewArchiveHandler(repo, storage, testPresignExpiry)
		require.NoError(t, err)

		ctx := createTestContextWithTenant(tenantID)
		resp := testDownloadArchiveRequest(ctx, t, handler, archiveID.String())

		defer resp.Body.Close()

		verifyErrorResponse(t, resp, fiber.StatusNotFound, "archive not found")
	})

	t.Run("archive not found - nil result", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		tenantID := uuid.New()
		archiveID := uuid.New()

		repo := repoMocks.NewMockArchiveMetadataRepository(ctrl)
		repo.EXPECT().GetByID(gomock.Any(), archiveID).Return(nil, nil)

		storage := storageMocks.NewMockObjectStorageClient(ctrl)

		handler, err := NewArchiveHandler(repo, storage, testPresignExpiry)
		require.NoError(t, err)

		ctx := createTestContextWithTenant(tenantID)
		resp := testDownloadArchiveRequest(ctx, t, handler, archiveID.String())

		defer resp.Body.Close()

		verifyErrorResponse(t, resp, fiber.StatusNotFound, "archive not found")
	})

	t.Run("different tenant returns not found", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		requestTenantID := uuid.New()
		archiveTenantID := uuid.New() // Different tenant
		archiveID := uuid.New()
		now := time.Now().UTC()

		archive := createTestArchiveMetadata(archiveTenantID, archiveID, now)

		repo := repoMocks.NewMockArchiveMetadataRepository(ctrl)
		repo.EXPECT().GetByID(gomock.Any(), archiveID).Return(archive, nil)

		storage := storageMocks.NewMockObjectStorageClient(ctrl)

		handler, err := NewArchiveHandler(repo, storage, testPresignExpiry)
		require.NoError(t, err)

		ctx := createTestContextWithTenant(requestTenantID)
		resp := testDownloadArchiveRequest(ctx, t, handler, archiveID.String())

		defer resp.Body.Close()

		verifyErrorResponse(t, resp, fiber.StatusNotFound, "archive not found")
	})

	t.Run("internal error from repository", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		tenantID := uuid.New()
		archiveID := uuid.New()

		repo := repoMocks.NewMockArchiveMetadataRepository(ctrl)
		repo.EXPECT().GetByID(gomock.Any(), archiveID).Return(nil, errTestRepoFailed)

		storage := storageMocks.NewMockObjectStorageClient(ctrl)

		handler, err := NewArchiveHandler(repo, storage, testPresignExpiry)
		require.NoError(t, err)

		ctx := createTestContextWithTenant(tenantID)
		resp := testDownloadArchiveRequest(ctx, t, handler, archiveID.String())

		defer resp.Body.Close()

		require.Equal(t, fiber.StatusInternalServerError, resp.StatusCode)
	})

	t.Run("internal error from storage presign", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		tenantID := uuid.New()
		archiveID := uuid.New()
		now := time.Now().UTC()

		archive := createTestArchiveMetadata(tenantID, archiveID, now)

		repo := repoMocks.NewMockArchiveMetadataRepository(ctrl)
		repo.EXPECT().GetByID(gomock.Any(), archiveID).Return(archive, nil)

		storage := storageMocks.NewMockObjectStorageClient(ctrl)
		storage.EXPECT().
			GeneratePresignedURL(gomock.Any(), archive.ArchiveKey, testPresignExpiry).
			Return("", errTestStorageFailed)

		handler, err := NewArchiveHandler(repo, storage, testPresignExpiry)
		require.NoError(t, err)

		ctx := createTestContextWithTenant(tenantID)
		resp := testDownloadArchiveRequest(ctx, t, handler, archiveID.String())

		defer resp.Body.Close()

		require.Equal(t, fiber.StatusInternalServerError, resp.StatusCode)
	})
}

func TestArchiveMetadataToResponse(t *testing.T) {
	t.Parallel()

	t.Run("nil input returns empty response", func(t *testing.T) {
		t.Parallel()

		resp := archiveMetadataToResponse(nil)
		assert.Empty(t, resp.ID)
		assert.Empty(t, resp.PartitionName)
	})

	t.Run("full conversion with archived_at", func(t *testing.T) {
		t.Parallel()

		now := time.Now().UTC()
		archive := &entities.ArchiveMetadata{
			ID:                  uuid.MustParse("11111111-1111-1111-1111-111111111111"),
			TenantID:            uuid.MustParse("22222222-2222-2222-2222-222222222222"),
			PartitionName:       "audit_logs_2024_q1",
			DateRangeStart:      time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			DateRangeEnd:        time.Date(2024, 3, 31, 23, 59, 59, 0, time.UTC),
			RowCount:            150000,
			CompressedSizeBytes: 10485760,
			StorageClass:        "GLACIER",
			Status:              entities.StatusComplete,
			ArchivedAt:          &now,
		}

		resp := archiveMetadataToResponse(archive)
		assert.Equal(t, "11111111-1111-1111-1111-111111111111", resp.ID)
		assert.Equal(t, "audit_logs_2024_q1", resp.PartitionName)
		assert.Equal(t, "2024-01-01T00:00:00Z", resp.DateRangeStart)
		assert.Equal(t, "2024-03-31T23:59:59Z", resp.DateRangeEnd)
		assert.Equal(t, int64(150000), resp.RowCount)
		assert.Equal(t, int64(10485760), resp.CompressedSizeBytes)
		assert.Equal(t, "GLACIER", resp.StorageClass)
		assert.Equal(t, "COMPLETE", resp.Status)
		assert.NotNil(t, resp.ArchivedAt)
	})

	t.Run("conversion without archived_at", func(t *testing.T) {
		t.Parallel()

		archive := &entities.ArchiveMetadata{
			ID:             uuid.New(),
			PartitionName:  "audit_logs_2024_q2",
			DateRangeStart: time.Date(2024, 4, 1, 0, 0, 0, 0, time.UTC),
			DateRangeEnd:   time.Date(2024, 6, 30, 23, 59, 59, 0, time.UTC),
			Status:         entities.StatusPending,
		}

		resp := archiveMetadataToResponse(archive)
		assert.Nil(t, resp.ArchivedAt)
	})
}

func TestArchiveMetadataToResponses(t *testing.T) {
	t.Parallel()

	t.Run("nil slice returns empty slice", func(t *testing.T) {
		t.Parallel()

		result := archiveMetadataToResponses(nil)
		assert.NotNil(t, result)
		assert.Empty(t, result)
	})

	t.Run("filters nil elements", func(t *testing.T) {
		t.Parallel()

		archives := []*entities.ArchiveMetadata{
			{
				ID:             uuid.New(),
				PartitionName:  "part1",
				DateRangeStart: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
				DateRangeEnd:   time.Date(2024, 3, 31, 0, 0, 0, 0, time.UTC),
				Status:         entities.StatusComplete,
			},
			nil,
		}

		result := archiveMetadataToResponses(archives)
		assert.Len(t, result, 1)
	})
}

func TestArchiveSentinelErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		err     error
		message string
	}{
		{"ErrArchiveRepoRequired", ErrArchiveRepoRequired, "archive metadata repository is required"},
		{"ErrStorageClientRequired", ErrStorageClientRequired, "object storage client is required"},
		{"ErrPresignExpiryRequired", ErrPresignExpiryRequired, "presign expiry must be positive"},
		{"ErrMissingArchiveID", ErrMissingArchiveID, "archive id is required"},
		{"ErrInvalidArchiveID", ErrInvalidArchiveID, "archive id must be a valid UUID"},
		{"ErrArchiveDateFromInvalid", ErrArchiveDateFromInvalid, "from must be a valid date (YYYY-MM-DD or RFC3339)"},
		{"ErrArchiveDateToInvalid", ErrArchiveDateToInvalid, "to must be a valid date (YYYY-MM-DD or RFC3339)"},
		{"ErrArchiveHandlerRequired", ErrArchiveHandlerRequired, "archive handler is required"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			require.Error(t, tt.err)
			assert.Equal(t, tt.message, tt.err.Error())
		})
	}
}

func TestRegisterArchiveRoutes(t *testing.T) {
	t.Parallel()

	t.Run("success", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		repo := repoMocks.NewMockArchiveMetadataRepository(ctrl)
		storage := storageMocks.NewMockObjectStorageClient(ctrl)

		handler, err := NewArchiveHandler(repo, storage, testPresignExpiry)
		require.NoError(t, err)

		app := fiber.New()
		protectedCalled := false

		protected := func(resource, action string) fiber.Router {
			protectedCalled = true
			require.Equal(t, auth.ResourceGovernance, resource)
			require.Equal(t, auth.ActionArchiveRead, action)

			return app
		}

		err = RegisterArchiveRoutes(protected, handler)
		require.NoError(t, err)
		require.True(t, protectedCalled)
	})

	t.Run("nil protected helper", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		repo := repoMocks.NewMockArchiveMetadataRepository(ctrl)
		storage := storageMocks.NewMockObjectStorageClient(ctrl)

		handler, err := NewArchiveHandler(repo, storage, testPresignExpiry)
		require.NoError(t, err)

		err = RegisterArchiveRoutes(nil, handler)
		require.ErrorIs(t, err, ErrProtectedRouteHelperRequired)
	})

	t.Run("nil handler", func(t *testing.T) {
		t.Parallel()

		app := fiber.New()
		protected := func(_, _ string) fiber.Router {
			return app
		}

		err := RegisterArchiveRoutes(protected, nil)
		require.ErrorIs(t, err, ErrArchiveHandlerRequired)
	})
}

// --- Helpers ---

func testListArchivesRequest(
	ctx context.Context,
	t *testing.T,
	handler *ArchiveHandler,
	queryParams string,
) *http.Response {
	t.Helper()

	app := newFiberTestApp(ctx)
	app.Get("/v1/governance/archives", handler.ListArchives)

	url := "/v1/governance/archives"
	if queryParams != "" {
		url += "?" + queryParams
	}

	req := httptest.NewRequest(http.MethodGet, url, http.NoBody)
	resp, err := app.Test(req)
	require.NoError(t, err)

	return resp
}

func testDownloadArchiveRequest(
	ctx context.Context,
	t *testing.T,
	handler *ArchiveHandler,
	archiveID string,
) *http.Response {
	t.Helper()

	app := newFiberTestApp(ctx)
	app.Get("/v1/governance/archives/:id/download", handler.DownloadArchive)

	url := "/v1/governance/archives/"
	if archiveID != "" {
		url += archiveID + "/download"
	}

	req := httptest.NewRequest(http.MethodGet, url, http.NoBody)
	resp, err := app.Test(req)
	require.NoError(t, err)

	return resp
}

func createTestArchives(tenantID uuid.UUID, count int, baseTime time.Time) []*entities.ArchiveMetadata {
	archives := make([]*entities.ArchiveMetadata, count)

	for i := 0; i < count; i++ {
		archivedAt := baseTime.Add(-time.Duration(i) * time.Hour)
		archives[i] = &entities.ArchiveMetadata{
			ID:                  uuid.New(),
			TenantID:            tenantID,
			PartitionName:       fmt.Sprintf("audit_logs_2024_q%d", i+1),
			DateRangeStart:      time.Date(2024, time.Month(1+i*3), 1, 0, 0, 0, 0, time.UTC),
			DateRangeEnd:        time.Date(2024, time.Month(3+i*3), 31, 23, 59, 59, 0, time.UTC),
			RowCount:            int64(100000 + i*50000),
			ArchiveKey:          fmt.Sprintf("archives/tenant-%s/2024-q%d.gz", tenantID.String(), i+1),
			Checksum:            fmt.Sprintf("sha256:abc123def456%d", i),
			CompressedSizeBytes: int64(10485760 + i*5242880),
			StorageClass:        "GLACIER",
			Status:              entities.StatusComplete,
			ArchivedAt:          &archivedAt,
			CreatedAt:           baseTime,
			UpdatedAt:           baseTime,
		}
	}

	return archives
}

func createTestArchiveMetadata(tenantID, archiveID uuid.UUID, now time.Time) *entities.ArchiveMetadata {
	return &entities.ArchiveMetadata{
		ID:                  archiveID,
		TenantID:            tenantID,
		PartitionName:       "audit_logs_2024_q1",
		DateRangeStart:      time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
		DateRangeEnd:        time.Date(2024, 3, 31, 23, 59, 59, 0, time.UTC),
		RowCount:            150000,
		ArchiveKey:          "archives/tenant-" + tenantID.String() + "/2024-q1.gz",
		Checksum:            "sha256:abc123def456789",
		CompressedSizeBytes: 10485760,
		StorageClass:        "GLACIER",
		Status:              entities.StatusComplete,
		ArchivedAt:          &now,
		CreatedAt:           now,
		UpdatedAt:           now,
	}
}
