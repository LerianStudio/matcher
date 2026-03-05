//go:build unit

package archivemetadata

import (
	"errors"
	"testing"

	pgcommon "github.com/LerianStudio/matcher/internal/shared/adapters/postgres/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestArchiveMetadataSentinelErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		err     error
		message string
	}{
		{"ErrRepositoryNotInitialized", ErrRepositoryNotInitialized, "archive metadata repository not initialized"},
		{"ErrMetadataRequired", ErrMetadataRequired, "archive metadata is required"},
		{"ErrIDRequired", ErrIDRequired, "id is required"},
		{"ErrTenantIDRequired", ErrTenantIDRequired, "tenant id is required"},
		{"ErrPartitionNameRequired", ErrPartitionNameRequired, "partition name is required"},
		{"ErrLimitMustBePositive", ErrLimitMustBePositive, "limit must be positive"},
		{"ErrNilScanner", ErrNilScanner, "nil scanner"},
		{"ErrTransactionRequired", ErrTransactionRequired, "transaction is required"},
		{"ErrInvalidArchiveStatus", ErrInvalidArchiveStatus, "invalid archive status from database"},
		{"ErrMetadataNotFound", ErrMetadataNotFound, "archive metadata not found"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			require.Error(t, tt.err)
			assert.Equal(t, tt.message, tt.err.Error())
		})
	}
}

func TestArchiveMetadataErrorsAreDifferent(t *testing.T) {
	t.Parallel()

	require.NotErrorIs(t, ErrRepositoryNotInitialized, ErrMetadataRequired)
	require.NotErrorIs(t, ErrMetadataRequired, ErrIDRequired)
	require.NotErrorIs(t, ErrIDRequired, ErrTenantIDRequired)
	require.NotErrorIs(t, ErrTenantIDRequired, ErrPartitionNameRequired)
	require.NotErrorIs(t, ErrPartitionNameRequired, ErrLimitMustBePositive)
	require.NotErrorIs(t, ErrLimitMustBePositive, ErrNilScanner)
	require.NotErrorIs(t, ErrNilScanner, ErrTransactionRequired)
	require.NotErrorIs(t, ErrTransactionRequired, ErrInvalidArchiveStatus)
}

func TestErrTransactionRequired_CanonicalIdentity(t *testing.T) {
	t.Parallel()

	require.True(t, errors.Is(ErrTransactionRequired, pgcommon.ErrTransactionRequired))
}
