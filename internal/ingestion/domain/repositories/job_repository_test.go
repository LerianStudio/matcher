//go:build unit

package repositories

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	libHTTP "github.com/LerianStudio/lib-commons/v4/commons/net/http"

	"github.com/LerianStudio/matcher/internal/ingestion/domain/entities"
)

func TestCursorFilterDefaults(t *testing.T) {
	t.Parallel()

	filter := CursorFilter{}

	require.Equal(t, 0, filter.Limit)
	require.Empty(t, filter.Cursor)
	require.Empty(t, filter.SortBy)
	require.Empty(t, filter.SortOrder)
}

func TestCursorFilterWithValues(t *testing.T) {
	t.Parallel()

	filter := CursorFilter{
		Limit:     25,
		Cursor:    "abc123",
		SortBy:    "created_at",
		SortOrder: "DESC",
	}

	require.Equal(t, 25, filter.Limit)
	require.Equal(t, "abc123", filter.Cursor)
	require.Equal(t, "created_at", filter.SortBy)
	require.Equal(t, "DESC", filter.SortOrder)
}

func TestJobRepositoryInterfaceCompiles(t *testing.T) {
	t.Parallel()

	// This test verifies the interface compiles correctly.
	// The actual implementation is tested via integration tests.
	var _ JobRepository = (*mockJobRepository)(nil)
}

// mockJobRepository is a minimal mock to verify the interface.
type mockJobRepository struct{}

func (m *mockJobRepository) Create(
	_ context.Context,
	_ *entities.IngestionJob,
) (*entities.IngestionJob, error) {
	return nil, nil
}

func (m *mockJobRepository) FindByID(
	_ context.Context,
	_ uuid.UUID,
) (*entities.IngestionJob, error) {
	return nil, nil
}

func (m *mockJobRepository) FindByContextID(
	_ context.Context,
	_ uuid.UUID,
	_ CursorFilter,
) ([]*entities.IngestionJob, libHTTP.CursorPagination, error) {
	return nil, libHTTP.CursorPagination{}, nil
}

func (m *mockJobRepository) Update(
	_ context.Context,
	_ *entities.IngestionJob,
) (*entities.IngestionJob, error) {
	return nil, nil
}
