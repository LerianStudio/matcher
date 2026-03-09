//go:build unit

package repositories

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/discovery/domain/entities"
)

// ErrExtractionNotFound is a local sentinel for mock testing.
var errExtractionNotFound = errors.New("extraction request not found")

func TestExtractionRepositoryInterfaceCompiles(t *testing.T) {
	t.Parallel()

	var _ ExtractionRepository = (*mockExtractionRepository)(nil)
}

type mockExtractionRepository struct {
	extractions map[uuid.UUID]*entities.ExtractionRequest
}

func (m *mockExtractionRepository) Create(
	ctx context.Context,
	req *entities.ExtractionRequest,
) error {
	return m.CreateWithTx(ctx, nil, req)
}

func (m *mockExtractionRepository) CreateWithTx(
	_ context.Context,
	_ *sql.Tx,
	req *entities.ExtractionRequest,
) error {
	if m.extractions == nil {
		m.extractions = make(map[uuid.UUID]*entities.ExtractionRequest)
	}

	m.extractions[req.ID] = req

	return nil
}

func (m *mockExtractionRepository) Update(
	ctx context.Context,
	req *entities.ExtractionRequest,
) error {
	return m.UpdateWithTx(ctx, nil, req)
}

func (m *mockExtractionRepository) UpdateWithTx(
	_ context.Context,
	_ *sql.Tx,
	req *entities.ExtractionRequest,
) error {
	if m.extractions == nil {
		m.extractions = make(map[uuid.UUID]*entities.ExtractionRequest)
	}

	m.extractions[req.ID] = req

	return nil
}

func (m *mockExtractionRepository) FindByID(
	_ context.Context,
	id uuid.UUID,
) (*entities.ExtractionRequest, error) {
	if req, ok := m.extractions[id]; ok {
		return req, nil
	}

	return nil, errExtractionNotFound
}

func (m *mockExtractionRepository) FindByIngestionJobID(
	_ context.Context,
	jobID uuid.UUID,
) (*entities.ExtractionRequest, error) {
	for _, req := range m.extractions {
		if req.IngestionJobID == jobID {
			return req, nil
		}
	}

	return nil, errExtractionNotFound
}

func TestMockExtractionRepositoryOperations(t *testing.T) {
	t.Parallel()

	t.Run("Create stores extraction request", func(t *testing.T) {
		t.Parallel()

		repo := &mockExtractionRepository{}
		reqID := uuid.New()
		req := &entities.ExtractionRequest{
			ID: reqID,
		}

		err := repo.Create(context.Background(), req)
		require.NoError(t, err)

		found, err := repo.FindByID(context.Background(), reqID)
		require.NoError(t, err)
		assert.Equal(t, reqID, found.ID)
	})

	t.Run("FindByID returns error for missing request", func(t *testing.T) {
		t.Parallel()

		repo := &mockExtractionRepository{}

		_, err := repo.FindByID(context.Background(), uuid.New())
		assert.ErrorIs(t, err, errExtractionNotFound)
	})

	t.Run("Update modifies existing request", func(t *testing.T) {
		t.Parallel()

		repo := &mockExtractionRepository{}
		reqID := uuid.New()
		jobID := uuid.New()
		req := &entities.ExtractionRequest{
			ID:             reqID,
			IngestionJobID: jobID,
		}

		err := repo.Create(context.Background(), req)
		require.NoError(t, err)

		err = repo.Update(context.Background(), req)
		require.NoError(t, err)

		found, err := repo.FindByID(context.Background(), reqID)
		require.NoError(t, err)
		assert.Equal(t, jobID, found.IngestionJobID)
	})

	t.Run("FindByIngestionJobID retrieves by job ID", func(t *testing.T) {
		t.Parallel()

		repo := &mockExtractionRepository{}
		reqID := uuid.New()
		jobID := uuid.New()
		req := &entities.ExtractionRequest{
			ID:             reqID,
			IngestionJobID: jobID,
		}

		err := repo.Create(context.Background(), req)
		require.NoError(t, err)

		found, err := repo.FindByIngestionJobID(context.Background(), jobID)
		require.NoError(t, err)
		assert.Equal(t, reqID, found.ID)
	})

	t.Run("FindByIngestionJobID returns error for missing job", func(t *testing.T) {
		t.Parallel()

		repo := &mockExtractionRepository{}

		_, err := repo.FindByIngestionJobID(context.Background(), uuid.New())
		assert.ErrorIs(t, err, errExtractionNotFound)
	})
}
