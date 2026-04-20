//go:build unit

package repositories

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

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

func (m *mockExtractionRepository) UpdateIfUnchanged(
	ctx context.Context,
	req *entities.ExtractionRequest,
	expectedUpdatedAt time.Time,
) error {
	return m.UpdateIfUnchangedWithTx(ctx, nil, req, expectedUpdatedAt)
}

func (m *mockExtractionRepository) UpdateIfUnchangedWithTx(
	_ context.Context,
	_ *sql.Tx,
	req *entities.ExtractionRequest,
	_ time.Time,
) error {
	if m.extractions == nil {
		m.extractions = make(map[uuid.UUID]*entities.ExtractionRequest)
	}

	m.extractions[req.ID] = req

	return nil
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

func (m *mockExtractionRepository) LinkIfUnlinked(
	_ context.Context,
	id uuid.UUID,
	ingestionJobID uuid.UUID,
) error {
	req, ok := m.extractions[id]
	if !ok {
		return errExtractionNotFound
	}

	req.IngestionJobID = ingestionJobID

	return nil
}

func (m *mockExtractionRepository) MarkBridgeFailed(
	_ context.Context,
	_ *entities.ExtractionRequest,
) error {
	return nil
}

func (m *mockExtractionRepository) MarkBridgeFailedWithTx(
	_ context.Context,
	_ *sql.Tx,
	_ *entities.ExtractionRequest,
) error {
	return nil
}

func (m *mockExtractionRepository) IncrementBridgeAttempts(
	_ context.Context,
	_ uuid.UUID,
	_ int,
) error {
	return nil
}

func (m *mockExtractionRepository) IncrementBridgeAttemptsWithTx(
	_ context.Context,
	_ *sql.Tx,
	_ uuid.UUID,
	_ int,
) error {
	return nil
}

func (m *mockExtractionRepository) FindEligibleForBridge(
	_ context.Context,
	_ int,
) ([]*entities.ExtractionRequest, error) {
	return nil, nil
}

func (m *mockExtractionRepository) CountBridgeReadiness(
	_ context.Context,
	_ time.Duration,
) (BridgeReadinessCounts, error) {
	return BridgeReadinessCounts{}, nil
}

func (m *mockExtractionRepository) ListBridgeCandidates(
	_ context.Context,
	_ string,
	_ time.Duration,
	_ time.Time,
	_ uuid.UUID,
	_ int,
) ([]*entities.ExtractionRequest, error) {
	return nil, nil
}

func (m *mockExtractionRepository) FindBridgeRetentionCandidates(
	_ context.Context,
	_ time.Duration,
	_ int,
) ([]*entities.ExtractionRequest, error) {
	return nil, nil
}

func (m *mockExtractionRepository) MarkCustodyDeleted(
	_ context.Context,
	_ uuid.UUID,
	_ time.Time,
) error {
	return nil
}

func (m *mockExtractionRepository) MarkCustodyDeletedWithTx(
	_ context.Context,
	_ *sql.Tx,
	_ uuid.UUID,
	_ time.Time,
) error {
	return nil
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
		req := &entities.ExtractionRequest{
			ID: reqID,
		}

		err := repo.Create(context.Background(), req)
		require.NoError(t, err)

		err = repo.Update(context.Background(), req)
		require.NoError(t, err)

		found, err := repo.FindByID(context.Background(), reqID)
		require.NoError(t, err)
		assert.Equal(t, reqID, found.ID)
	})
}
