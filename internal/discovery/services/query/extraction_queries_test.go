//go:build unit

package query

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/discovery/domain/entities"
	"github.com/LerianStudio/matcher/internal/discovery/domain/repositories"
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

type mockExtractionRepoForQuery struct {
	findByIDFn func(ctx context.Context, id uuid.UUID) (*entities.ExtractionRequest, error)
}

func (m *mockExtractionRepoForQuery) FindByID(ctx context.Context, id uuid.UUID) (*entities.ExtractionRequest, error) {
	if m.findByIDFn != nil {
		return m.findByIDFn(ctx, id)
	}

	return nil, nil
}

func (m *mockExtractionRepoForQuery) Create(_ context.Context, _ *entities.ExtractionRequest) error {
	return nil
}

func (m *mockExtractionRepoForQuery) CreateWithTx(_ context.Context, _ sharedPorts.Tx, _ *entities.ExtractionRequest) error {
	return nil
}

func (m *mockExtractionRepoForQuery) Update(_ context.Context, _ *entities.ExtractionRequest) error {
	return nil
}

func (m *mockExtractionRepoForQuery) UpdateIfUnchanged(_ context.Context, _ *entities.ExtractionRequest, _ time.Time) error {
	return nil
}

func (m *mockExtractionRepoForQuery) UpdateIfUnchangedWithTx(_ context.Context, _ sharedPorts.Tx, _ *entities.ExtractionRequest, _ time.Time) error {
	return nil
}

func (m *mockExtractionRepoForQuery) UpdateWithTx(_ context.Context, _ sharedPorts.Tx, _ *entities.ExtractionRequest) error {
	return nil
}

func (m *mockExtractionRepoForQuery) LinkIfUnlinked(_ context.Context, _ uuid.UUID, _ uuid.UUID) error {
	return nil
}

func (m *mockExtractionRepoForQuery) FindEligibleForBridge(_ context.Context, _ int) ([]*entities.ExtractionRequest, error) {
	return nil, nil
}

func (m *mockExtractionRepoForQuery) CountBridgeReadiness(_ context.Context, _ time.Duration) (repositories.BridgeReadinessCounts, error) {
	return repositories.BridgeReadinessCounts{}, nil
}

func (m *mockExtractionRepoForQuery) ListBridgeCandidates(
	_ context.Context,
	_ string,
	_ time.Duration,
	_ time.Time,
	_ uuid.UUID,
	_ int,
) ([]*entities.ExtractionRequest, error) {
	return nil, nil
}

func TestGetExtraction_Success(t *testing.T) {
	t.Parallel()

	extractionID := uuid.New()
	expected := &entities.ExtractionRequest{ID: extractionID}

	repo := &mockExtractionRepoForQuery{
		findByIDFn: func(_ context.Context, id uuid.UUID) (*entities.ExtractionRequest, error) {
			assert.Equal(t, extractionID, id)
			return expected, nil
		},
	}

	uc := &UseCase{extractionRepo: repo}

	result, err := uc.GetExtraction(context.Background(), extractionID)

	require.NoError(t, err)
	assert.Equal(t, expected, result)
}

func TestGetExtraction_NotFound_ReturnsError(t *testing.T) {
	t.Parallel()

	repo := &mockExtractionRepoForQuery{
		findByIDFn: func(_ context.Context, _ uuid.UUID) (*entities.ExtractionRequest, error) {
			return nil, repositories.ErrExtractionNotFound
		},
	}

	uc := &UseCase{extractionRepo: repo}

	result, err := uc.GetExtraction(context.Background(), uuid.New())

	assert.Error(t, err)
	assert.Nil(t, result)
	assert.True(t, errors.Is(err, ErrExtractionNotFound))
}

func TestGetExtraction_NilResult_ReturnsNotFound(t *testing.T) {
	t.Parallel()

	repo := &mockExtractionRepoForQuery{
		findByIDFn: func(_ context.Context, _ uuid.UUID) (*entities.ExtractionRequest, error) {
			return nil, nil
		},
	}

	uc := &UseCase{extractionRepo: repo}

	result, err := uc.GetExtraction(context.Background(), uuid.New())

	assert.Error(t, err)
	assert.Nil(t, result)
	assert.True(t, errors.Is(err, ErrExtractionNotFound))
}

func TestGetExtraction_RepoError_PropagatesWrapped(t *testing.T) {
	t.Parallel()

	repoErr := errors.New("database error")

	repo := &mockExtractionRepoForQuery{
		findByIDFn: func(_ context.Context, _ uuid.UUID) (*entities.ExtractionRequest, error) {
			return nil, repoErr
		},
	}

	uc := &UseCase{extractionRepo: repo}

	result, err := uc.GetExtraction(context.Background(), uuid.New())

	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "get extraction")
}
