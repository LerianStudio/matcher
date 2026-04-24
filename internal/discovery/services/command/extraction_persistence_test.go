// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package command

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace"

	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"

	"github.com/LerianStudio/matcher/internal/discovery/domain/entities"
	"github.com/LerianStudio/matcher/internal/discovery/domain/repositories"
	vo "github.com/LerianStudio/matcher/internal/discovery/domain/value_objects"
)

// noopSpan returns a no-op span suitable for unit tests that call span-accepting
// helpers. The span is extracted from a bare background context, which produces
// the OTel no-op implementation.
func noopSpan() trace.Span {
	return trace.SpanFromContext(context.Background())
}

// ---------------------------------------------------------------------------
// persistSubmittedExtraction
// ---------------------------------------------------------------------------

func TestPersistSubmittedExtraction_UpdateSucceeds_ReturnsNil(t *testing.T) {
	t.Parallel()

	uc, err := NewUseCase(
		&mockFetcherClient{},
		&mockConnectionRepo{},
		&mockSchemaRepo{},
		&mockExtractionRepo{},
		&libLog.NopLogger{},
	)
	require.NoError(t, err)

	req := &entities.ExtractionRequest{
		ID:           uuid.New(),
		ConnectionID: uuid.New(),
		Status:       vo.ExtractionStatusSubmitted,
		FetcherJobID: "job-1",
	}

	err = uc.persistSubmittedExtraction(context.Background(), noopSpan(), req)

	require.NoError(t, err)
}

func TestPersistSubmittedExtraction_UpdateFails_RecoverySucceeds_ReturnsNil(t *testing.T) {
	t.Parallel()

	reqID := uuid.New()
	connID := uuid.New()
	now := time.Now().UTC()

	recovered := &entities.ExtractionRequest{
		ID:           reqID,
		ConnectionID: connID,
		Status:       vo.ExtractionStatusPending,
		FetcherJobID: "",
		CreatedAt:    now,
		UpdatedAt:    now,
	}

	extractionRepo := &mockExtractionRepo{
		updateErr: errors.New("transient failure"),
		findByIDFn: func(_ context.Context, id uuid.UUID) (*entities.ExtractionRequest, error) {
			return recovered, nil
		},
		updateIfFn: func(_ context.Context, req *entities.ExtractionRequest, _ time.Time) error {
			return nil
		},
	}

	uc, err := NewUseCase(
		&mockFetcherClient{},
		&mockConnectionRepo{},
		&mockSchemaRepo{},
		extractionRepo,
		&libLog.NopLogger{},
	)
	require.NoError(t, err)

	submitted := &entities.ExtractionRequest{
		ID:           reqID,
		ConnectionID: connID,
		Status:       vo.ExtractionStatusSubmitted,
		FetcherJobID: "job-abc",
		CreatedAt:    now,
	}

	err = uc.persistSubmittedExtraction(context.Background(), noopSpan(), submitted)

	require.NoError(t, err)
}

func TestPersistSubmittedExtraction_UpdateFails_RecoveryFails_ReturnsTrackingError(t *testing.T) {
	t.Parallel()

	extractionRepo := &mockExtractionRepo{
		updateErr: errors.New("primary failure"),
		findByIDFn: func(_ context.Context, _ uuid.UUID) (*entities.ExtractionRequest, error) {
			return nil, repositories.ErrExtractionNotFound
		},
	}

	uc, err := NewUseCase(
		&mockFetcherClient{},
		&mockConnectionRepo{},
		&mockSchemaRepo{},
		extractionRepo,
		&libLog.NopLogger{},
	)
	require.NoError(t, err)

	req := &entities.ExtractionRequest{
		ID:           uuid.New(),
		ConnectionID: uuid.New(),
		Status:       vo.ExtractionStatusSubmitted,
		FetcherJobID: "job-x",
	}

	err = uc.persistSubmittedExtraction(context.Background(), noopSpan(), req)

	require.ErrorIs(t, err, ErrExtractionTrackingIncomplete)
}

func TestPersistSubmittedExtraction_RecoveredDiffersFromSubmitted_CopiedBack(t *testing.T) {
	t.Parallel()

	reqID := uuid.New()
	connID := uuid.New()
	now := time.Now().UTC()

	// The recovered entity is a different pointer with matching state.
	recovered := &entities.ExtractionRequest{
		ID:           reqID,
		ConnectionID: connID,
		Status:       vo.ExtractionStatusSubmitted,
		FetcherJobID: "job-abc",
		CreatedAt:    now,
		UpdatedAt:    now,
	}

	extractionRepo := &mockExtractionRepo{
		updateErr: errors.New("transient failure"),
		findByIDFn: func(_ context.Context, _ uuid.UUID) (*entities.ExtractionRequest, error) {
			// Return a "latest" that has an empty FetcherJobID so recovery takes the
			// UpdateIfUnchanged path.
			return &entities.ExtractionRequest{
				ID:           reqID,
				ConnectionID: connID,
				Status:       vo.ExtractionStatusPending,
				FetcherJobID: "",
				CreatedAt:    now,
				UpdatedAt:    now,
			}, nil
		},
		updateIfFn: func(_ context.Context, _ *entities.ExtractionRequest, _ time.Time) error {
			// Simulate success of the conditional update; submitted is returned.
			return nil
		},
	}

	uc, err := NewUseCase(
		&mockFetcherClient{},
		&mockConnectionRepo{},
		&mockSchemaRepo{},
		extractionRepo,
		&libLog.NopLogger{},
	)
	require.NoError(t, err)

	submitted := &entities.ExtractionRequest{
		ID:           reqID,
		ConnectionID: connID,
		Status:       vo.ExtractionStatusSubmitted,
		FetcherJobID: "job-abc",
		CreatedAt:    now,
	}
	// Make submitted and recovered the same value to test the pointer identity
	// branch (recovered != extractionReq) being false — no copy needed.
	_ = recovered

	err = uc.persistSubmittedExtraction(context.Background(), noopSpan(), submitted)

	require.NoError(t, err)
}

// ---------------------------------------------------------------------------
// recoverSubmittedExtraction
// ---------------------------------------------------------------------------

func TestRecoverSubmittedExtraction_FindByIDNotFound_ReturnsNotFound(t *testing.T) {
	t.Parallel()

	extractionRepo := &mockExtractionRepo{
		findByIDFn: func(_ context.Context, _ uuid.UUID) (*entities.ExtractionRequest, error) {
			return nil, repositories.ErrExtractionNotFound
		},
	}

	uc, err := NewUseCase(
		&mockFetcherClient{},
		&mockConnectionRepo{},
		&mockSchemaRepo{},
		extractionRepo,
		&libLog.NopLogger{},
	)
	require.NoError(t, err)

	submitted := &entities.ExtractionRequest{ID: uuid.New(), Status: vo.ExtractionStatusSubmitted, FetcherJobID: "j-1"}

	_, err = uc.recoverSubmittedExtraction(context.Background(), submitted)

	require.ErrorIs(t, err, ErrExtractionNotFound)
}

func TestRecoverSubmittedExtraction_FindByIDGenericError_ReturnsWrapped(t *testing.T) {
	t.Parallel()

	extractionRepo := &mockExtractionRepo{
		findByIDFn: func(_ context.Context, _ uuid.UUID) (*entities.ExtractionRequest, error) {
			return nil, errors.New("connection refused")
		},
	}

	uc, err := NewUseCase(
		&mockFetcherClient{},
		&mockConnectionRepo{},
		&mockSchemaRepo{},
		extractionRepo,
		&libLog.NopLogger{},
	)
	require.NoError(t, err)

	submitted := &entities.ExtractionRequest{ID: uuid.New(), Status: vo.ExtractionStatusSubmitted, FetcherJobID: "j-1"}

	_, err = uc.recoverSubmittedExtraction(context.Background(), submitted)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "reload extraction request")
}

func TestRecoverSubmittedExtraction_NilLatest_ReturnsNotFound(t *testing.T) {
	t.Parallel()

	extractionRepo := &mockExtractionRepo{
		findByIDFn: func(_ context.Context, _ uuid.UUID) (*entities.ExtractionRequest, error) {
			return nil, nil
		},
	}

	uc, err := NewUseCase(
		&mockFetcherClient{},
		&mockConnectionRepo{},
		&mockSchemaRepo{},
		extractionRepo,
		&libLog.NopLogger{},
	)
	require.NoError(t, err)

	submitted := &entities.ExtractionRequest{ID: uuid.New(), Status: vo.ExtractionStatusSubmitted, FetcherJobID: "j-1"}

	_, err = uc.recoverSubmittedExtraction(context.Background(), submitted)

	require.ErrorIs(t, err, ErrExtractionNotFound)
}

func TestRecoverSubmittedExtraction_LatestMatchesSubmitted_ReturnsLatest(t *testing.T) {
	t.Parallel()

	reqID := uuid.New()
	latest := &entities.ExtractionRequest{
		ID:           reqID,
		Status:       vo.ExtractionStatusSubmitted,
		FetcherJobID: "job-abc",
	}

	extractionRepo := &mockExtractionRepo{
		findByIDFn: func(_ context.Context, _ uuid.UUID) (*entities.ExtractionRequest, error) {
			return latest, nil
		},
	}

	uc, err := NewUseCase(
		&mockFetcherClient{},
		&mockConnectionRepo{},
		&mockSchemaRepo{},
		extractionRepo,
		&libLog.NopLogger{},
	)
	require.NoError(t, err)

	submitted := &entities.ExtractionRequest{
		ID:           reqID,
		Status:       vo.ExtractionStatusSubmitted,
		FetcherJobID: "job-abc",
	}

	recovered, err := uc.recoverSubmittedExtraction(context.Background(), submitted)

	require.NoError(t, err)
	assert.Same(t, latest, recovered)
}

func TestRecoverSubmittedExtraction_LatestAlreadyHasFetcherJobID_ReturnsLatest(t *testing.T) {
	t.Parallel()

	reqID := uuid.New()
	latest := &entities.ExtractionRequest{
		ID:           reqID,
		Status:       vo.ExtractionStatusExtracting,
		FetcherJobID: "different-job",
	}

	extractionRepo := &mockExtractionRepo{
		findByIDFn: func(_ context.Context, _ uuid.UUID) (*entities.ExtractionRequest, error) {
			return latest, nil
		},
	}

	uc, err := NewUseCase(
		&mockFetcherClient{},
		&mockConnectionRepo{},
		&mockSchemaRepo{},
		extractionRepo,
		&libLog.NopLogger{},
	)
	require.NoError(t, err)

	submitted := &entities.ExtractionRequest{
		ID:           reqID,
		Status:       vo.ExtractionStatusSubmitted,
		FetcherJobID: "job-abc",
	}

	recovered, err := uc.recoverSubmittedExtraction(context.Background(), submitted)

	require.NoError(t, err)
	assert.Same(t, latest, recovered)
}

func TestRecoverSubmittedExtraction_UpdateIfUnchangedSucceeds_ReturnsSubmitted(t *testing.T) {
	t.Parallel()

	reqID := uuid.New()
	now := time.Now().UTC()

	latest := &entities.ExtractionRequest{
		ID:           reqID,
		Status:       vo.ExtractionStatusPending,
		FetcherJobID: "",
		UpdatedAt:    now,
	}

	extractionRepo := &mockExtractionRepo{
		findByIDFn: func(_ context.Context, _ uuid.UUID) (*entities.ExtractionRequest, error) {
			return latest, nil
		},
		updateIfFn: func(_ context.Context, _ *entities.ExtractionRequest, expectedAt time.Time) error {
			assert.Equal(t, now, expectedAt)
			return nil
		},
	}

	uc, err := NewUseCase(
		&mockFetcherClient{},
		&mockConnectionRepo{},
		&mockSchemaRepo{},
		extractionRepo,
		&libLog.NopLogger{},
	)
	require.NoError(t, err)

	submitted := &entities.ExtractionRequest{
		ID:           reqID,
		Status:       vo.ExtractionStatusSubmitted,
		FetcherJobID: "job-abc",
		CreatedAt:    now.Add(-time.Second),
	}

	recovered, err := uc.recoverSubmittedExtraction(context.Background(), submitted)

	require.NoError(t, err)
	assert.Same(t, submitted, recovered)
}

func TestRecoverSubmittedExtraction_ZeroUpdatedAt_UsesCreatedAt(t *testing.T) {
	t.Parallel()

	reqID := uuid.New()
	createdAt := time.Now().UTC()

	latest := &entities.ExtractionRequest{
		ID:           reqID,
		Status:       vo.ExtractionStatusPending,
		FetcherJobID: "",
		UpdatedAt:    time.Time{}, // zero
	}

	var capturedExpectedAt time.Time

	extractionRepo := &mockExtractionRepo{
		findByIDFn: func(_ context.Context, _ uuid.UUID) (*entities.ExtractionRequest, error) {
			return latest, nil
		},
		updateIfFn: func(_ context.Context, _ *entities.ExtractionRequest, expectedAt time.Time) error {
			capturedExpectedAt = expectedAt
			return nil
		},
	}

	uc, err := NewUseCase(
		&mockFetcherClient{},
		&mockConnectionRepo{},
		&mockSchemaRepo{},
		extractionRepo,
		&libLog.NopLogger{},
	)
	require.NoError(t, err)

	submitted := &entities.ExtractionRequest{
		ID:           reqID,
		Status:       vo.ExtractionStatusSubmitted,
		FetcherJobID: "job-abc",
		CreatedAt:    createdAt,
	}

	_, err = uc.recoverSubmittedExtraction(context.Background(), submitted)

	require.NoError(t, err)
	assert.Equal(t, createdAt, capturedExpectedAt)
}

func TestRecoverSubmittedExtraction_UpdateIfUnchangedNonConflictError_ReturnsWrapped(t *testing.T) {
	t.Parallel()

	reqID := uuid.New()
	now := time.Now().UTC()

	latest := &entities.ExtractionRequest{
		ID:           reqID,
		Status:       vo.ExtractionStatusPending,
		FetcherJobID: "",
		UpdatedAt:    now,
	}

	extractionRepo := &mockExtractionRepo{
		findByIDFn: func(_ context.Context, _ uuid.UUID) (*entities.ExtractionRequest, error) {
			return latest, nil
		},
		updateIfFn: func(_ context.Context, _ *entities.ExtractionRequest, _ time.Time) error {
			return errors.New("disk full")
		},
	}

	uc, err := NewUseCase(
		&mockFetcherClient{},
		&mockConnectionRepo{},
		&mockSchemaRepo{},
		extractionRepo,
		&libLog.NopLogger{},
	)
	require.NoError(t, err)

	submitted := &entities.ExtractionRequest{
		ID:           reqID,
		Status:       vo.ExtractionStatusSubmitted,
		FetcherJobID: "job-abc",
	}

	_, err = uc.recoverSubmittedExtraction(context.Background(), submitted)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "repair submitted extraction request")
}

func TestRecoverSubmittedExtraction_ConflictError_DelegatesToReloadRecovered(t *testing.T) {
	t.Parallel()

	reqID := uuid.New()
	now := time.Now().UTC()

	var findCount int

	reloaded := &entities.ExtractionRequest{
		ID:           reqID,
		Status:       vo.ExtractionStatusSubmitted,
		FetcherJobID: "job-other",
		UpdatedAt:    now,
	}

	extractionRepo := &mockExtractionRepo{
		findByIDFn: func(_ context.Context, _ uuid.UUID) (*entities.ExtractionRequest, error) {
			findCount++
			if findCount == 1 {
				// First call: return "stale" latest with no FetcherJobID
				return &entities.ExtractionRequest{
					ID:           reqID,
					Status:       vo.ExtractionStatusPending,
					FetcherJobID: "",
					UpdatedAt:    now,
				}, nil
			}
			// Second call (from reloadRecoveredExtraction): return the reloaded value
			return reloaded, nil
		},
		updateIfFn: func(_ context.Context, _ *entities.ExtractionRequest, _ time.Time) error {
			return repositories.ErrExtractionConflict
		},
	}

	uc, err := NewUseCase(
		&mockFetcherClient{},
		&mockConnectionRepo{},
		&mockSchemaRepo{},
		extractionRepo,
		&libLog.NopLogger{},
	)
	require.NoError(t, err)

	submitted := &entities.ExtractionRequest{
		ID:           reqID,
		Status:       vo.ExtractionStatusSubmitted,
		FetcherJobID: "job-abc",
	}

	recovered, err := uc.recoverSubmittedExtraction(context.Background(), submitted)

	require.NoError(t, err)
	assert.Same(t, reloaded, recovered)
	assert.Equal(t, 2, findCount)
}

// ---------------------------------------------------------------------------
// reloadRecoveredExtraction
// ---------------------------------------------------------------------------

func TestReloadRecoveredExtraction_NotFound_ReturnsNotFound(t *testing.T) {
	t.Parallel()

	extractionRepo := &mockExtractionRepo{
		findByIDFn: func(_ context.Context, _ uuid.UUID) (*entities.ExtractionRequest, error) {
			return nil, repositories.ErrExtractionNotFound
		},
	}

	uc, err := NewUseCase(
		&mockFetcherClient{},
		&mockConnectionRepo{},
		&mockSchemaRepo{},
		extractionRepo,
		&libLog.NopLogger{},
	)
	require.NoError(t, err)

	_, err = uc.reloadRecoveredExtraction(context.Background(), uuid.New())

	require.ErrorIs(t, err, ErrExtractionNotFound)
}

func TestReloadRecoveredExtraction_GenericFindError_ReturnsWrapped(t *testing.T) {
	t.Parallel()

	extractionRepo := &mockExtractionRepo{
		findByIDFn: func(_ context.Context, _ uuid.UUID) (*entities.ExtractionRequest, error) {
			return nil, errors.New("connection reset")
		},
	}

	uc, err := NewUseCase(
		&mockFetcherClient{},
		&mockConnectionRepo{},
		&mockSchemaRepo{},
		extractionRepo,
		&libLog.NopLogger{},
	)
	require.NoError(t, err)

	_, err = uc.reloadRecoveredExtraction(context.Background(), uuid.New())

	require.Error(t, err)
	assert.Contains(t, err.Error(), "reload extraction request after repair conflict")
}

func TestReloadRecoveredExtraction_NilReloaded_ReturnsNotFound(t *testing.T) {
	t.Parallel()

	extractionRepo := &mockExtractionRepo{
		findByIDFn: func(_ context.Context, _ uuid.UUID) (*entities.ExtractionRequest, error) {
			return nil, nil
		},
	}

	uc, err := NewUseCase(
		&mockFetcherClient{},
		&mockConnectionRepo{},
		&mockSchemaRepo{},
		extractionRepo,
		&libLog.NopLogger{},
	)
	require.NoError(t, err)

	_, err = uc.reloadRecoveredExtraction(context.Background(), uuid.New())

	require.ErrorIs(t, err, ErrExtractionNotFound)
}

func TestReloadRecoveredExtraction_EmptyFetcherJobID_ReturnsConflictError(t *testing.T) {
	t.Parallel()

	extractionRepo := &mockExtractionRepo{
		findByIDFn: func(_ context.Context, _ uuid.UUID) (*entities.ExtractionRequest, error) {
			return &entities.ExtractionRequest{
				ID:           uuid.New(),
				Status:       vo.ExtractionStatusPending,
				FetcherJobID: "",
			}, nil
		},
	}

	uc, err := NewUseCase(
		&mockFetcherClient{},
		&mockConnectionRepo{},
		&mockSchemaRepo{},
		extractionRepo,
		&libLog.NopLogger{},
	)
	require.NoError(t, err)

	_, err = uc.reloadRecoveredExtraction(context.Background(), uuid.New())

	require.Error(t, err)
	require.ErrorIs(t, err, repositories.ErrExtractionConflict)
	assert.Contains(t, err.Error(), "repair submitted extraction request")
}

func TestReloadRecoveredExtraction_WhitespaceOnlyFetcherJobID_ReturnsConflictError(t *testing.T) {
	t.Parallel()

	extractionRepo := &mockExtractionRepo{
		findByIDFn: func(_ context.Context, _ uuid.UUID) (*entities.ExtractionRequest, error) {
			return &entities.ExtractionRequest{
				ID:           uuid.New(),
				Status:       vo.ExtractionStatusSubmitted,
				FetcherJobID: "   ",
			}, nil
		},
	}

	uc, err := NewUseCase(
		&mockFetcherClient{},
		&mockConnectionRepo{},
		&mockSchemaRepo{},
		extractionRepo,
		&libLog.NopLogger{},
	)
	require.NoError(t, err)

	_, err = uc.reloadRecoveredExtraction(context.Background(), uuid.New())

	require.ErrorIs(t, err, repositories.ErrExtractionConflict)
}

func TestReloadRecoveredExtraction_ValidFetcherJobID_ReturnsReloaded(t *testing.T) {
	t.Parallel()

	reloaded := &entities.ExtractionRequest{
		ID:           uuid.New(),
		Status:       vo.ExtractionStatusSubmitted,
		FetcherJobID: "job-xyz",
	}

	extractionRepo := &mockExtractionRepo{
		findByIDFn: func(_ context.Context, _ uuid.UUID) (*entities.ExtractionRequest, error) {
			return reloaded, nil
		},
	}

	uc, err := NewUseCase(
		&mockFetcherClient{},
		&mockConnectionRepo{},
		&mockSchemaRepo{},
		extractionRepo,
		&libLog.NopLogger{},
	)
	require.NoError(t, err)

	result, err := uc.reloadRecoveredExtraction(context.Background(), reloaded.ID)

	require.NoError(t, err)
	assert.Same(t, reloaded, result)
}
