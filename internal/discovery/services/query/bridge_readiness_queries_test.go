// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

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
	vo "github.com/LerianStudio/matcher/internal/discovery/domain/value_objects"
)

// readinessRepoStub is a focused mock that only implements the methods used
// by the bridge-readiness queries; the rest of the ExtractionRepository
// surface delegates to the existing mockExtractionRepoForQuery so we share
// one interface implementation per file.
type readinessRepoStub struct {
	mockExtractionRepoForQuery

	countFn func(ctx context.Context, threshold time.Duration) (repositories.BridgeReadinessCounts, error)
	listFn  func(ctx context.Context, state string, threshold time.Duration, ca time.Time, ia uuid.UUID, limit int) ([]*entities.ExtractionRequest, error)
}

func (r *readinessRepoStub) CountBridgeReadiness(ctx context.Context, threshold time.Duration) (repositories.BridgeReadinessCounts, error) {
	if r.countFn != nil {
		return r.countFn(ctx, threshold)
	}

	return repositories.BridgeReadinessCounts{}, nil
}

func (r *readinessRepoStub) ListBridgeCandidates(
	ctx context.Context,
	state string,
	threshold time.Duration,
	ca time.Time,
	ia uuid.UUID,
	limit int,
) ([]*entities.ExtractionRequest, error) {
	if r.listFn != nil {
		return r.listFn(ctx, state, threshold, ca, ia, limit)
	}

	return nil, nil
}

// Compile-time assertion that the stub satisfies the full interface.
var _ repositories.ExtractionRepository = (*readinessRepoStub)(nil)

func TestCountBridgeReadinessByTenant_HappyPath(t *testing.T) {
	t.Parallel()

	want := repositories.BridgeReadinessCounts{
		Ready: 10, Pending: 2, Stale: 1, Failed: 4,
	}

	repo := &readinessRepoStub{
		countFn: func(_ context.Context, threshold time.Duration) (repositories.BridgeReadinessCounts, error) {
			assert.Equal(t, 30*time.Minute, threshold)
			return want, nil
		},
	}

	uc := &UseCase{extractionRepo: repo}

	summary, err := uc.CountBridgeReadinessByTenant(context.Background(), 30*time.Minute)
	require.NoError(t, err)
	require.NotNil(t, summary)

	assert.Equal(t, want, summary.Counts)
	assert.Equal(t, 30*time.Minute, summary.StaleThreshold)
	assert.WithinDuration(t, time.Now().UTC(), summary.GeneratedAt, 5*time.Second)
}

func TestCountBridgeReadinessByTenant_NegativeThresholdRejected(t *testing.T) {
	t.Parallel()

	uc := &UseCase{extractionRepo: &readinessRepoStub{}}

	_, err := uc.CountBridgeReadinessByTenant(context.Background(), -time.Second)
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrReadinessThresholdInvalid))
}

func TestCountBridgeReadinessByTenant_NilUseCase(t *testing.T) {
	t.Parallel()

	var uc *UseCase
	_, err := uc.CountBridgeReadinessByTenant(context.Background(), time.Hour)
	assert.ErrorIs(t, err, ErrNilBridgeReadinessUseCase)
}

func TestCountBridgeReadinessByTenant_RepoError(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("db down")
	repo := &readinessRepoStub{
		countFn: func(_ context.Context, _ time.Duration) (repositories.BridgeReadinessCounts, error) {
			return repositories.BridgeReadinessCounts{}, wantErr
		},
	}

	uc := &UseCase{extractionRepo: repo}

	_, err := uc.CountBridgeReadinessByTenant(context.Background(), time.Hour)
	require.Error(t, err)
	assert.True(t, errors.Is(err, wantErr))
}

func TestListBridgeCandidates_HappyPath_DerivesAge(t *testing.T) {
	t.Parallel()

	createdAt := time.Now().UTC().Add(-90 * time.Second)
	row := &entities.ExtractionRequest{
		ID:           uuid.New(),
		ConnectionID: uuid.New(),
		Status:       vo.ExtractionStatusComplete,
		CreatedAt:    createdAt,
		UpdatedAt:    createdAt,
	}

	repo := &readinessRepoStub{
		listFn: func(_ context.Context, state string, _ time.Duration, _ time.Time, _ uuid.UUID, limit int) ([]*entities.ExtractionRequest, error) {
			assert.Equal(t, "pending", state)
			assert.Equal(t, 25, limit)
			return []*entities.ExtractionRequest{row}, nil
		},
	}

	uc := &UseCase{extractionRepo: repo}

	got, err := uc.ListBridgeCandidates(context.Background(), "pending", time.Hour, time.Time{}, uuid.Nil, 25)
	require.NoError(t, err)
	require.Len(t, got, 1)

	assert.Equal(t, vo.BridgeReadinessPending, got[0].ReadinessState)
	assert.Same(t, row, got[0].Extraction)
	assert.GreaterOrEqual(t, got[0].AgeSeconds, int64(85))
	assert.LessOrEqual(t, got[0].AgeSeconds, int64(120))
}

func TestListBridgeCandidates_FiltersNilRows(t *testing.T) {
	t.Parallel()

	repo := &readinessRepoStub{
		listFn: func(_ context.Context, _ string, _ time.Duration, _ time.Time, _ uuid.UUID, _ int) ([]*entities.ExtractionRequest, error) {
			return []*entities.ExtractionRequest{nil, {
				ID: uuid.New(), CreatedAt: time.Now().UTC(),
			}, nil}, nil
		},
	}

	uc := &UseCase{extractionRepo: repo}

	got, err := uc.ListBridgeCandidates(context.Background(), "ready", time.Hour, time.Time{}, uuid.Nil, 10)
	require.NoError(t, err)
	assert.Len(t, got, 1, "nil rows should be filtered")
}

func TestListBridgeCandidates_InvalidState(t *testing.T) {
	t.Parallel()

	uc := &UseCase{extractionRepo: &readinessRepoStub{}}

	_, err := uc.ListBridgeCandidates(context.Background(), "bogus", time.Hour, time.Time{}, uuid.Nil, 10)
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrInvalidReadinessState))
}

func TestListBridgeCandidates_NegativeLimit(t *testing.T) {
	t.Parallel()

	uc := &UseCase{extractionRepo: &readinessRepoStub{}}

	_, err := uc.ListBridgeCandidates(context.Background(), "ready", time.Hour, time.Time{}, uuid.Nil, -1)
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrReadinessLimitInvalid))
}

func TestListBridgeCandidates_NegativeThreshold(t *testing.T) {
	t.Parallel()

	uc := &UseCase{extractionRepo: &readinessRepoStub{}}

	_, err := uc.ListBridgeCandidates(context.Background(), "ready", -time.Second, time.Time{}, uuid.Nil, 10)
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrReadinessThresholdInvalid))
}

func TestListBridgeCandidates_NilUseCase(t *testing.T) {
	t.Parallel()

	var uc *UseCase
	_, err := uc.ListBridgeCandidates(context.Background(), "ready", time.Hour, time.Time{}, uuid.Nil, 10)
	assert.ErrorIs(t, err, ErrNilBridgeReadinessUseCase)
}

func TestListBridgeCandidates_RepoError(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("db gone")
	repo := &readinessRepoStub{
		listFn: func(_ context.Context, _ string, _ time.Duration, _ time.Time, _ uuid.UUID, _ int) ([]*entities.ExtractionRequest, error) {
			return nil, wantErr
		},
	}

	uc := &UseCase{extractionRepo: repo}

	_, err := uc.ListBridgeCandidates(context.Background(), "stale", time.Hour, time.Time{}, uuid.Nil, 10)
	require.Error(t, err)
	assert.True(t, errors.Is(err, wantErr))
}

func TestListBridgeCandidates_PassesCursorThrough(t *testing.T) {
	t.Parallel()

	cursorTime := time.Now().UTC().Add(-time.Hour)
	cursorID := uuid.New()

	repo := &readinessRepoStub{
		listFn: func(_ context.Context, _ string, _ time.Duration, ca time.Time, ia uuid.UUID, _ int) ([]*entities.ExtractionRequest, error) {
			assert.Equal(t, cursorTime, ca)
			assert.Equal(t, cursorID, ia)
			return nil, nil
		},
	}

	uc := &UseCase{extractionRepo: repo}

	_, err := uc.ListBridgeCandidates(context.Background(), "ready", time.Hour, cursorTime, cursorID, 10)
	require.NoError(t, err)
}
