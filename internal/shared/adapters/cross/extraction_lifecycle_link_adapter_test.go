// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package cross

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	discoveryEntities "github.com/LerianStudio/matcher/internal/discovery/domain/entities"
	discoveryRepositories "github.com/LerianStudio/matcher/internal/discovery/domain/repositories"
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

// errLinkWriterBackend is a sentinel error used for simulating downstream
// failure scenarios in the extraction lifecycle link adapter tests.
var errLinkWriterBackend = errors.New("link writer backend failure")

// fakeExtractionRepo is a compact manual mock for the small subset of the
// ExtractionRepository interface exercised by the link adapter. Other methods
// return zero values so tests stay focused on the paths under test.
type fakeExtractionRepo struct {
	findResult *discoveryEntities.ExtractionRequest
	findErr    error
	updateErr  error
	updateCall *discoveryEntities.ExtractionRequest
}

func (repo *fakeExtractionRepo) Create(_ context.Context, _ *discoveryEntities.ExtractionRequest) error {
	return nil
}

func (repo *fakeExtractionRepo) CreateWithTx(_ context.Context, _ sharedPorts.Tx, _ *discoveryEntities.ExtractionRequest) error {
	return nil
}

func (repo *fakeExtractionRepo) Update(_ context.Context, req *discoveryEntities.ExtractionRequest) error {
	repo.updateCall = req

	return repo.updateErr
}

func (repo *fakeExtractionRepo) UpdateIfUnchanged(
	_ context.Context,
	_ *discoveryEntities.ExtractionRequest,
	_ time.Time,
) error {
	return nil
}

func (repo *fakeExtractionRepo) UpdateIfUnchangedWithTx(
	_ context.Context,
	_ sharedPorts.Tx,
	_ *discoveryEntities.ExtractionRequest,
	_ time.Time,
) error {
	return nil
}

func (repo *fakeExtractionRepo) UpdateWithTx(
	_ context.Context,
	_ sharedPorts.Tx,
	_ *discoveryEntities.ExtractionRequest,
) error {
	return nil
}

func (repo *fakeExtractionRepo) FindByID(
	_ context.Context,
	_ uuid.UUID,
) (*discoveryEntities.ExtractionRequest, error) {
	return repo.findResult, repo.findErr
}

func TestNewExtractionLifecycleLinkWriterAdapter_RejectsNilRepo(t *testing.T) {
	t.Parallel()

	adapter, err := NewExtractionLifecycleLinkWriterAdapter(nil)
	require.Nil(t, adapter)
	require.ErrorIs(t, err, sharedPorts.ErrNilExtractionLifecycleLinkWriter)
}

func TestLinkExtractionToIngestion_HappyPath_PersistsLink(t *testing.T) {
	t.Parallel()

	extractionID := uuid.New()
	ingestionID := uuid.New()
	connectionID := uuid.New()

	repo := &fakeExtractionRepo{
		findResult: &discoveryEntities.ExtractionRequest{
			ID:           extractionID,
			ConnectionID: connectionID,
		},
	}

	adapter, err := NewExtractionLifecycleLinkWriterAdapter(
		discoveryRepositories.ExtractionRepository(repo),
	)
	require.NoError(t, err)

	err = adapter.LinkExtractionToIngestion(context.Background(), extractionID, ingestionID)
	require.NoError(t, err)

	require.NotNil(t, repo.updateCall)
	require.Equal(t, ingestionID, repo.updateCall.IngestionJobID)
}

func TestLinkExtractionToIngestion_AlreadyLinked_ReturnsIdempotencySentinel(t *testing.T) {
	t.Parallel()

	extractionID := uuid.New()
	preexistingIngestionID := uuid.New()

	repo := &fakeExtractionRepo{
		findResult: &discoveryEntities.ExtractionRequest{
			ID:             extractionID,
			ConnectionID:   uuid.New(),
			IngestionJobID: preexistingIngestionID,
		},
	}

	adapter, err := NewExtractionLifecycleLinkWriterAdapter(
		discoveryRepositories.ExtractionRepository(repo),
	)
	require.NoError(t, err)

	err = adapter.LinkExtractionToIngestion(context.Background(), extractionID, uuid.New())
	require.Error(t, err)
	require.ErrorIs(t, err, sharedPorts.ErrExtractionAlreadyLinked)

	// The adapter must not issue an Update call when the link is preserved.
	require.Nil(t, repo.updateCall, "Update must not be called when extraction is already linked")
}

func TestLinkExtractionToIngestion_NotFound_SurfacesSentinel(t *testing.T) {
	t.Parallel()

	repo := &fakeExtractionRepo{
		findErr: discoveryRepositories.ErrExtractionNotFound,
	}

	adapter, err := NewExtractionLifecycleLinkWriterAdapter(
		discoveryRepositories.ExtractionRepository(repo),
	)
	require.NoError(t, err)

	err = adapter.LinkExtractionToIngestion(context.Background(), uuid.New(), uuid.New())
	require.ErrorIs(t, err, discoveryRepositories.ErrExtractionNotFound)
}

func TestLinkExtractionToIngestion_MissingExtractionID_ReturnsSentinel(t *testing.T) {
	t.Parallel()

	adapter, err := NewExtractionLifecycleLinkWriterAdapter(
		discoveryRepositories.ExtractionRepository(&fakeExtractionRepo{}),
	)
	require.NoError(t, err)

	err = adapter.LinkExtractionToIngestion(context.Background(), uuid.Nil, uuid.New())
	require.ErrorIs(t, err, sharedPorts.ErrLinkExtractionIDRequired)
}

func TestLinkExtractionToIngestion_MissingIngestionJobID_ReturnsSentinel(t *testing.T) {
	t.Parallel()

	adapter, err := NewExtractionLifecycleLinkWriterAdapter(
		discoveryRepositories.ExtractionRepository(&fakeExtractionRepo{}),
	)
	require.NoError(t, err)

	err = adapter.LinkExtractionToIngestion(context.Background(), uuid.New(), uuid.Nil)
	require.ErrorIs(t, err, sharedPorts.ErrLinkIngestionJobIDRequired)
}

func TestLinkExtractionToIngestion_PersistError_WrapsUnderlying(t *testing.T) {
	t.Parallel()

	extractionID := uuid.New()
	repo := &fakeExtractionRepo{
		findResult: &discoveryEntities.ExtractionRequest{
			ID:           extractionID,
			ConnectionID: uuid.New(),
		},
		updateErr: errLinkWriterBackend,
	}

	adapter, err := NewExtractionLifecycleLinkWriterAdapter(
		discoveryRepositories.ExtractionRepository(repo),
	)
	require.NoError(t, err)

	err = adapter.LinkExtractionToIngestion(context.Background(), extractionID, uuid.New())
	require.Error(t, err)
	require.ErrorIs(t, err, errLinkWriterBackend)
}

// TestLinkExtractionToIngestion_NilAdapter_ReturnsSentinel exercises the
// defensive nil-receiver guard that mirrors the intake-adapter behavior.
func TestLinkExtractionToIngestion_NilAdapter_ReturnsSentinel(t *testing.T) {
	t.Parallel()

	var adapter *ExtractionLifecycleLinkWriterAdapter

	err := adapter.LinkExtractionToIngestion(context.Background(), uuid.New(), uuid.New())
	require.ErrorIs(t, err, sharedPorts.ErrNilExtractionLifecycleLinkWriter)
}

// TestLinkExtractionToIngestion_FindByIDNonSentinelError_WrapsError
// exercises the FindByID error branch where the underlying error is NOT the
// ErrExtractionNotFound sentinel. The adapter must wrap it with the
// "load extraction for link" prefix.
func TestLinkExtractionToIngestion_FindByIDNonSentinelError_WrapsError(t *testing.T) {
	t.Parallel()

	repo := &fakeExtractionRepo{findErr: errLinkWriterBackend}

	adapter, err := NewExtractionLifecycleLinkWriterAdapter(
		discoveryRepositories.ExtractionRepository(repo),
	)
	require.NoError(t, err)

	err = adapter.LinkExtractionToIngestion(context.Background(), uuid.New(), uuid.New())
	require.Error(t, err)
	require.ErrorIs(t, err, errLinkWriterBackend)
	require.Contains(t, err.Error(), "load extraction for link")
}

// TestLinkExtractionToIngestion_FindByIDReturnsNil_ReturnsNotFoundSentinel
// exercises the branch where FindByID returns (nil, nil) — i.e. "not found"
// expressed via a nil result rather than the sentinel error.
func TestLinkExtractionToIngestion_FindByIDReturnsNil_ReturnsNotFoundSentinel(t *testing.T) {
	t.Parallel()

	repo := &fakeExtractionRepo{findResult: nil, findErr: nil}

	adapter, err := NewExtractionLifecycleLinkWriterAdapter(
		discoveryRepositories.ExtractionRepository(repo),
	)
	require.NoError(t, err)

	err = adapter.LinkExtractionToIngestion(context.Background(), uuid.New(), uuid.New())
	require.ErrorIs(t, err, discoveryRepositories.ErrExtractionNotFound)
}
