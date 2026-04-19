// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package cross

import (
	"context"
	"regexp"
	"testing"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/shared/infrastructure/testutil"
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

// TestNewBridgeSourceResolverAdapter_RejectsNilProvider exercises the
// defensive constructor check.
func TestNewBridgeSourceResolverAdapter_RejectsNilProvider(t *testing.T) {
	t.Parallel()

	adapter, err := NewBridgeSourceResolverAdapter(nil)
	require.Nil(t, adapter)
	require.ErrorIs(t, err, ErrNilBridgeSourceResolverProvider)
}

// TestResolveSourceForConnection_HappyPath_ReturnsTarget is the
// canonical test: a FETCHER source wired to the given connection id is
// returned as a BridgeSourceTarget.
func TestResolveSourceForConnection_HappyPath_ReturnsTarget(t *testing.T) {
	t.Parallel()

	db, mock, dbErr := sqlmock.New()
	require.NoError(t, dbErr)
	defer func() { _ = db.Close() }()

	provider := testutil.NewMockProviderFromDB(t, db)

	adapter, err := NewBridgeSourceResolverAdapter(provider)
	require.NoError(t, err)

	connID := uuid.New()
	sourceID := uuid.New()
	contextID := uuid.New()

	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta(
		`SELECT id, context_id
			FROM reconciliation_sources
			WHERE type = 'FETCHER' AND config->>'connection_id' = $1::text
			ORDER BY created_at ASC
			LIMIT 2`,
	)).WithArgs(connID.String()).WillReturnRows(
		sqlmock.NewRows([]string{"id", "context_id"}).
			AddRow(sourceID.String(), contextID.String()),
	)
	mock.ExpectCommit()

	target, err := adapter.ResolveSourceForConnection(context.Background(), connID)
	require.NoError(t, err)
	assert.Equal(t, sourceID, target.SourceID)
	assert.Equal(t, contextID, target.ContextID)
	assert.Equal(t, "json", target.Format)

	require.NoError(t, mock.ExpectationsWereMet())
}

// TestResolveSourceForConnection_NoMatch_ReturnsUnresolvable asserts the
// "no FETCHER source wired for this connection" path surfaces as the
// canonical sentinel.
func TestResolveSourceForConnection_NoMatch_ReturnsUnresolvable(t *testing.T) {
	t.Parallel()

	db, mock, dbErr := sqlmock.New()
	require.NoError(t, dbErr)
	defer func() { _ = db.Close() }()

	provider := testutil.NewMockProviderFromDB(t, db)

	adapter, err := NewBridgeSourceResolverAdapter(provider)
	require.NoError(t, err)

	connID := uuid.New()

	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta(
		`SELECT id, context_id
			FROM reconciliation_sources
			WHERE type = 'FETCHER' AND config->>'connection_id' = $1::text
			ORDER BY created_at ASC
			LIMIT 2`,
	)).WithArgs(connID.String()).WillReturnRows(
		sqlmock.NewRows([]string{"id", "context_id"}),
	)
	mock.ExpectRollback()

	_, err = adapter.ResolveSourceForConnection(context.Background(), connID)
	require.ErrorIs(t, err, sharedPorts.ErrBridgeSourceUnresolvable)

	require.NoError(t, mock.ExpectationsWereMet())
}

// TestResolveSourceForConnection_EmptyConnectionID_Rejected exercises
// input validation.
func TestResolveSourceForConnection_EmptyConnectionID_Rejected(t *testing.T) {
	t.Parallel()

	db, _, dbErr := sqlmock.New()
	require.NoError(t, dbErr)
	defer func() { _ = db.Close() }()

	provider := testutil.NewMockProviderFromDB(t, db)

	adapter, err := NewBridgeSourceResolverAdapter(provider)
	require.NoError(t, err)

	_, err = adapter.ResolveSourceForConnection(context.Background(), uuid.Nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "connection id is required")
}

// TestResolveSourceForConnection_NilAdapter_ReturnsSentinel guards the
// defensive nil-receiver branch.
func TestResolveSourceForConnection_NilAdapter_ReturnsSentinel(t *testing.T) {
	t.Parallel()

	var adapter *BridgeSourceResolverAdapter

	_, err := adapter.ResolveSourceForConnection(context.Background(), uuid.New())
	require.ErrorIs(t, err, ErrNilBridgeSourceResolverProvider)
}

// TestResolveSourceForConnection_MultipleSources_ReturnsTypedError covers the
// legacy-data regression path: migration 000032 now prevents new duplicates,
// but the adapter still defends against two FETCHER sources sharing a
// connection_id by returning ErrMultipleFetcherSourcesForConnection instead
// of silently picking the oldest row.
func TestResolveSourceForConnection_MultipleSources_ReturnsTypedError(t *testing.T) {
	t.Parallel()

	db, mock, dbErr := sqlmock.New()
	require.NoError(t, dbErr)
	defer func() { _ = db.Close() }()

	provider := testutil.NewMockProviderFromDB(t, db)

	adapter, err := NewBridgeSourceResolverAdapter(provider)
	require.NoError(t, err)

	connID := uuid.New()

	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta(
		`SELECT id, context_id
			FROM reconciliation_sources
			WHERE type = 'FETCHER' AND config->>'connection_id' = $1::text
			ORDER BY created_at ASC
			LIMIT 2`,
	)).WithArgs(connID.String()).WillReturnRows(
		sqlmock.NewRows([]string{"id", "context_id"}).
			AddRow(uuid.New().String(), uuid.New().String()).
			AddRow(uuid.New().String(), uuid.New().String()),
	)
	mock.ExpectRollback()

	_, err = adapter.ResolveSourceForConnection(context.Background(), connID)
	require.ErrorIs(t, err, ErrMultipleFetcherSourcesForConnection)

	require.NoError(t, mock.ExpectationsWereMet())
}

// Compile-time interface assertion: the adapter implements the port.
var _ sharedPorts.BridgeSourceResolver = (*BridgeSourceResolverAdapter)(nil)
