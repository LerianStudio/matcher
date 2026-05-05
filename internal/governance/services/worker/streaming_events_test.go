// Copyright 2026 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package worker

import (
	"context"
	"database/sql"
	"encoding/json"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	tmcore "github.com/LerianStudio/lib-commons/v5/commons/tenant-manager/core"
	"github.com/LerianStudio/lib-streaming/v2/streamingtest"

	"github.com/LerianStudio/matcher/internal/governance/domain/entities"
	"github.com/LerianStudio/matcher/internal/streaming/emission"
)

func TestFormatArchiveTimeUsesUTCAndRFC3339Nano(t *testing.T) {
	input := time.Date(2026, time.May, 4, 10, 11, 12, 13, time.FixedZone("BRT", -3*60*60))

	formatted := formatArchiveTime(input)

	parsed, err := time.Parse(time.RFC3339Nano, formatted)
	require.NoError(t, err)
	assert.Equal(t, input.UTC(), parsed)
}

// TestEmitArchiveEventAttachesAllOptionalFields verifies the optional-field
// gating in emitArchiveEvent: when the underlying ArchiveMetadata has
// non-zero values for checksum, row_count, compressed_size_bytes,
// storage_class, and ArchivedAt, every one of those fields is present on
// the wire. This is the "fully populated" archive.completed shape that
// downstream compliance dashboards depend on.
func TestEmitArchiveEventAttachesAllOptionalFields(t *testing.T) {
	mockEmitter := streamingtest.NewMockEmitter()
	worker := &ArchivalWorker{streamEmitter: mockEmitter}

	tenantID := uuid.MustParse("018f4f95-0000-7000-8000-000000000001")
	ctx := tmcore.ContextWithTenantID(context.Background(), tenantID.String())

	tx, sqlMock := newArchiveEventTx(t, ctx)

	rangeStart := time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)
	rangeEnd := time.Date(2026, time.February, 1, 0, 0, 0, 0, time.UTC)
	createdAt := time.Date(2026, time.February, 2, 12, 0, 0, 0, time.UTC)
	updatedAt := time.Date(2026, time.February, 2, 12, 5, 0, 0, time.UTC)
	archivedAt := time.Date(2026, time.February, 2, 12, 4, 30, 0, time.UTC)

	metadata := &entities.ArchiveMetadata{
		ID:                  uuid.MustParse("018f4f95-1111-7000-8000-000000000002"),
		TenantID:            tenantID,
		PartitionName:       "audit_logs_2026_01",
		DateRangeStart:      rangeStart,
		DateRangeEnd:        rangeEnd,
		RowCount:            12345,
		ArchiveKey:          "archive/2026/01/audit.json.gz",
		Checksum:            "sha256:deadbeefcafef00d",
		CompressedSizeBytes: 9876543,
		StorageClass:        "STANDARD",
		Status:              entities.StatusComplete,
		ArchivedAt:          &archivedAt,
		CreatedAt:           createdAt,
		UpdatedAt:           updatedAt,
	}

	err := worker.emitArchiveEvent(ctx, tx, "archive.completed", metadata.ID.String(), metadata)
	require.NoError(t, err)

	streamingtest.AssertEventCount(t, mockEmitter, "archive.completed", 1)
	streamingtest.AssertTenantID(t, mockEmitter, tenantID.String())

	requests := mockEmitter.Requests()
	require.Len(t, requests, 1)
	request := requests[0]
	assert.Equal(t, "archive.completed", request.DefinitionKey)
	assert.Equal(t, metadata.ID.String(), request.Subject)

	var payload map[string]any
	require.NoError(t, json.Unmarshal(request.Payload, &payload))

	// Catalog-required identifying fields are always present.
	assert.Equal(t, metadata.ID.String(), payload["archive_metadata_id"])
	assert.Equal(t, tenantID.String(), payload["tenant_id"])
	assert.Equal(t, "audit_logs_2026_01", payload["partition_name"])
	assert.Equal(t, string(entities.StatusComplete), payload["status"])

	// Timestamps formatted via emission.FormatTime — RFC3339Nano UTC.
	assert.Equal(t, emission.FormatTime(rangeStart), payload["date_range_start"])
	assert.Equal(t, emission.FormatTime(rangeEnd), payload["date_range_end"])
	assert.Equal(t, emission.FormatTime(createdAt), payload["created_at"])
	assert.Equal(t, emission.FormatTime(updatedAt), payload["updated_at"])
	assert.Equal(t, emission.FormatTime(archivedAt), payload["archived_at"])

	// Optional fields populated because the metadata carried non-zero values.
	assert.Equal(t, "sha256:deadbeefcafef00d", payload["checksum"])
	assert.Equal(t, float64(12345), payload["row_count"])
	assert.Equal(t, float64(9876543), payload["compressed_size_bytes"])
	assert.Equal(t, "STANDARD", payload["storage_class"])

	// sqlmock saw the begin (no commit/rollback expected — emit-only path).
	assert.NoError(t, sqlMock.ExpectationsWereMet())
}

// TestEmitArchiveEventOmitsOptionalFieldsWhenMetadataIsMinimal verifies the
// other half of the optional-field contract: when Checksum is empty,
// RowCount and CompressedSizeBytes are zero, StorageClass is empty, and
// ArchivedAt is nil, those fields are ABSENT from the payload (not just
// zero-valued). This keeps the wire format unambiguous for archives that
// failed before computing those values.
func TestEmitArchiveEventOmitsOptionalFieldsWhenMetadataIsMinimal(t *testing.T) {
	mockEmitter := streamingtest.NewMockEmitter()
	worker := &ArchivalWorker{streamEmitter: mockEmitter}

	tenantID := uuid.MustParse("018f4f95-0000-7000-8000-000000000001")
	ctx := tmcore.ContextWithTenantID(context.Background(), tenantID.String())

	tx, sqlMock := newArchiveEventTx(t, ctx)

	rangeStart := time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)
	rangeEnd := time.Date(2026, time.February, 1, 0, 0, 0, 0, time.UTC)

	metadata := &entities.ArchiveMetadata{
		ID:             uuid.MustParse("018f4f95-1111-7000-8000-000000000002"),
		TenantID:       tenantID,
		PartitionName:  "audit_logs_2026_01",
		DateRangeStart: rangeStart,
		DateRangeEnd:   rangeEnd,
		// RowCount, CompressedSizeBytes intentionally zero.
		// Checksum, StorageClass intentionally empty.
		// ArchivedAt intentionally nil.
		Status:    entities.StatusPending,
		CreatedAt: time.Now().UTC(),
		UpdatedAt: time.Now().UTC(),
	}

	err := worker.emitArchiveEvent(ctx, tx, "archive_metadata.created", metadata.ID.String(), metadata)
	require.NoError(t, err)

	requests := mockEmitter.Requests()
	require.Len(t, requests, 1)

	var payload map[string]any
	require.NoError(t, json.Unmarshal(requests[0].Payload, &payload))

	// Required identifiers always present.
	assert.Equal(t, metadata.ID.String(), payload["archive_metadata_id"])
	assert.Equal(t, tenantID.String(), payload["tenant_id"])
	assert.Equal(t, "audit_logs_2026_01", payload["partition_name"])

	// Optional fields are ABSENT — the gating contract.
	assert.NotContains(t, payload, "checksum",
		"empty Checksum must not appear as an empty string on the wire")
	assert.NotContains(t, payload, "row_count",
		"zero RowCount must not appear as 0 on the wire")
	assert.NotContains(t, payload, "compressed_size_bytes",
		"zero CompressedSizeBytes must not appear as 0 on the wire")
	assert.NotContains(t, payload, "storage_class",
		"empty StorageClass must not appear as an empty string on the wire")
	assert.NotContains(t, payload, "archived_at",
		"nil ArchivedAt must not appear at all on the wire")

	assert.NoError(t, sqlMock.ExpectationsWereMet())
}

// TestEmitArchiveEventNoOpForNilMetadata verifies the early return: a nil
// metadata pointer must not produce any emission. The archive lifecycle
// should never invoke this with nil, but the defensive guard keeps a stale
// caller from blowing up the streaming pipeline.
func TestEmitArchiveEventNoOpForNilMetadata(t *testing.T) {
	mockEmitter := streamingtest.NewMockEmitter()
	worker := &ArchivalWorker{streamEmitter: mockEmitter}
	ctx := tmcore.ContextWithTenantID(context.Background(), "018f4f95-0000-7000-8000-000000000001")

	err := worker.emitArchiveEvent(ctx, nil, "archive.completed", "subject", nil)

	require.NoError(t, err)
	streamingtest.AssertNoEvents(t, mockEmitter)
}

// newArchiveEventTx provides a sqlmock-backed *sql.Tx for emit-only tests.
// emitArchiveEvent needs a non-nil tx to satisfy the
// emission.RequireOutboxTx() invariant; it does NOT execute any SQL on the
// tx, so we only set ExpectBegin. We deliberately do NOT register an
// ExpectRollback / ExpectCommit because emitArchiveEvent does not call
// either; setting one would force ExpectationsWereMet() to run AFTER
// cleanup, which complicates assertions inside the test body.
//
// To prevent a goroutine leak (database/sql spawns an awaitDone goroutine
// per BeginTx that only exits on Commit/Rollback OR ctx cancellation), we
// wrap the caller ctx with a CancelFunc and cancel it in t.Cleanup. This
// unblocks awaitDone without requiring a Rollback expectation. db.Close()
// alone does NOT terminate awaitDone — only ctx cancellation does, because
// BeginTx wraps the user ctx with context.WithCancel internally and
// awaitDone selects on the resulting Done() channel.
func newArchiveEventTx(t *testing.T, ctx context.Context) (*sql.Tx, sqlmock.Sqlmock) {
	t.Helper()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	txCtx, cancel := context.WithCancel(ctx)
	t.Cleanup(func() {
		cancel()
		_ = db.Close()
	})

	mock.ExpectBegin()

	tx, err := db.BeginTx(txCtx, nil)
	require.NoError(t, err)

	return tx, mock
}
