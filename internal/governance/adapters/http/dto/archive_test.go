// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package dto

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"

	"github.com/LerianStudio/matcher/internal/governance/domain/entities"
)

func TestArchiveMetadataToResponse_NilInput(t *testing.T) {
	t.Parallel()

	resp := ArchiveMetadataToResponse(nil)
	assert.Empty(t, resp.ID)
	assert.Empty(t, resp.PartitionName)
	assert.Empty(t, resp.Status)
	assert.Nil(t, resp.ArchivedAt)
}

func TestArchiveMetadataToResponse_FullEntity(t *testing.T) {
	t.Parallel()

	archivedAt := time.Date(2024, 4, 1, 2, 30, 0, 0, time.UTC)
	am := &entities.ArchiveMetadata{
		ID:                  uuid.MustParse("550e8400-e29b-41d4-a716-446655440000"),
		PartitionName:       "audit_logs_2024_q1",
		DateRangeStart:      time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
		DateRangeEnd:        time.Date(2024, 3, 31, 23, 59, 59, 0, time.UTC),
		RowCount:            150000,
		CompressedSizeBytes: 10485760,
		StorageClass:        "GLACIER",
		Status:              entities.StatusComplete,
		ArchivedAt:          &archivedAt,
	}

	resp := ArchiveMetadataToResponse(am)

	assert.Equal(t, "550e8400-e29b-41d4-a716-446655440000", resp.ID)
	assert.Equal(t, "audit_logs_2024_q1", resp.PartitionName)
	assert.Equal(t, "2024-01-01T00:00:00Z", resp.DateRangeStart)
	assert.Equal(t, "2024-03-31T23:59:59Z", resp.DateRangeEnd)
	assert.Equal(t, int64(150000), resp.RowCount)
	assert.Equal(t, int64(10485760), resp.CompressedSizeBytes)
	assert.Equal(t, "GLACIER", resp.StorageClass)
	assert.Equal(t, "COMPLETE", resp.Status)
	assert.NotNil(t, resp.ArchivedAt)
	assert.Equal(t, "2024-04-01T02:30:00Z", *resp.ArchivedAt)
}

func TestArchiveMetadataToResponse_NilArchivedAt(t *testing.T) {
	t.Parallel()

	am := &entities.ArchiveMetadata{
		ID:             uuid.MustParse("550e8400-e29b-41d4-a716-446655440000"),
		PartitionName:  "audit_logs_2024_q2",
		DateRangeStart: time.Date(2024, 4, 1, 0, 0, 0, 0, time.UTC),
		DateRangeEnd:   time.Date(2024, 6, 30, 23, 59, 59, 0, time.UTC),
		Status:         entities.StatusPending,
		ArchivedAt:     nil,
	}

	resp := ArchiveMetadataToResponse(am)
	assert.Nil(t, resp.ArchivedAt)
	assert.Equal(t, "PENDING", resp.Status)
}

func TestArchiveMetadataToResponses_NilSlice(t *testing.T) {
	t.Parallel()

	result := ArchiveMetadataToResponses(nil)
	assert.Empty(t, result)
}

func TestArchiveMetadataToResponses_EmptySlice(t *testing.T) {
	t.Parallel()

	result := ArchiveMetadataToResponses([]*entities.ArchiveMetadata{})
	assert.Empty(t, result)
}

func TestArchiveMetadataToResponses_FiltersNil(t *testing.T) {
	t.Parallel()

	archives := []*entities.ArchiveMetadata{
		{
			ID:             uuid.MustParse("11111111-1111-1111-1111-111111111111"),
			PartitionName:  "partition_1",
			DateRangeStart: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			DateRangeEnd:   time.Date(2024, 3, 31, 23, 59, 59, 0, time.UTC),
			Status:         entities.StatusComplete,
		},
		nil,
		{
			ID:             uuid.MustParse("22222222-2222-2222-2222-222222222222"),
			PartitionName:  "partition_2",
			DateRangeStart: time.Date(2024, 4, 1, 0, 0, 0, 0, time.UTC),
			DateRangeEnd:   time.Date(2024, 6, 30, 23, 59, 59, 0, time.UTC),
			Status:         entities.StatusComplete,
		},
	}

	result := ArchiveMetadataToResponses(archives)
	assert.Len(t, result, 2)
	assert.Equal(t, "11111111-1111-1111-1111-111111111111", result[0].ID)
	assert.Equal(t, "22222222-2222-2222-2222-222222222222", result[1].ID)
}

func TestArchiveMetadataToResponses_MultipleEntries(t *testing.T) {
	t.Parallel()

	archivedAt := time.Date(2024, 7, 1, 0, 0, 0, 0, time.UTC)
	archives := []*entities.ArchiveMetadata{
		{
			ID:             uuid.MustParse("11111111-1111-1111-1111-111111111111"),
			PartitionName:  "partition_a",
			DateRangeStart: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			DateRangeEnd:   time.Date(2024, 3, 31, 23, 59, 59, 0, time.UTC),
			Status:         entities.StatusComplete,
			ArchivedAt:     &archivedAt,
		},
		{
			ID:             uuid.MustParse("22222222-2222-2222-2222-222222222222"),
			PartitionName:  "partition_b",
			DateRangeStart: time.Date(2024, 4, 1, 0, 0, 0, 0, time.UTC),
			DateRangeEnd:   time.Date(2024, 6, 30, 23, 59, 59, 0, time.UTC),
			Status:         entities.StatusPending,
		},
	}

	result := ArchiveMetadataToResponses(archives)
	assert.Len(t, result, 2)
	assert.NotNil(t, result[0].ArchivedAt)
	assert.Nil(t, result[1].ArchivedAt)
}

func TestArchiveDownloadResponse_Fields(t *testing.T) {
	t.Parallel()

	resp := ArchiveDownloadResponse{
		DownloadURL: "https://s3.example.com/archive.gz",
		ExpiresAt:   "2026-02-05T13:00:00Z",
		Checksum:    "sha256:abc123",
	}

	assert.Equal(t, "https://s3.example.com/archive.gz", resp.DownloadURL)
	assert.Equal(t, "2026-02-05T13:00:00Z", resp.ExpiresAt)
	assert.Equal(t, "sha256:abc123", resp.Checksum)
}

func TestListArchivesResponse_Fields(t *testing.T) {
	t.Parallel()

	resp := ListArchivesResponse{
		Items:   []ArchiveMetadataResponse{{ID: "test-1"}},
		Limit:   20,
		HasMore: true,
	}

	assert.Len(t, resp.Items, 1)
	assert.Equal(t, 20, resp.Limit)
	assert.True(t, resp.HasMore)
}
