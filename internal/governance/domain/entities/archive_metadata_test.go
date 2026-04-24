// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package entities

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewArchiveMetadata_ValidInput(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	tenantID := uuid.New()
	partitionName := "audit_logs_2026_01"
	rangeStart := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	rangeEnd := time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC)

	am, err := NewArchiveMetadata(ctx, tenantID, partitionName, rangeStart, rangeEnd)
	require.NoError(t, err)
	require.NotNil(t, am)

	assert.NotEqual(t, uuid.Nil, am.ID)
	assert.Equal(t, tenantID, am.TenantID)
	assert.Equal(t, partitionName, am.PartitionName)
	assert.Equal(t, rangeStart, am.DateRangeStart)
	assert.Equal(t, rangeEnd, am.DateRangeEnd)
	assert.Equal(t, StatusPending, am.Status)
	assert.Zero(t, am.RowCount)
	assert.Empty(t, am.ArchiveKey)
	assert.Empty(t, am.Checksum)
	assert.Zero(t, am.CompressedSizeBytes)
	assert.Empty(t, am.StorageClass)
	assert.Empty(t, am.ErrorMessage)
	assert.Nil(t, am.ArchivedAt)
	assert.False(t, am.CreatedAt.IsZero())
	assert.False(t, am.UpdatedAt.IsZero())
}

func TestNewArchiveMetadata_Validation(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	validTenantID := uuid.New()
	validStart := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	validEnd := time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC)

	tests := []struct {
		name          string
		tenantID      uuid.UUID
		partitionName string
		rangeStart    time.Time
		rangeEnd      time.Time
		expectedErr   error
	}{
		{
			name:          "nil tenant ID",
			tenantID:      uuid.Nil,
			partitionName: "audit_logs_2026_01",
			rangeStart:    validStart,
			rangeEnd:      validEnd,
			expectedErr:   ErrArchiveTenantIDRequired,
		},
		{
			name:          "empty partition name",
			tenantID:      validTenantID,
			partitionName: "",
			rangeStart:    validStart,
			rangeEnd:      validEnd,
			expectedErr:   ErrPartitionNameRequired,
		},
		{
			name:          "zero range start",
			tenantID:      validTenantID,
			partitionName: "audit_logs_2026_01",
			rangeStart:    time.Time{},
			rangeEnd:      validEnd,
			expectedErr:   ErrDateRangeStartRequired,
		},
		{
			name:          "zero range end",
			tenantID:      validTenantID,
			partitionName: "audit_logs_2026_01",
			rangeStart:    validStart,
			rangeEnd:      time.Time{},
			expectedErr:   ErrDateRangeEndRequired,
		},
		{
			name:          "range end before start",
			tenantID:      validTenantID,
			partitionName: "audit_logs_2026_01",
			rangeStart:    validEnd,
			rangeEnd:      validStart,
			expectedErr:   ErrDateRangeEndBeforeStart,
		},
		{
			name:          "range end equals start",
			tenantID:      validTenantID,
			partitionName: "audit_logs_2026_01",
			rangeStart:    validStart,
			rangeEnd:      validStart,
			expectedErr:   ErrDateRangeEndBeforeStart,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := NewArchiveMetadata(ctx, tt.tenantID, tt.partitionName, tt.rangeStart, tt.rangeEnd)
			require.ErrorIs(t, err, tt.expectedErr)
		})
	}
}

func TestArchiveMetadata_ValidStateTransitions(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	newMetadata := func() *ArchiveMetadata {
		am, err := NewArchiveMetadata(
			ctx,
			uuid.New(),
			"audit_logs_2026_01",
			time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
			time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC),
		)
		require.NoError(t, err)

		return am
	}

	t.Run("PENDING to EXPORTING", func(t *testing.T) {
		t.Parallel()

		am := newMetadata()
		err := am.MarkExporting()
		require.NoError(t, err)
		assert.Equal(t, StatusExporting, am.Status)
	})

	t.Run("EXPORTING to EXPORTED", func(t *testing.T) {
		t.Parallel()

		am := newMetadata()
		require.NoError(t, am.MarkExporting())

		err := am.MarkExported(1000)
		require.NoError(t, err)
		assert.Equal(t, StatusExported, am.Status)
		assert.Equal(t, int64(1000), am.RowCount)
	})

	t.Run("EXPORTED to UPLOADING", func(t *testing.T) {
		t.Parallel()

		am := newMetadata()
		require.NoError(t, am.MarkExporting())
		require.NoError(t, am.MarkExported(100))

		err := am.MarkUploading()
		require.NoError(t, err)
		assert.Equal(t, StatusUploading, am.Status)
	})

	t.Run("UPLOADING to UPLOADED", func(t *testing.T) {
		t.Parallel()

		am := newMetadata()
		require.NoError(t, am.MarkExporting())
		require.NoError(t, am.MarkExported(100))
		require.NoError(t, am.MarkUploading())

		err := am.MarkUploaded("archives/tenant/2026/01/audit_logs.jsonl.gz", "sha256:abc123", 5000, "GLACIER")
		require.NoError(t, err)
		assert.Equal(t, StatusUploaded, am.Status)
		assert.Equal(t, "archives/tenant/2026/01/audit_logs.jsonl.gz", am.ArchiveKey)
		assert.Equal(t, "sha256:abc123", am.Checksum)
		assert.Equal(t, int64(5000), am.CompressedSizeBytes)
		assert.Equal(t, "GLACIER", am.StorageClass)
	})

	t.Run("UPLOADED to VERIFYING", func(t *testing.T) {
		t.Parallel()

		am := newMetadata()
		require.NoError(t, am.MarkExporting())
		require.NoError(t, am.MarkExported(100))
		require.NoError(t, am.MarkUploading())
		require.NoError(t, am.MarkUploaded("key", "checksum", 1000, "GLACIER"))

		err := am.MarkVerifying()
		require.NoError(t, err)
		assert.Equal(t, StatusVerifying, am.Status)
	})

	t.Run("VERIFYING to VERIFIED", func(t *testing.T) {
		t.Parallel()

		am := newMetadata()
		require.NoError(t, am.MarkExporting())
		require.NoError(t, am.MarkExported(100))
		require.NoError(t, am.MarkUploading())
		require.NoError(t, am.MarkUploaded("key", "checksum", 1000, "GLACIER"))
		require.NoError(t, am.MarkVerifying())

		err := am.MarkVerified()
		require.NoError(t, err)
		assert.Equal(t, StatusVerified, am.Status)
	})

	t.Run("VERIFIED to DETACHING", func(t *testing.T) {
		t.Parallel()

		am := newMetadata()
		require.NoError(t, am.MarkExporting())
		require.NoError(t, am.MarkExported(100))
		require.NoError(t, am.MarkUploading())
		require.NoError(t, am.MarkUploaded("key", "checksum", 1000, "GLACIER"))
		require.NoError(t, am.MarkVerifying())
		require.NoError(t, am.MarkVerified())

		err := am.MarkDetaching()
		require.NoError(t, err)
		assert.Equal(t, StatusDetaching, am.Status)
	})

	t.Run("DETACHING to COMPLETE", func(t *testing.T) {
		t.Parallel()

		am := newMetadata()
		require.NoError(t, am.MarkExporting())
		require.NoError(t, am.MarkExported(100))
		require.NoError(t, am.MarkUploading())
		require.NoError(t, am.MarkUploaded("key", "checksum", 1000, "GLACIER"))
		require.NoError(t, am.MarkVerifying())
		require.NoError(t, am.MarkVerified())
		require.NoError(t, am.MarkDetaching())

		err := am.MarkComplete()
		require.NoError(t, err)
		assert.Equal(t, StatusComplete, am.Status)
		assert.NotNil(t, am.ArchivedAt)
	})

	t.Run("full lifecycle", func(t *testing.T) {
		t.Parallel()

		am := newMetadata()
		require.NoError(t, am.MarkExporting())
		require.NoError(t, am.MarkExported(500))
		require.NoError(t, am.MarkUploading())
		require.NoError(t, am.MarkUploaded("key", "sha256:abc", 2048, "GLACIER"))
		require.NoError(t, am.MarkVerifying())
		require.NoError(t, am.MarkVerified())
		require.NoError(t, am.MarkDetaching())
		require.NoError(t, am.MarkComplete())

		assert.Equal(t, StatusComplete, am.Status)
		assert.NotNil(t, am.ArchivedAt)
		assert.Equal(t, int64(500), am.RowCount)
	})
}

func TestArchiveMetadata_InvalidStateTransitions(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	newMetadata := func() *ArchiveMetadata {
		am, err := NewArchiveMetadata(
			ctx,
			uuid.New(),
			"audit_logs_2026_01",
			time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
			time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC),
		)
		require.NoError(t, err)

		return am
	}

	tests := []struct {
		name       string
		setupState func(am *ArchiveMetadata)
		transition func(am *ArchiveMetadata) error
	}{
		{
			name:       "PENDING cannot mark exported",
			setupState: func(_ *ArchiveMetadata) {},
			transition: func(am *ArchiveMetadata) error { return am.MarkExported(100) },
		},
		{
			name:       "PENDING cannot mark uploading",
			setupState: func(_ *ArchiveMetadata) {},
			transition: func(am *ArchiveMetadata) error { return am.MarkUploading() },
		},
		{
			name:       "PENDING cannot mark uploaded",
			setupState: func(_ *ArchiveMetadata) {},
			transition: func(am *ArchiveMetadata) error {
				return am.MarkUploaded("key", "checksum", 100, "GLACIER")
			},
		},
		{
			name:       "PENDING cannot mark verifying",
			setupState: func(_ *ArchiveMetadata) {},
			transition: func(am *ArchiveMetadata) error { return am.MarkVerifying() },
		},
		{
			name:       "PENDING cannot mark verified",
			setupState: func(_ *ArchiveMetadata) {},
			transition: func(am *ArchiveMetadata) error { return am.MarkVerified() },
		},
		{
			name:       "PENDING cannot mark detaching",
			setupState: func(_ *ArchiveMetadata) {},
			transition: func(am *ArchiveMetadata) error { return am.MarkDetaching() },
		},
		{
			name:       "PENDING cannot mark complete",
			setupState: func(_ *ArchiveMetadata) {},
			transition: func(am *ArchiveMetadata) error { return am.MarkComplete() },
		},
		{
			name: "EXPORTING cannot mark exporting again",
			setupState: func(am *ArchiveMetadata) {
				require.NoError(t, am.MarkExporting())
			},
			transition: func(am *ArchiveMetadata) error { return am.MarkExporting() },
		},
		{
			name: "EXPORTED cannot mark exported again",
			setupState: func(am *ArchiveMetadata) {
				require.NoError(t, am.MarkExporting())
				require.NoError(t, am.MarkExported(100))
			},
			transition: func(am *ArchiveMetadata) error { return am.MarkExported(100) },
		},
		{
			name: "UPLOADED cannot mark uploading again",
			setupState: func(am *ArchiveMetadata) {
				require.NoError(t, am.MarkExporting())
				require.NoError(t, am.MarkExported(100))
				require.NoError(t, am.MarkUploading())
				require.NoError(t, am.MarkUploaded("key", "checksum", 100, "GLACIER"))
			},
			transition: func(am *ArchiveMetadata) error { return am.MarkUploading() },
		},
		{
			name: "COMPLETE cannot transition further",
			setupState: func(am *ArchiveMetadata) {
				require.NoError(t, am.MarkExporting())
				require.NoError(t, am.MarkExported(100))
				require.NoError(t, am.MarkUploading())
				require.NoError(t, am.MarkUploaded("key", "checksum", 100, "GLACIER"))
				require.NoError(t, am.MarkVerifying())
				require.NoError(t, am.MarkVerified())
				require.NoError(t, am.MarkDetaching())
				require.NoError(t, am.MarkComplete())
			},
			transition: func(am *ArchiveMetadata) error { return am.MarkExporting() },
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			am := newMetadata()
			tt.setupState(am)

			err := tt.transition(am)
			require.ErrorIs(t, err, ErrInvalidStateTransition)
		})
	}
}

func TestArchiveMetadata_MarkExported_InvalidRowCount(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	am, err := NewArchiveMetadata(
		ctx,
		uuid.New(),
		"audit_logs_2026_01",
		time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC),
	)
	require.NoError(t, err)
	require.NoError(t, am.MarkExporting())

	err = am.MarkExported(-1)
	require.ErrorIs(t, err, ErrRowCountMustBeNonNegative)
}

func TestArchiveMetadata_MarkExported_ZeroRowCount(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	am, err := NewArchiveMetadata(
		ctx,
		uuid.New(),
		"audit_logs_2026_01",
		time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC),
	)
	require.NoError(t, err)
	require.NoError(t, am.MarkExporting())

	err = am.MarkExported(0)
	require.NoError(t, err)
	assert.Equal(t, StatusExported, am.Status)
	assert.Equal(t, int64(0), am.RowCount)
}

func TestArchiveMetadata_MarkUploaded_ValidationErrors(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	newUploadingMetadata := func() *ArchiveMetadata {
		am, err := NewArchiveMetadata(
			ctx,
			uuid.New(),
			"audit_logs_2026_01",
			time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
			time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC),
		)
		require.NoError(t, err)
		require.NoError(t, am.MarkExporting())
		require.NoError(t, am.MarkExported(100))
		require.NoError(t, am.MarkUploading())

		return am
	}

	t.Run("empty archive key", func(t *testing.T) {
		t.Parallel()

		am := newUploadingMetadata()
		err := am.MarkUploaded("", "checksum", 100, "GLACIER")
		require.ErrorIs(t, err, ErrArchiveKeyRequired)
	})

	t.Run("empty checksum", func(t *testing.T) {
		t.Parallel()

		am := newUploadingMetadata()
		err := am.MarkUploaded("key", "", 100, "GLACIER")
		require.ErrorIs(t, err, ErrChecksumRequired)
	})

	t.Run("negative compressed size", func(t *testing.T) {
		t.Parallel()

		am := newUploadingMetadata()
		err := am.MarkUploaded("key", "checksum", -1, "GLACIER")
		require.ErrorIs(t, err, ErrCompressedSizeNonNegative)
	})

	t.Run("empty storage class", func(t *testing.T) {
		t.Parallel()

		am := newUploadingMetadata()
		err := am.MarkUploaded("key", "checksum", 100, "")
		require.ErrorIs(t, err, ErrStorageClassRequired)
	})

	t.Run("zero compressed size is valid", func(t *testing.T) {
		t.Parallel()

		am := newUploadingMetadata()
		err := am.MarkUploaded("key", "checksum", 0, "GLACIER")
		require.NoError(t, err)
	})
}

func TestArchiveMetadata_MarkError(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	t.Run("preserves current status", func(t *testing.T) {
		t.Parallel()

		am, err := NewArchiveMetadata(
			ctx,
			uuid.New(),
			"audit_logs_2026_01",
			time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
			time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC),
		)
		require.NoError(t, err)
		require.NoError(t, am.MarkExporting())

		am.MarkError("connection timeout")

		assert.Equal(t, StatusExporting, am.Status)
		assert.Equal(t, "connection timeout", am.ErrorMessage)
	})

	t.Run("preserves status at any state", func(t *testing.T) {
		t.Parallel()

		am, err := NewArchiveMetadata(
			ctx,
			uuid.New(),
			"audit_logs_2026_01",
			time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
			time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC),
		)
		require.NoError(t, err)
		require.NoError(t, am.MarkExporting())
		require.NoError(t, am.MarkExported(100))
		require.NoError(t, am.MarkUploading())
		require.NoError(t, am.MarkUploaded("key", "checksum", 100, "GLACIER"))
		require.NoError(t, am.MarkVerifying())

		am.MarkError("checksum mismatch")

		assert.Equal(t, StatusVerifying, am.Status)
		assert.Equal(t, "checksum mismatch", am.ErrorMessage)
	})

	t.Run("empty string defaults to unknown error", func(t *testing.T) {
		t.Parallel()

		am, err := NewArchiveMetadata(
			ctx,
			uuid.New(),
			"audit_logs_2026_01",
			time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
			time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC),
		)
		require.NoError(t, err)

		am.MarkError("")

		assert.Equal(t, "unknown error", am.ErrorMessage)
		assert.Equal(t, StatusPending, am.Status)
	})

	t.Run("can overwrite previous error", func(t *testing.T) {
		t.Parallel()

		am, err := NewArchiveMetadata(
			ctx,
			uuid.New(),
			"audit_logs_2026_01",
			time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
			time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC),
		)
		require.NoError(t, err)

		am.MarkError("first error")
		assert.Equal(t, "first error", am.ErrorMessage)

		am.MarkError("second error")
		assert.Equal(t, "second error", am.ErrorMessage)
	})
}

func TestArchiveMetadata_StatusConstants(t *testing.T) {
	t.Parallel()

	statuses := []ArchiveStatus{
		StatusPending,
		StatusExporting,
		StatusExported,
		StatusUploading,
		StatusUploaded,
		StatusVerifying,
		StatusVerified,
		StatusDetaching,
		StatusComplete,
	}

	seen := make(map[ArchiveStatus]bool)

	for _, s := range statuses {
		assert.NotEmpty(t, s)
		assert.False(t, seen[s], "duplicate status: %s", s)

		seen[s] = true
	}
}

func TestArchiveMetadata_NilReceiverGuards(t *testing.T) {
	t.Parallel()

	var am *ArchiveMetadata

	tests := []struct {
		name string
		call func() error
	}{
		{name: "MarkExporting", call: am.MarkExporting},
		{name: "MarkExported", call: func() error { return am.MarkExported(1) }},
		{name: "MarkUploading", call: am.MarkUploading},
		{name: "MarkUploaded", call: func() error { return am.MarkUploaded("k", "c", 1, "GLACIER") }},
		{name: "MarkVerifying", call: am.MarkVerifying},
		{name: "MarkVerified", call: am.MarkVerified},
		{name: "MarkDetaching", call: am.MarkDetaching},
		{name: "MarkComplete", call: am.MarkComplete},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			require.ErrorIs(t, tt.call(), ErrNilArchiveMetadata)
		})
	}

	require.NotPanics(t, func() { am.MarkError("ignored") })
}

func TestArchiveStatus_IsValid(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		status ArchiveStatus
		want   bool
	}{
		{name: "PENDING is valid", status: StatusPending, want: true},
		{name: "EXPORTING is valid", status: StatusExporting, want: true},
		{name: "EXPORTED is valid", status: StatusExported, want: true},
		{name: "UPLOADING is valid", status: StatusUploading, want: true},
		{name: "UPLOADED is valid", status: StatusUploaded, want: true},
		{name: "VERIFYING is valid", status: StatusVerifying, want: true},
		{name: "VERIFIED is valid", status: StatusVerified, want: true},
		{name: "DETACHING is valid", status: StatusDetaching, want: true},
		{name: "COMPLETE is valid", status: StatusComplete, want: true},
		{name: "empty string is invalid", status: ArchiveStatus(""), want: false},
		{name: "lowercase pending is invalid", status: ArchiveStatus("pending"), want: false},
		{name: "arbitrary string is invalid", status: ArchiveStatus("UNKNOWN"), want: false},
		{name: "partial match is invalid", status: ArchiveStatus("PEND"), want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, tt.status.IsValid())
		})
	}
}

func TestArchiveMetadata_SentinelErrors(t *testing.T) {
	t.Parallel()

	errs := []error{
		ErrNilArchiveMetadata,
		ErrInvalidStateTransition,
		ErrArchiveTenantIDRequired,
		ErrPartitionNameRequired,
		ErrDateRangeStartRequired,
		ErrDateRangeEndRequired,
		ErrDateRangeEndBeforeStart,
		ErrRowCountMustBeNonNegative,
		ErrArchiveKeyRequired,
		ErrChecksumRequired,
		ErrCompressedSizeNonNegative,
		ErrStorageClassRequired,
	}

	for _, err := range errs {
		assert.Error(t, err)
		assert.NotEmpty(t, err.Error())
	}
}
