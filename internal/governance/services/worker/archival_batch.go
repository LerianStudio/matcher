// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package worker

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/attribute"

	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v5/commons/opentelemetry"

	"github.com/LerianStudio/matcher/internal/auth"
	"github.com/LerianStudio/matcher/internal/governance/domain/entities"
	"github.com/LerianStudio/matcher/internal/governance/services/command"
)

// archiveTenant identifies and processes partitions eligible for cold archival.
// Returns (processed, failed, err) partition counts. A partition counts as
// processed when archivePartition returns nil; it counts as failed when the
// metadata lookup or archivePartition surfaces an error. Already-complete
// partitions are a no-op in both directions.
func (aw *ArchivalWorker) archiveTenant(ctx context.Context, tenantID uuid.UUID) (int, int, error) {
	logger, tracer := aw.tracking(ctx)

	ctx, span := tracer.Start(ctx, "governance.archival.archive_tenant")
	defer span.End()

	partitions, err := aw.partitionMgr.ListPartitions(ctx)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to list partitions", err)
		return 0, 0, fmt.Errorf("list partitions: %w", err)
	}

	warmCutoff := time.Now().UTC().AddDate(0, -aw.cfg.WarmRetentionMonths, 0)

	var processed, failed int

	for i := range partitions {
		partition := &partitions[i]

		// A partition is eligible for cold archival when its entire date range
		// is older than the warm retention period.
		if !partition.RangeEnd.Before(warmCutoff) {
			continue
		}

		metadata, err := aw.getOrCreateMetadata(ctx, tenantID, partition)
		if err != nil {
			failed++

			logger.Log(ctx, libLog.LevelWarn, fmt.Sprintf("failed to get/create archive metadata for partition %s: %v", partition.Name, err))

			continue
		}

		// Skip already completed partitions.
		if metadata.Status == entities.StatusComplete {
			continue
		}

		if err := aw.archivePartition(ctx, metadata); err != nil {
			failed++

			logger.Log(ctx, libLog.LevelWarn, fmt.Sprintf("failed to archive partition %s: %v", partition.Name, err))

			continue
		}

		processed++
	}

	return processed, failed, nil
}

// getOrCreateMetadata retrieves existing archive metadata or creates a new record.
func (aw *ArchivalWorker) getOrCreateMetadata(
	ctx context.Context,
	tenantID uuid.UUID,
	partition *command.PartitionInfo,
) (*entities.ArchiveMetadata, error) {
	existing, err := aw.archiveRepo.GetByPartition(ctx, tenantID, partition.Name)
	if err == nil && existing != nil {
		return existing, nil
	}

	metadata, err := entities.NewArchiveMetadata(ctx, tenantID, partition.Name, partition.RangeStart, partition.RangeEnd)
	if err != nil {
		return nil, fmt.Errorf("create archive metadata: %w", err)
	}

	if err := aw.archiveRepo.Create(ctx, metadata); err != nil {
		return nil, fmt.Errorf("persist archive metadata: %w", err)
	}

	return metadata, nil
}

// archivePartition processes a partition through the archival state machine,
// advancing from the current state forward. Each state transition is persisted
// before proceeding. On error, the metadata is marked with the error and the
// function returns so the next cycle can retry.
//
// DESIGN CONSTRAINT: handlePartitionError always returns non-nil error.
// All callers MUST return immediately after handlePartitionError to ensure
// local variables (exportBuf, checksum) are not reused with stale data
// after metadata has been reloaded from the database.
//
//nolint:cyclop,gocyclo // state machine requires sequential checks for each state
func (aw *ArchivalWorker) archivePartition(ctx context.Context, metadata *entities.ArchiveMetadata) error {
	_, tracer := aw.tracking(ctx)

	ctx, span := tracer.Start(ctx, "governance.archival.archive_partition")
	defer span.End()

	span.SetAttributes(
		attribute.String("partition.name", metadata.PartitionName),
		attribute.String("partition.status", string(metadata.Status)),
	)

	// PENDING -> EXPORTING
	if metadata.Status == entities.StatusPending {
		if err := aw.transitionTo(ctx, metadata, metadata.MarkExporting); err != nil {
			return aw.handlePartitionError(ctx, metadata, "mark exporting", err)
		}
	}

	// EXPORTING -> EXPORTED: streaming is deferred to the UPLOADING phase where
	// it is coupled with the S3 upload via io.Pipe. This avoids buffering the
	// full partition in memory. We still advance the state-machine waypoint so
	// crash-recovery semantics (EXPORTED can resume at UPLOADING, UPLOADING can
	// re-stream) are preserved.
	if metadata.Status == entities.StatusExporting {
		if err := aw.transitionToExported(ctx, metadata, 0); err != nil {
			return aw.handlePartitionError(ctx, metadata, "mark exported", err)
		}
	}

	// EXPORTED -> UPLOADING
	if metadata.Status == entities.StatusExported {
		if err := aw.transitionTo(ctx, metadata, metadata.MarkUploading); err != nil {
			return aw.handlePartitionError(ctx, metadata, "mark uploading", err)
		}
	}

	// UPLOADING -> UPLOADED: stream partition rows directly into object storage.
	// This replaces the prior buffer-then-upload approach with io.Pipe so memory
	// stays O(chunk size) regardless of partition size. On crash recovery (re-entry
	// from UPLOADING) the same streaming path is used — there is no special-case.
	if metadata.Status == entities.StatusUploading {
		if err := aw.handleUploadingState(ctx, metadata); err != nil {
			return err
		}
	}

	// UPLOADED -> VERIFYING
	if metadata.Status == entities.StatusUploaded {
		if err := aw.transitionTo(ctx, metadata, metadata.MarkVerifying); err != nil {
			return aw.handlePartitionError(ctx, metadata, "mark verifying", err)
		}
	}

	// VERIFYING -> VERIFIED: checksum and row count verification
	if metadata.Status == entities.StatusVerifying {
		if err := aw.verifyArchive(ctx, metadata); err != nil {
			return aw.handlePartitionError(ctx, metadata, "verify archive", err)
		}

		if err := aw.transitionTo(ctx, metadata, metadata.MarkVerified); err != nil {
			return aw.handlePartitionError(ctx, metadata, "mark verified", err)
		}
	}

	// VERIFIED -> DETACHING: detach and drop partition
	if metadata.Status == entities.StatusVerified {
		if err := aw.transitionTo(ctx, metadata, metadata.MarkDetaching); err != nil {
			return aw.handlePartitionError(ctx, metadata, "mark detaching", err)
		}
	}

	// DETACHING -> COMPLETE: detach, drop, and mark complete
	if metadata.Status == entities.StatusDetaching {
		if err := aw.handleDetachingState(ctx, metadata); err != nil {
			return err
		}
	}

	return nil
}

// handleUploadingState handles the UPLOADING -> UPLOADED transition by streaming
// the partition rows directly into object storage via io.Pipe. Memory stays
// O(chunk size) regardless of partition row count. On crash-recovery re-entry,
// the same streaming path is used — there is no buffered-vs-reexport split.
func (aw *ArchivalWorker) handleUploadingState(
	ctx context.Context,
	metadata *entities.ArchiveMetadata,
) error {
	archiveKey, err := aw.archiveKey(metadata)
	if err != nil {
		return aw.handlePartitionError(ctx, metadata, "build archive key", err)
	}

	rowCount, checksum, compressedSize, err := aw.streamPartitionUpload(ctx, metadata, archiveKey)
	if err != nil {
		return aw.handlePartitionError(ctx, metadata, "stream and upload partition", err)
	}

	// Persist the row count that we only learned after consuming the stream.
	// The earlier MarkExported transition used 0 as a placeholder; update now so
	// downstream verification and operators see the real count.
	metadata.RowCount = rowCount

	if err := aw.transitionToUploaded(ctx, metadata, archiveKey, checksum, compressedSize); err != nil {
		return aw.handlePartitionError(ctx, metadata, "mark uploaded", err)
	}

	logger, _ := aw.tracking(ctx)

	logger.With(
		libLog.String("partition_name", metadata.PartitionName),
		libLog.String("archive_key", archiveKey),
		libLog.Any("row_count", rowCount),
		libLog.Any("compressed_size", compressedSize),
	).Log(ctx, libLog.LevelInfo, "uploaded archive for partition")

	return nil
}

// handleDetachingState handles the DETACHING -> COMPLETE transition.
// It detaches the partition, drops it, and marks the archive as complete.
func (aw *ArchivalWorker) handleDetachingState(
	ctx context.Context,
	metadata *entities.ArchiveMetadata,
) error {
	if err := aw.detachAndDrop(ctx, metadata); err != nil {
		return aw.handlePartitionError(ctx, metadata, "detach and drop partition", err)
	}

	if err := aw.transitionTo(ctx, metadata, metadata.MarkComplete); err != nil {
		return aw.handlePartitionError(ctx, metadata, "mark complete", err)
	}

	logger, _ := aw.tracking(ctx)

	logger.With(
		libLog.String("partition_name", metadata.PartitionName),
		libLog.Any("row_count", metadata.RowCount),
	).Log(ctx, libLog.LevelInfo, "partition archived")

	return nil
}

// transitionTo applies a state transition function and persists the result.
func (aw *ArchivalWorker) transitionTo(
	ctx context.Context,
	metadata *entities.ArchiveMetadata,
	transition func() error,
) error {
	if err := transition(); err != nil {
		return fmt.Errorf("state transition: %w", err)
	}

	if err := aw.archiveRepo.Update(ctx, metadata); err != nil {
		return fmt.Errorf("persist state transition: %w", err)
	}

	return nil
}

// transitionToExported applies the MarkExported transition with row count.
func (aw *ArchivalWorker) transitionToExported(
	ctx context.Context,
	metadata *entities.ArchiveMetadata,
	rowCount int64,
) error {
	if err := metadata.MarkExported(rowCount); err != nil {
		return fmt.Errorf("mark exported: %w", err)
	}

	if err := aw.archiveRepo.Update(ctx, metadata); err != nil {
		return fmt.Errorf("persist exported state: %w", err)
	}

	return nil
}

// transitionToUploaded applies the MarkUploaded transition with archive details.
func (aw *ArchivalWorker) transitionToUploaded(
	ctx context.Context,
	metadata *entities.ArchiveMetadata,
	archiveKey, checksum string,
	compressedSize int64,
) error {
	if err := metadata.MarkUploaded(archiveKey, checksum, compressedSize, aw.cfg.StorageClass); err != nil {
		return fmt.Errorf("mark uploaded: %w", err)
	}

	if err := aw.archiveRepo.Update(ctx, metadata); err != nil {
		return fmt.Errorf("persist uploaded state: %w", err)
	}

	return nil
}

func buildPartitionExportQuery(partitionName string) (string, error) {
	if !archivalPartitionNameRegex.MatchString(partitionName) {
		return "", fmt.Errorf("build partition export query: %w: %s", command.ErrInvalidPartitionName, partitionName)
	}

	var builder strings.Builder
	builder.Grow(len(partitionName) + partitionExportQueryOverhead)
	builder.WriteString("SELECT id, tenant_id, entity_type, entity_id, action, actor_id, changes, created_at, tenant_seq, prev_hash, record_hash, hash_version FROM ")
	builder.WriteString(auth.QuoteIdentifier(partitionName))
	builder.WriteString(" ORDER BY created_at")

	return builder.String(), nil
}
