// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package worker

import (
	"bufio"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"

	"github.com/google/uuid"

	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v5/commons/opentelemetry"

	"github.com/LerianStudio/matcher/internal/governance/domain/entities"
)

// verifyArchive downloads the archive from object storage, decompresses it,
// re-computes the SHA-256 checksum while counting JSONL lines, and compares
// both values against metadata. This ensures corrupted or partial uploads are
// caught before the source partition is detached.
func (aw *ArchivalWorker) verifyArchive(
	ctx context.Context,
	metadata *entities.ArchiveMetadata,
) error {
	if metadata.Checksum == "" {
		return fmt.Errorf("%w: checksum is empty", ErrChecksumMismatch)
	}

	if metadata.RowCount <= 0 {
		return fmt.Errorf("%w: row count is %d", ErrRowCountMismatch, metadata.RowCount)
	}

	// Download the archive from object storage.
	reader, err := aw.storage.Download(ctx, metadata.ArchiveKey)
	if err != nil {
		return fmt.Errorf("download archive for verification: %w", err)
	}

	defer reader.Close()

	// Decompress the gzip stream.
	gzReader, err := gzip.NewReader(reader)
	if err != nil {
		return fmt.Errorf("open gzip reader for verification: %w", err)
	}

	defer gzReader.Close()

	// Stream through SHA-256 hasher while counting JSONL lines.
	hasher := sha256.New()

	var lineCount int64

	scanner := bufio.NewScanner(gzReader)
	// Audit log JSONL lines may exceed the default 64KB token size when changes
	// payloads contain large diffs. Use a 1MB buffer to match practical limits.
	const maxVerificationLineSize = 1 << 20 // 1MB
	scanner.Buffer(make([]byte, 0, maxVerificationLineSize), maxVerificationLineSize)

	for scanner.Scan() {
		line := scanner.Bytes()

		// Write the JSON line + newline to the hasher, matching the
		// encoding pattern used during export (json bytes + "\n").
		if _, err := hasher.Write(line); err != nil {
			return fmt.Errorf("hash verification line: %w", err)
		}

		if _, err := hasher.Write([]byte("\n")); err != nil {
			return fmt.Errorf("hash verification newline: %w", err)
		}

		lineCount++
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("scan archive for verification: %w", err)
	}

	// Compare computed checksum against stored value.
	computedChecksum := hex.EncodeToString(hasher.Sum(nil))
	if computedChecksum != metadata.Checksum {
		return fmt.Errorf("%w: expected %s, got %s", ErrChecksumMismatch, metadata.Checksum, computedChecksum)
	}

	// Compare line count against stored row count.
	if lineCount != metadata.RowCount {
		return fmt.Errorf("%w: expected %d rows, got %d", ErrRowCountMismatch, metadata.RowCount, lineCount)
	}

	return nil
}

// detachAndDrop detaches the partition from the parent table and drops it.
func (aw *ArchivalWorker) detachAndDrop(ctx context.Context, metadata *entities.ArchiveMetadata) error {
	if err := aw.partitionMgr.DetachPartition(ctx, metadata.PartitionName); err != nil {
		return fmt.Errorf("detach partition: %w", err)
	}

	if err := aw.partitionMgr.DropPartition(ctx, metadata.PartitionName); err != nil {
		return fmt.Errorf("drop partition: %w", err)
	}

	return nil
}

// handlePartitionError marks the metadata with the error, persists the state,
// and reloads the metadata from the database to prevent in-memory state
// corruption (e.g., when a transition mutated in-memory state but the
// subsequent DB persist failed).
func (aw *ArchivalWorker) handlePartitionError(
	ctx context.Context,
	metadata *entities.ArchiveMetadata,
	operation string,
	err error,
) error {
	_, tracer := aw.tracking(ctx)

	_, span := tracer.Start(ctx, "governance.archival.handle_error")
	defer span.End()

	wrappedErr := fmt.Errorf("%s: %w", operation, err)

	libOpentelemetry.HandleSpanError(span, "archival partition error", wrappedErr)

	metadata.MarkError(wrappedErr.Error())

	if updateErr := aw.archiveRepo.Update(ctx, metadata); updateErr != nil {
		aw.logger.With(
			libLog.String("partition_name", metadata.PartitionName),
			libLog.Err(updateErr),
		).Log(ctx, libLog.LevelError, "failed to persist error state for partition")
	}

	// Reload metadata from DB to ensure in-memory state matches persisted state.
	// This guards against scenarios where a state transition mutated the in-memory
	// object but the DB persist failed, leaving the pointer in an inconsistent state.
	if metadata.ID != uuid.Nil {
		reloaded, reloadErr := aw.archiveRepo.GetByID(ctx, metadata.ID)

		switch {
		case reloadErr == nil && reloaded != nil:
			*metadata = *reloaded
		case reloadErr != nil && aw.logger != nil:
			aw.logger.With(
				libLog.String("partition_name", metadata.PartitionName),
				libLog.Err(reloadErr),
			).Log(ctx, libLog.LevelWarn, "failed to reload metadata for partition after error")
		}
	}

	return wrappedErr
}
