// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package worker

import (
	"compress/gzip"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hash"
	"io"
	"time"

	"go.opentelemetry.io/otel/attribute"

	"github.com/LerianStudio/lib-commons/v5/commons/runtime"
	libS3 "github.com/LerianStudio/lib-commons/v5/commons/tenant-manager/s3"

	"github.com/LerianStudio/matcher/internal/auth"
	"github.com/LerianStudio/matcher/internal/governance/domain/entities"
	"github.com/LerianStudio/matcher/internal/governance/services/command"
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

// countingWriter wraps an io.Writer and counts the bytes written to it.
// Used to measure compressed archive size without buffering.
type countingWriter struct {
	w io.Writer
	n int64
}

func (cw *countingWriter) Write(p []byte) (int, error) {
	written, err := cw.w.Write(p)
	cw.n += int64(written)

	if err != nil {
		return written, fmt.Errorf("archival writer write: %w", err)
	}

	return written, nil
}

// streamPartitionUpload streams all rows from the partition through gzip
// compression into the object-storage upload via io.Pipe. The producer side
// runs in a goroutine; any error is propagated to the consumer via
// pw.CloseWithError so UploadWithOptions observes it and fails.
//
// Memory is bounded by the pipe's internal buffer plus the gzip window —
// typically a few hundred KB — regardless of partition row count. This
// replaces the prior approach that buffered the full gzipped partition in
// memory before uploading.
func (aw *ArchivalWorker) streamPartitionUpload(
	ctx context.Context,
	metadata *entities.ArchiveMetadata,
	archiveKey string,
) (int64, string, int64, error) {
	_, tracer := aw.tracking(ctx)

	ctx, span := tracer.Start(ctx, "governance.archival.stream_upload")
	defer span.End()

	span.SetAttributes(attribute.String("partition.name", metadata.PartitionName))

	result, err := withArchivalCurrentDBResult(ctx, aw, func(currentDB *sql.DB) (struct {
		rowCount       int64
		checksum       string
		compressedSize int64
	}, error,
	) {
		return streamPartitionViaPipe(ctx, aw, currentDB, metadata, archiveKey)
	})
	if err != nil {
		return 0, "", 0, err
	}

	span.SetAttributes(
		attribute.Int64("archival.row_count", result.rowCount),
		attribute.Int64("archival.compressed_bytes", result.compressedSize),
	)

	return result.rowCount, result.checksum, result.compressedSize, nil
}

// streamPartitionViaPipe runs the producer (DB query → gzip → pipe writer)
// and consumer (pipe reader → object storage) concurrently. The producer
// closes the pipe writer with any error so the uploader observes it.
func streamPartitionViaPipe(
	ctx context.Context,
	aw *ArchivalWorker,
	currentDB *sql.DB,
	metadata *entities.ArchiveMetadata,
	archiveKey string,
) (struct {
	rowCount       int64
	checksum       string
	compressedSize int64
}, error,
) {
	var zero struct {
		rowCount       int64
		checksum       string
		compressedSize int64
	}

	tx, err := currentDB.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return zero, fmt.Errorf("begin read transaction: %w", err)
	}

	defer func() { _ = tx.Rollback() }()

	if err := auth.ApplyTenantSchema(ctx, tx); err != nil {
		return zero, fmt.Errorf("apply tenant schema: %w", err)
	}

	query, err := buildPartitionExportQuery(metadata.PartitionName)
	if err != nil {
		return zero, err
	}

	//nolint:rowserrcheck // rows.Err() is checked inside encodePartitionRows after iteration completes.
	rows, err := tx.QueryContext(ctx, query)
	if err != nil {
		return zero, fmt.Errorf("query partition %s: %w", metadata.PartitionName, err)
	}

	defer rows.Close()

	pipeReader, pipeWriter := io.Pipe()

	counter := &countingWriter{w: pipeWriter}
	hasher := sha256.New()

	gzWriter, err := gzip.NewWriterLevel(counter, gzip.BestCompression)
	if err != nil {
		_ = pipeWriter.Close()

		return zero, fmt.Errorf("create gzip writer: %w", err)
	}

	type producerResult struct {
		rowCount int64
		err      error
	}

	producerCh := make(chan producerResult, 1)

	runtime.SafeGoWithContextAndComponent(
		ctx,
		aw.logger,
		"governance",
		"archival_worker.encode_partition_producer",
		runtime.KeepRunning,
		func(_ context.Context) {
			var (
				count     int64
				encodeErr error
			)
			// Deferred inside the passed function so the pipe always closes
			// and the consumer unblocks — even if a panic is recovered by the
			// SafeGo wrapper's outer defer.
			defer func() {
				// CloseWithError(nil) is equivalent to Close(); closing the
				// pipe writer signals EOF (or the error) to the uploader.
				_ = pipeWriter.CloseWithError(encodeErr)
				producerCh <- producerResult{rowCount: count, err: encodeErr}
			}()

			count, encodeErr = encodePartitionRows(rows, gzWriter, hasher)
			// Closing gzWriter flushes remaining compressed bytes to the pipe.
			if closeErr := gzWriter.Close(); encodeErr == nil {
				encodeErr = closeErr
			}
		},
	)

	_, uploadErr := aw.storage.UploadWithOptions(
		ctx,
		archiveKey,
		pipeReader,
		archiveContentType,
		sharedPorts.WithStorageClass(aw.cfg.StorageClass),
	)
	// Ensure the reader side is closed so the producer unblocks even if the
	// uploader returned early (e.g. auth failure without reading to EOF).
	_ = pipeReader.Close()

	prod := <-producerCh

	// Upload error takes precedence over producer error because a closed-pipe
	// producer error is typically a *consequence* of the uploader abandoning
	// the reader. Reporting the upload failure gives operators the actual
	// root cause.
	if uploadErr != nil {
		return zero, fmt.Errorf("upload archive: %w", uploadErr)
	}

	if prod.err != nil {
		return zero, fmt.Errorf("encode partition rows: %w", prod.err)
	}

	if err := tx.Commit(); err != nil {
		return zero, fmt.Errorf("commit read transaction: %w", err)
	}

	return struct {
		rowCount       int64
		checksum       string
		compressedSize int64
	}{
		rowCount:       prod.rowCount,
		checksum:       hex.EncodeToString(hasher.Sum(nil)),
		compressedSize: counter.n,
	}, nil
}

// encodePartitionRows iterates over all rows from a partition query, encodes each
// as a JSON line into the gzip writer, and computes a running checksum.
func encodePartitionRows(rows *sql.Rows, gzWriter *gzip.Writer, hasher hash.Hash) (int64, error) {
	encoder := json.NewEncoder(gzWriter)

	var rowCount int64

	for rows.Next() {
		row, err := scanAuditLogRow(rows)
		if err != nil {
			return 0, fmt.Errorf("scan audit log row: %w", err)
		}

		// Encode row as JSON and compute checksum on the uncompressed JSON line.
		jsonBytes, err := json.Marshal(row)
		if err != nil {
			return 0, fmt.Errorf("marshal audit log row: %w", err)
		}

		if _, err := hasher.Write(jsonBytes); err != nil {
			return 0, fmt.Errorf("hash audit log row: %w", err)
		}

		if _, err := hasher.Write([]byte("\n")); err != nil {
			return 0, fmt.Errorf("hash newline: %w", err)
		}

		if err := encoder.Encode(row); err != nil {
			return 0, fmt.Errorf("encode audit log row to gzip: %w", err)
		}

		rowCount++
	}

	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("iterate partition rows: %w", err)
	}

	return rowCount, nil
}

// auditLogRow represents a single audit log record for JSON-lines export.
type auditLogRow struct {
	ID          string  `json:"id"`
	TenantID    string  `json:"tenant_id"`
	EntityType  string  `json:"entity_type"`
	EntityID    string  `json:"entity_id"`
	Action      string  `json:"action"`
	ActorID     *string `json:"actor_id,omitempty"`
	Changes     *string `json:"changes,omitempty"`
	CreatedAt   string  `json:"created_at"`
	TenantSeq   *int64  `json:"tenant_seq,omitempty"`
	PrevHash    *string `json:"prev_hash,omitempty"`
	RecordHash  *string `json:"record_hash,omitempty"`
	HashVersion *int    `json:"hash_version,omitempty"`
}

// scanAuditLogRow scans a single row from the audit_logs partition query.
func scanAuditLogRow(rows *sql.Rows) (*auditLogRow, error) {
	var row auditLogRow

	var createdAt time.Time

	if err := rows.Scan(
		&row.ID,
		&row.TenantID,
		&row.EntityType,
		&row.EntityID,
		&row.Action,
		&row.ActorID,
		&row.Changes,
		&createdAt,
		&row.TenantSeq,
		&row.PrevHash,
		&row.RecordHash,
		&row.HashVersion,
	); err != nil {
		return nil, fmt.Errorf("scan row: %w", err)
	}

	row.CreatedAt = createdAt.UTC().Format(time.RFC3339Nano)

	return &row, nil
}

// archiveKey generates the object storage key for an archive.
// Format: {tenant_id}/{prefix}/{year}/{month}/{archive_id}/audit_logs_{YYYY_MM}.jsonl.gz
//
// The archive_id UUID segment prevents path enumeration attacks: even if a
// presigned URL for one archive leaks, an attacker cannot guess paths for
// other tenants' or months' archives without knowing the per-archive UUID.
func (aw *ArchivalWorker) archiveKey(metadata *entities.ArchiveMetadata) (string, error) {
	year, month, _ := metadata.DateRangeStart.Date()

	originalKey := fmt.Sprintf("%s/%04d/%02d/%s/audit_logs_%04d_%02d.jsonl.gz",
		aw.cfg.StoragePrefix,
		year,
		month,
		metadata.ID.String(),
		year,
		month,
	)

	key, err := libS3.GetObjectStorageKey(metadata.TenantID.String(), originalKey)
	if err != nil {
		return "", fmt.Errorf("build scoped archive storage key: %w", err)
	}

	return key, nil
}

func withArchivalCurrentDBResult[T any](ctx context.Context, aw *ArchivalWorker, fn func(*sql.DB) (T, error)) (T, error) {
	var zero T
	if aw == nil {
		return zero, command.ErrNilDB
	}

	if _, hasExplicitTenant := auth.LookupTenantID(ctx); !hasExplicitTenant {
		if aw.db == nil {
			return zero, command.ErrNilDB
		}

		return fn(aw.db)
	}

	if aw.infraProvider != nil {
		return withArchivalProviderDBResult(ctx, aw.infraProvider, fn)
	}

	if aw.db == nil {
		return zero, command.ErrNilDB
	}

	return fn(aw.db)
}

func withArchivalProviderDBResult[T any](
	ctx context.Context,
	infraProvider sharedPorts.InfrastructureProvider,
	fn func(*sql.DB) (T, error),
) (T, error) {
	var zero T

	lease, err := infraProvider.GetPrimaryDB(ctx)
	if err != nil {
		return zero, fmt.Errorf("resolve primary postgres db: %w", err)
	}

	if lease == nil {
		return zero, command.ErrNilDB
	}
	defer lease.Release()

	db := lease.DB()
	if db == nil {
		return zero, command.ErrNilDB
	}

	return fn(db)
}
