// Copyright 2026 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package worker

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/LerianStudio/matcher/internal/governance/domain/entities"
	"github.com/LerianStudio/matcher/internal/streaming/emission"
)

func (aw *ArchivalWorker) emitArchiveEvent(ctx context.Context, tx *sql.Tx, definitionKey, subject string, metadata *entities.ArchiveMetadata) error {
	if metadata == nil {
		return nil
	}

	payload := map[string]any{
		"archive_metadata_id": metadata.ID.String(),
		"tenant_id":           metadata.TenantID.String(),
		"partition_name":      metadata.PartitionName,
		"date_range_start":    formatArchiveTime(metadata.DateRangeStart),
		"date_range_end":      formatArchiveTime(metadata.DateRangeEnd),
		"status":              string(metadata.Status),
		"created_at":          formatArchiveTime(metadata.CreatedAt),
		"updated_at":          formatArchiveTime(metadata.UpdatedAt),
	}

	if metadata.Checksum != "" {
		payload["checksum"] = metadata.Checksum
	}

	if metadata.RowCount > 0 {
		payload["row_count"] = metadata.RowCount
	}

	if metadata.CompressedSizeBytes > 0 {
		payload["compressed_size_bytes"] = metadata.CompressedSizeBytes
	}

	if metadata.StorageClass != "" {
		payload["storage_class"] = metadata.StorageClass
	}

	if metadata.ArchivedAt != nil {
		payload["archived_at"] = formatArchiveTime(*metadata.ArchivedAt)
	}

	options := []emission.Option{emission.RequireOutboxTx()}
	if tx != nil {
		options = append(options, emission.WithOutboxTx(tx))
	}

	if err := emission.Emit(ctx, aw.streamEmitter, definitionKey, subject, payload, options...); err != nil {
		return fmt.Errorf("emit archive streaming event %s: %w", definitionKey, err)
	}

	return nil
}

// formatArchiveTime delegates to emission.FormatTime; preserved as a thin
// wrapper for backward compatibility with existing unit tests.
func formatArchiveTime(value time.Time) string {
	return emission.FormatTime(value)
}
