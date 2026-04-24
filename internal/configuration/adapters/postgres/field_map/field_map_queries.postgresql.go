// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package field_map

import (
	stdctx "context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/google/uuid"

	libCommons "github.com/LerianStudio/lib-commons/v5/commons"
	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v5/commons/opentelemetry"

	"github.com/LerianStudio/matcher/internal/configuration/adapters/postgres/common"
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
)

// FindByID retrieves a field map by its ID.
func (repo *Repository) FindByID(ctx stdctx.Context, id uuid.UUID) (*shared.FieldMap, error) {
	if repo == nil || repo.provider == nil {
		return nil, ErrRepoNotInitialized
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "postgres.find_field_map_by_id")
	defer span.End()

	result, err := common.WithTenantReadQuery(
		ctx,
		repo.provider,
		func(qe common.QueryExecutor) (*shared.FieldMap, error) {
			row := qe.QueryRowContext(
				ctx,
				"SELECT "+fieldMapColumns+" FROM field_maps WHERE id = $1",
				id.String(),
			)

			return scanFieldMap(row)
		},
	)
	if err != nil {
		if !errors.Is(err, sql.ErrNoRows) {
			libOpentelemetry.HandleSpanError(span, "failed to find field map by id", err)

			logger.With(libLog.Err(err)).Log(ctx, libLog.LevelError, "failed to find field map by id")
		}

		return nil, fmt.Errorf("find field map by id: %w", err)
	}

	return result, nil
}

// FindBySourceID retrieves a field map by its source ID.
func (repo *Repository) FindBySourceID(
	ctx stdctx.Context,
	sourceID uuid.UUID,
) (*shared.FieldMap, error) {
	if repo == nil || repo.provider == nil {
		return nil, ErrRepoNotInitialized
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "postgres.find_field_map_by_source")
	defer span.End()

	result, err := common.WithTenantReadQuery(
		ctx,
		repo.provider,
		func(qe common.QueryExecutor) (*shared.FieldMap, error) {
			row := qe.QueryRowContext(
				ctx,
				"SELECT "+fieldMapColumns+" FROM field_maps WHERE source_id = $1 ORDER BY version DESC LIMIT 1",
				sourceID.String(),
			)

			return scanFieldMap(row)
		},
	)
	if err != nil {
		if !errors.Is(err, sql.ErrNoRows) {
			libOpentelemetry.HandleSpanError(span, "failed to find field map by source", err)

			logger.With(libLog.Err(err)).Log(ctx, libLog.LevelError, "failed to find field map by source")
		}

		return nil, fmt.Errorf("find field map by source: %w", err)
	}

	return result, nil
}

// FindBySourceIDWithTx retrieves a field map by its source ID using an existing transaction.
// This enables consistent snapshot reads when the caller already holds a transaction.
func (repo *Repository) FindBySourceIDWithTx(
	ctx stdctx.Context,
	tx *sql.Tx,
	sourceID uuid.UUID,
) (*shared.FieldMap, error) {
	if repo == nil || repo.provider == nil {
		return nil, ErrRepoNotInitialized
	}

	if tx == nil {
		return nil, ErrTransactionRequired
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "postgres.find_field_map_by_source_with_tx")
	defer span.End()

	result, err := common.WithTenantTxOrExistingProvider(
		ctx,
		repo.provider,
		tx,
		func(innerTx *sql.Tx) (*shared.FieldMap, error) {
			row := innerTx.QueryRowContext(
				ctx,
				"SELECT "+fieldMapColumns+" FROM field_maps WHERE source_id = $1 ORDER BY version DESC LIMIT 1",
				sourceID.String(),
			)

			return scanFieldMap(row)
		},
	)
	if err != nil {
		if !errors.Is(err, sql.ErrNoRows) {
			libOpentelemetry.HandleSpanError(span, "failed to find field map by source with tx", err)

			logger.With(libLog.Err(err)).Log(ctx, libLog.LevelError, "failed to find field map by source with tx")
		}

		return nil, fmt.Errorf("find field map by source with tx: %w", err)
	}

	return result, nil
}

// ExistsBySourceIDsWithTx checks which source IDs have field maps using an existing transaction.
// This enables consistent snapshot reads when the caller already holds a transaction.
func (repo *Repository) ExistsBySourceIDsWithTx(
	ctx stdctx.Context,
	tx *sql.Tx,
	sourceIDs []uuid.UUID,
) (map[uuid.UUID]bool, error) {
	if repo == nil || repo.provider == nil {
		return nil, ErrRepoNotInitialized
	}

	if tx == nil {
		return nil, ErrTransactionRequired
	}

	result := make(map[uuid.UUID]bool, len(sourceIDs))
	if len(sourceIDs) == 0 {
		return result, nil
	}

	deduped := dedupeSourceIDs(sourceIDs)

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "postgres.exists_field_maps_by_source_ids_with_tx")
	defer span.End()

	result, err := common.WithTenantTxOrExistingProvider(
		ctx,
		repo.provider,
		tx,
		func(innerTx *sql.Tx) (map[uuid.UUID]bool, error) {
			existsMap := make(map[uuid.UUID]bool, len(deduped))

			for start := 0; start < len(deduped); start += existsBySourceIDsBatchSize {
				end := min(start+existsBySourceIDsBatchSize, len(deduped))

				batch := deduped[start:end]

				if err := repo.existsBySourceIDsBatch(ctx, innerTx, batch, existsMap); err != nil {
					return nil, err
				}
			}

			return existsMap, nil
		},
	)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to check field maps existence with tx", err)

		logger.With(libLog.Err(err)).Log(ctx, libLog.LevelError, "failed to check field maps existence with tx")

		return nil, fmt.Errorf("check field maps existence by source ids with tx: %w", err)
	}

	return result, nil
}

// ExistsBySourceIDs checks which source IDs have field maps and returns a map.
// The input is processed in batches of existsBySourceIDsBatchSize to prevent
// Postgres parameter limit issues and query planner degradation with large IN clauses.
func (repo *Repository) ExistsBySourceIDs(
	ctx stdctx.Context,
	sourceIDs []uuid.UUID,
) (map[uuid.UUID]bool, error) {
	if repo == nil || repo.provider == nil {
		return nil, ErrRepoNotInitialized
	}

	result := make(map[uuid.UUID]bool, len(sourceIDs))
	if len(sourceIDs) == 0 {
		return result, nil
	}

	deduped := dedupeSourceIDs(sourceIDs)

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "postgres.exists_field_maps_by_source_ids")
	defer span.End()

	result, err := common.WithTenantReadQuery(
		ctx,
		repo.provider,
		func(qe common.QueryExecutor) (map[uuid.UUID]bool, error) {
			existsMap := make(map[uuid.UUID]bool, len(deduped))

			for start := 0; start < len(deduped); start += existsBySourceIDsBatchSize {
				end := min(start+existsBySourceIDsBatchSize, len(deduped))

				batch := deduped[start:end]

				if err := repo.existsBySourceIDsBatch(ctx, qe, batch, existsMap); err != nil {
					return nil, err
				}
			}

			return existsMap, nil
		},
	)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to check field maps existence", err)

		logger.With(libLog.Err(err)).Log(ctx, libLog.LevelError, "failed to check field maps existence")

		return nil, fmt.Errorf("check field maps existence by source ids: %w", err)
	}

	return result, nil
}

// dedupeSourceIDs removes duplicate source IDs to reduce query size.
func dedupeSourceIDs(sourceIDs []uuid.UUID) []uuid.UUID {
	seen := make(map[uuid.UUID]struct{}, len(sourceIDs))
	result := make([]uuid.UUID, 0, len(sourceIDs))

	for _, id := range sourceIDs {
		if _, exists := seen[id]; !exists {
			seen[id] = struct{}{}
			result = append(result, id)
		}
	}

	return result
}

// existsBySourceIDsBatch executes a single batch query for ExistsBySourceIDs.
func (repo *Repository) existsBySourceIDsBatch(
	ctx stdctx.Context,
	qe common.QueryExecutor,
	batch []uuid.UUID,
	existsMap map[uuid.UUID]bool,
) (err error) {
	args := make([]any, len(batch))
	for i, id := range batch {
		args[i] = id.String()
	}

	query := "SELECT DISTINCT source_id FROM field_maps WHERE source_id IN (" + joinPlaceholders(
		len(batch),
	) + ")" // #nosec G202 -- placeholders are generated safely

	rows, err := qe.QueryContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("query existing source ids: %w", err)
	}

	defer func() {
		if closeErr := rows.Close(); closeErr != nil && err == nil {
			err = closeErr
		}
	}()

	for rows.Next() {
		var sourceID uuid.UUID
		if err := rows.Scan(&sourceID); err != nil {
			return err
		}

		existsMap[sourceID] = true
	}

	return rows.Err()
}

// joinPlaceholders creates placeholder string like "$1, $2, $3" for count parameters.
func joinPlaceholders(count int) string {
	if count <= 0 {
		return ""
	}

	var builder strings.Builder

	builder.WriteString("$1")

	for i := 2; i <= count; i++ {
		builder.WriteString(", $")
		builder.WriteString(strconv.Itoa(i))
	}

	return builder.String()
}
