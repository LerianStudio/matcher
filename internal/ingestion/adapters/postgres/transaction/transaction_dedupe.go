// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package transaction

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/google/uuid"
	"github.com/lib/pq"

	libCommons "github.com/LerianStudio/lib-commons/v5/commons"
	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v5/commons/opentelemetry"

	pgcommon "github.com/LerianStudio/matcher/internal/ingestion/adapters/postgres/common"
	repositories "github.com/LerianStudio/matcher/internal/ingestion/domain/repositories"
)

// ExistsBySourceAndExternalID checks if a transaction exists by source ID and external ID.
func (repo *Repository) ExistsBySourceAndExternalID(
	ctx context.Context,
	sourceID uuid.UUID,
	externalID string,
) (bool, error) {
	if repo == nil || repo.provider == nil {
		return false, errTxRepoNotInit
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	ctx, span := tracer.Start(ctx, "postgres.exists_transaction_by_external_id")

	defer span.End()

	exists, err := pgcommon.WithTenantTxProvider(
		ctx,
		repo.provider,
		func(tx *sql.Tx) (bool, error) {
			row := tx.QueryRowContext(
				ctx,
				"SELECT EXISTS (SELECT 1 FROM transactions WHERE source_id = $1 AND external_id = $2)",
				sourceID.String(),
				externalID,
			)

			var result bool

			if err := row.Scan(&result); err != nil {
				return false, fmt.Errorf("failed to scan result: %w", err)
			}

			return result, nil
		},
	)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to check transaction existence", err)

		logger.With(libLog.Err(err)).Log(ctx, libLog.LevelError, "failed to check transaction existence")

		return false, fmt.Errorf("failed to check transaction existence: %w", err)
	}

	return exists, nil
}

// ExistsBulkBySourceAndExternalID checks existence for multiple source/external ID pairs in a single query.
func (repo *Repository) ExistsBulkBySourceAndExternalID(
	ctx context.Context,
	keys []repositories.ExternalIDKey,
) (map[repositories.ExternalIDKey]bool, error) {
	if repo == nil || repo.provider == nil {
		return nil, errTxRepoNotInit
	}

	if len(keys) == 0 {
		return make(map[repositories.ExternalIDKey]bool), nil
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	ctx, span := tracer.Start(ctx, "postgres.exists_bulk_transaction_by_external_id")

	defer span.End()

	sourceIDs := make([]string, len(keys))
	externalIDs := make([]string, len(keys))

	for i, key := range keys {
		sourceIDs[i] = key.SourceID.String()
		externalIDs[i] = key.ExternalID
	}

	result, err := pgcommon.WithTenantTxProvider(
		ctx,
		repo.provider,
		func(tx *sql.Tx) (map[repositories.ExternalIDKey]bool, error) {
			existsMap := make(map[repositories.ExternalIDKey]bool, len(keys))

			const query = `
				SELECT t.source_id, t.external_id
				FROM transactions t
				INNER JOIN unnest($1::uuid[], $2::text[]) AS v(source_id, external_id)
				ON t.source_id = v.source_id AND t.external_id = v.external_id
			`

			rows, err := tx.QueryContext(ctx, query, pq.Array(sourceIDs), pq.Array(externalIDs))
			if err != nil {
				return nil, fmt.Errorf("failed to query existing transactions: %w", err)
			}
			defer rows.Close()

			for rows.Next() {
				var (
					sourceID   uuid.UUID
					externalID string
				)

				if err := rows.Scan(&sourceID, &externalID); err != nil {
					return nil, fmt.Errorf("failed to scan row: %w", err)
				}

				existsMap[repositories.ExternalIDKey{SourceID: sourceID, ExternalID: externalID}] = true
			}

			if err := rows.Err(); err != nil {
				return nil, fmt.Errorf("failed to iterate rows: %w", err)
			}

			return existsMap, nil
		},
	)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to check bulk transaction existence", err)

		logger.With(libLog.Err(err)).Log(ctx, libLog.LevelError, "failed to check bulk transaction existence")

		return nil, fmt.Errorf("failed to check bulk transaction existence: %w", err)
	}

	return result, nil
}
