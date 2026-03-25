package field_map

import (
	stdctx "context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	libCommons "github.com/LerianStudio/lib-commons/v4/commons"
	libLog "github.com/LerianStudio/lib-commons/v4/commons/log"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v4/commons/opentelemetry"

	"github.com/LerianStudio/matcher/internal/configuration/adapters/postgres/common"
	"github.com/LerianStudio/matcher/internal/configuration/domain/entities"
	"github.com/LerianStudio/matcher/internal/shared/ports"
)

const fieldMapColumns = "id, context_id, source_id, mapping, version, created_at, updated_at"

// existsBySourceIDsBatchSize limits the number of source IDs per IN clause query.
// This protects against Postgres parameter limits (max ~32767) and prevents
// query planner degradation with very large IN clauses. The value is chosen
// conservatively to ensure good performance across typical workloads.
const existsBySourceIDsBatchSize = 1000

// Repository provides PostgreSQL operations for field maps.
type Repository struct {
	provider ports.InfrastructureProvider
}

// NewRepository creates a new field map repository.
func NewRepository(provider ports.InfrastructureProvider) *Repository {
	return &Repository{provider: provider}
}

// Create inserts a new field map into the database.
func (repo *Repository) Create(
	ctx stdctx.Context,
	entity *entities.FieldMap,
) (*entities.FieldMap, error) {
	if repo == nil || repo.provider == nil {
		return nil, ErrRepoNotInitialized
	}

	if entity == nil {
		return nil, ErrFieldMapEntityRequired
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "repository.field_map.create")
	defer span.End()

	result, err := common.WithTenantTxProvider(
		ctx,
		repo.provider,
		func(tx *sql.Tx) (*entities.FieldMap, error) {
			return repo.executeCreate(ctx, tx, entity)
		},
	)
	if err != nil {
		wrappedErr := fmt.Errorf("create field map: %w", err)
		libOpentelemetry.HandleSpanError(span, "failed to create field map", wrappedErr)

		logger.With(libLog.Any("error", wrappedErr.Error())).Log(ctx, libLog.LevelError, "failed to create field map")

		return nil, wrappedErr
	}

	return result, nil
}

// CreateWithTx inserts a new field map using the provided transaction.
func (repo *Repository) CreateWithTx(
	ctx stdctx.Context,
	tx *sql.Tx,
	entity *entities.FieldMap,
) (*entities.FieldMap, error) {
	if repo == nil || repo.provider == nil {
		return nil, ErrRepoNotInitialized
	}

	if entity == nil {
		return nil, ErrFieldMapEntityRequired
	}

	if tx == nil {
		return nil, ErrTransactionRequired
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "postgres.create_field_map_with_tx")
	defer span.End()

	result, err := common.WithTenantTxOrExistingProvider(
		ctx,
		repo.provider,
		tx,
		func(innerTx *sql.Tx) (*entities.FieldMap, error) {
			return repo.executeCreate(ctx, innerTx, entity)
		},
	)
	if err != nil {
		wrappedErr := fmt.Errorf("create field map with tx: %w", err)
		libOpentelemetry.HandleSpanError(span, "failed to create field map", wrappedErr)

		logger.With(libLog.Any("error", wrappedErr.Error())).Log(ctx, libLog.LevelError, "failed to create field map")

		return nil, wrappedErr
	}

	return result, nil
}

// executeCreate performs the actual field map creation within a transaction.
func (repo *Repository) executeCreate(
	ctx stdctx.Context,
	tx *sql.Tx,
	entity *entities.FieldMap,
) (*entities.FieldMap, error) {
	model, err := NewFieldMapPostgreSQLModel(entity)
	if err != nil {
		return nil, err
	}

	_, err = tx.ExecContext(
		ctx,
		`INSERT INTO field_maps (id, context_id, source_id, mapping, version, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		model.ID,
		model.ContextID,
		model.SourceID,
		model.Mapping,
		model.Version,
		model.CreatedAt,
		model.UpdatedAt,
	)
	if err != nil {
		return nil, err
	}

	return model.ToEntity()
}

// FindByID retrieves a field map by its ID.
func (repo *Repository) FindByID(ctx stdctx.Context, id uuid.UUID) (*entities.FieldMap, error) {
	if repo == nil || repo.provider == nil {
		return nil, ErrRepoNotInitialized
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "postgres.find_field_map_by_id")
	defer span.End()

	result, err := common.WithTenantTxProvider(
		ctx,
		repo.provider,
		func(tx *sql.Tx) (*entities.FieldMap, error) {
			row := tx.QueryRowContext(
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

			logger.With(libLog.Any("error", err.Error())).Log(ctx, libLog.LevelError, "failed to find field map by id")
		}

		return nil, fmt.Errorf("find field map by id: %w", err)
	}

	return result, nil
}

// FindBySourceID retrieves a field map by its source ID.
func (repo *Repository) FindBySourceID(
	ctx stdctx.Context,
	sourceID uuid.UUID,
) (*entities.FieldMap, error) {
	if repo == nil || repo.provider == nil {
		return nil, ErrRepoNotInitialized
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "postgres.find_field_map_by_source")
	defer span.End()

	result, err := common.WithTenantTxProvider(
		ctx,
		repo.provider,
		func(tx *sql.Tx) (*entities.FieldMap, error) {
			row := tx.QueryRowContext(
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

			logger.With(libLog.Any("error", err.Error())).Log(ctx, libLog.LevelError, "failed to find field map by source")
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
) (*entities.FieldMap, error) {
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
		func(innerTx *sql.Tx) (*entities.FieldMap, error) {
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

			logger.With(libLog.Any("error", err.Error())).Log(ctx, libLog.LevelError, "failed to find field map by source with tx")
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

		logger.With(libLog.Any("error", err.Error())).Log(ctx, libLog.LevelError, "failed to check field maps existence with tx")

		return nil, fmt.Errorf("check field maps existence by source ids with tx: %w", err)
	}

	return result, nil
}

// Update modifies an existing field map.
func (repo *Repository) Update(
	ctx stdctx.Context,
	entity *entities.FieldMap,
) (*entities.FieldMap, error) {
	if repo == nil || repo.provider == nil {
		return nil, ErrRepoNotInitialized
	}

	if entity == nil {
		return nil, ErrFieldMapEntityRequired
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "postgres.update_field_map")
	defer span.End()

	entity.UpdatedAt = time.Now().UTC()

	result, err := common.WithTenantTxProvider(
		ctx,
		repo.provider,
		func(tx *sql.Tx) (*entities.FieldMap, error) {
			return repo.executeUpdate(ctx, tx, entity)
		},
	)
	if err != nil {
		wrappedErr := fmt.Errorf("update field map: %w", err)

		if !errors.Is(err, sql.ErrNoRows) {
			libOpentelemetry.HandleSpanError(span, "failed to update field map", wrappedErr)

			logger.With(libLog.Any("error", wrappedErr.Error())).Log(ctx, libLog.LevelError, "failed to update field map")
		}

		return nil, wrappedErr
	}

	return result, nil
}

// UpdateWithTx modifies an existing field map using the provided transaction.
func (repo *Repository) UpdateWithTx(
	ctx stdctx.Context,
	tx *sql.Tx,
	entity *entities.FieldMap,
) (*entities.FieldMap, error) {
	if repo == nil || repo.provider == nil {
		return nil, ErrRepoNotInitialized
	}

	if entity == nil {
		return nil, ErrFieldMapEntityRequired
	}

	if tx == nil {
		return nil, ErrTransactionRequired
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "postgres.update_field_map_with_tx")
	defer span.End()

	entity.UpdatedAt = time.Now().UTC()

	result, err := common.WithTenantTxOrExistingProvider(
		ctx,
		repo.provider,
		tx,
		func(innerTx *sql.Tx) (*entities.FieldMap, error) {
			return repo.executeUpdate(ctx, innerTx, entity)
		},
	)
	if err != nil {
		wrappedErr := fmt.Errorf("update field map with tx: %w", err)

		if !errors.Is(err, sql.ErrNoRows) {
			libOpentelemetry.HandleSpanError(span, "failed to update field map", wrappedErr)

			logger.With(libLog.Any("error", wrappedErr.Error())).Log(ctx, libLog.LevelError, "failed to update field map")
		}

		return nil, wrappedErr
	}

	return result, nil
}

// executeUpdate performs the actual field map update within a transaction.
func (repo *Repository) executeUpdate(
	ctx stdctx.Context,
	tx *sql.Tx,
	entity *entities.FieldMap,
) (*entities.FieldMap, error) {
	model, err := NewFieldMapPostgreSQLModel(entity)
	if err != nil {
		return nil, err
	}

	result, err := tx.ExecContext(
		ctx,
		`UPDATE field_maps SET mapping = $1, version = $2, updated_at = $3 WHERE id = $4`,
		model.Mapping,
		model.Version,
		model.UpdatedAt,
		model.ID,
	)
	if err != nil {
		return nil, err
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return nil, err
	}

	if rowsAffected == 0 {
		return nil, sql.ErrNoRows
	}

	return model.ToEntity()
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

	result, err := common.WithTenantTxProvider(
		ctx,
		repo.provider,
		func(tx *sql.Tx) (map[uuid.UUID]bool, error) {
			existsMap := make(map[uuid.UUID]bool, len(deduped))

			for start := 0; start < len(deduped); start += existsBySourceIDsBatchSize {
				end := min(start+existsBySourceIDsBatchSize, len(deduped))

				batch := deduped[start:end]

				if err := repo.existsBySourceIDsBatch(ctx, tx, batch, existsMap); err != nil {
					return nil, err
				}
			}

			return existsMap, nil
		},
	)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to check field maps existence", err)

		logger.With(libLog.Any("error", err.Error())).Log(ctx, libLog.LevelError, "failed to check field maps existence")

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
	tx *sql.Tx,
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

	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return err
	}

	defer func() {
		if closeErr := rows.Close(); closeErr != nil && err == nil {
			err = closeErr
		}
	}()

	for rows.Next() {
		var sourceIDStr string
		if err := rows.Scan(&sourceIDStr); err != nil {
			return err
		}

		sourceID, err := uuid.Parse(sourceIDStr)
		if err != nil {
			return fmt.Errorf("failed to parse source ID: %w", err)
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

// Delete removes a field map from the database.
func (repo *Repository) Delete(ctx stdctx.Context, id uuid.UUID) error {
	if repo == nil || repo.provider == nil {
		return ErrRepoNotInitialized
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "postgres.delete_field_map")
	defer span.End()

	_, err := common.WithTenantTxProvider(ctx, repo.provider, func(tx *sql.Tx) (bool, error) {
		return repo.executeDelete(ctx, tx, id)
	})
	if err != nil {
		wrappedErr := fmt.Errorf("delete field map: %w", err)

		if !errors.Is(err, sql.ErrNoRows) {
			libOpentelemetry.HandleSpanError(span, "failed to delete field map", wrappedErr)

			logger.With(libLog.Any("error", wrappedErr.Error())).Log(ctx, libLog.LevelError, "failed to delete field map")
		}

		return wrappedErr
	}

	return nil
}

// DeleteWithTx removes a field map using the provided transaction.
func (repo *Repository) DeleteWithTx(ctx stdctx.Context, tx *sql.Tx, id uuid.UUID) error {
	if repo == nil || repo.provider == nil {
		return ErrRepoNotInitialized
	}

	if tx == nil {
		return ErrTransactionRequired
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "postgres.delete_field_map_with_tx")
	defer span.End()

	_, err := common.WithTenantTxOrExistingProvider(
		ctx,
		repo.provider,
		tx,
		func(innerTx *sql.Tx) (bool, error) {
			return repo.executeDelete(ctx, innerTx, id)
		},
	)
	if err != nil {
		wrappedErr := fmt.Errorf("delete field map with tx: %w", err)

		if !errors.Is(err, sql.ErrNoRows) {
			libOpentelemetry.HandleSpanError(span, "failed to delete field map", wrappedErr)

			logger.With(libLog.Any("error", wrappedErr.Error())).Log(ctx, libLog.LevelError, "failed to delete field map")
		}

		return wrappedErr
	}

	return nil
}

// executeDelete performs the actual field map deletion within a transaction.
func (repo *Repository) executeDelete(ctx stdctx.Context, tx *sql.Tx, id uuid.UUID) (bool, error) {
	result, err := tx.ExecContext(ctx, "DELETE FROM field_maps WHERE id = $1", id.String())
	if err != nil {
		return false, err
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return false, err
	}

	if rowsAffected == 0 {
		return false, sql.ErrNoRows
	}

	return true, nil
}

func scanFieldMap(scanner interface{ Scan(dest ...any) error }) (*entities.FieldMap, error) {
	var model FieldMapPostgreSQLModel
	if err := scanner.Scan(
		&model.ID,
		&model.ContextID,
		&model.SourceID,
		&model.Mapping,
		&model.Version,
		&model.CreatedAt,
		&model.UpdatedAt,
	); err != nil {
		return nil, err
	}

	return model.ToEntity()
}
