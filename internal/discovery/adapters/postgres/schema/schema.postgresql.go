package schema

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/google/uuid"

	libCommons "github.com/LerianStudio/lib-commons/v5/commons"
	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v5/commons/opentelemetry"

	"github.com/LerianStudio/matcher/internal/discovery/domain/entities"
	"github.com/LerianStudio/matcher/internal/discovery/domain/repositories"
	pgcommon "github.com/LerianStudio/matcher/internal/shared/adapters/postgres/common"
	"github.com/LerianStudio/matcher/internal/shared/ports"
)

// Compile-time check that Repository implements SchemaRepository.
var _ repositories.SchemaRepository = (*Repository)(nil)

const (
	tableName  = "discovered_schemas"
	allColumns = "id, connection_id, table_name, columns, discovered_at"
)

// Repository provides PostgreSQL operations for DiscoveredSchema entities.
type Repository struct {
	provider ports.InfrastructureProvider
}

// NewRepository creates a new schema repository.
func NewRepository(provider ports.InfrastructureProvider) *Repository {
	return &Repository{provider: provider}
}

// UpsertBatch replaces the schema snapshot for every connection represented in
// the batch within a single transaction.
func (repo *Repository) UpsertBatch(ctx context.Context, schemas []*entities.DiscoveredSchema) error {
	if repo == nil || repo.provider == nil {
		return ErrRepoNotInitialized
	}

	if len(schemas) == 0 {
		return nil
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "postgres.upsert_batch_discovered_schemas")
	defer span.End()

	_, err := pgcommon.WithTenantTxProvider(ctx, repo.provider, func(tx *sql.Tx) (bool, error) {
		if execErr := repo.executeUpsertBatch(ctx, tx, schemas); execErr != nil {
			return false, execErr
		}

		return true, nil
	})
	if err != nil {
		wrappedErr := fmt.Errorf("upsert batch discovered schemas: %w", err)
		libOpentelemetry.HandleSpanError(span, "failed to upsert batch discovered schemas", wrappedErr)
		logger.With(libLog.Any("error", wrappedErr.Error())).Log(ctx, libLog.LevelError, "failed to upsert batch discovered schemas")

		return wrappedErr
	}

	return nil
}

// UpsertBatchWithTx replaces the schema snapshot for every connection
// represented in the batch within an existing transaction.
func (repo *Repository) UpsertBatchWithTx(ctx context.Context, tx *sql.Tx, schemas []*entities.DiscoveredSchema) error {
	if repo == nil || repo.provider == nil {
		return ErrRepoNotInitialized
	}

	if len(schemas) == 0 {
		return nil
	}

	if tx == nil {
		return ErrTransactionRequired
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "postgres.upsert_batch_discovered_schemas_with_tx")
	defer span.End()

	_, err := pgcommon.WithTenantTxOrExistingProvider(ctx, repo.provider, tx, func(innerTx *sql.Tx) (bool, error) {
		if execErr := repo.executeUpsertBatch(ctx, innerTx, schemas); execErr != nil {
			return false, execErr
		}

		return true, nil
	})
	if err != nil {
		wrappedErr := fmt.Errorf("upsert batch discovered schemas with tx: %w", err)
		libOpentelemetry.HandleSpanError(span, "failed to upsert batch discovered schemas", wrappedErr)
		logger.With(libLog.Any("error", wrappedErr.Error())).Log(ctx, libLog.LevelError, "failed to upsert batch discovered schemas")

		return wrappedErr
	}

	return nil
}

// executeUpsertBatch performs the actual snapshot replacement within a
// transaction by deleting prior rows for the affected connections and then
// inserting the latest schema tables.
func (repo *Repository) executeUpsertBatch(ctx context.Context, tx *sql.Tx, schemas []*entities.DiscoveredSchema) error {
	for _, connectionID := range uniqueConnectionIDs(schemas) {
		if _, err := tx.ExecContext(
			ctx,
			"DELETE FROM "+tableName+" WHERE connection_id = $1",
			connectionID.String(),
		); err != nil {
			return fmt.Errorf("delete schemas for connection %s: %w", connectionID.String(), err)
		}
	}

	query := `INSERT INTO ` + tableName + ` (` + allColumns + `)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (connection_id, table_name) DO UPDATE SET
			columns = EXCLUDED.columns,
			discovered_at = EXCLUDED.discovered_at`

	for _, sch := range schemas {
		model, err := FromDomain(sch)
		if err != nil {
			return fmt.Errorf("convert schema to model: %w", err)
		}

		_, err = tx.ExecContext(ctx, query,
			model.ID,
			model.ConnectionID,
			model.TableName,
			model.Columns,
			model.DiscoveredAt,
		)
		if err != nil {
			return fmt.Errorf("execute upsert discovered schema for table %s: %w", sch.TableName, err)
		}
	}

	return nil
}

func uniqueConnectionIDs(schemas []*entities.DiscoveredSchema) []uuid.UUID {
	seen := make(map[uuid.UUID]struct{}, len(schemas))
	ids := make([]uuid.UUID, 0, len(schemas))

	for _, schema := range schemas {
		if schema == nil {
			continue
		}

		if _, ok := seen[schema.ConnectionID]; ok {
			continue
		}

		seen[schema.ConnectionID] = struct{}{}
		ids = append(ids, schema.ConnectionID)
	}

	return ids
}

// FindByConnectionID retrieves all schemas discovered for a given connection.
func (repo *Repository) FindByConnectionID(ctx context.Context, connectionID uuid.UUID) ([]*entities.DiscoveredSchema, error) {
	if repo == nil || repo.provider == nil {
		return nil, ErrRepoNotInitialized
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "postgres.find_schemas_by_connection_id")
	defer span.End()

	result, err := pgcommon.WithTenantTxProvider(ctx, repo.provider, func(tx *sql.Tx) ([]*entities.DiscoveredSchema, error) {
		rows, queryErr := tx.QueryContext(
			ctx,
			"SELECT "+allColumns+" FROM "+tableName+" WHERE connection_id = $1 ORDER BY table_name ASC",
			connectionID.String(),
		)
		if queryErr != nil {
			return nil, fmt.Errorf("query schemas by connection id: %w", queryErr)
		}

		defer func() {
			_ = rows.Close()
		}()

		var schemas []*entities.DiscoveredSchema

		for rows.Next() {
			entity, scanErr := scanSchema(rows)
			if scanErr != nil {
				return nil, scanErr
			}

			schemas = append(schemas, entity)
		}

		if err := rows.Err(); err != nil {
			return nil, fmt.Errorf("iterate discovered schemas: %w", err)
		}

		return schemas, nil
	})
	if err != nil {
		wrappedErr := fmt.Errorf("find schemas by connection id: %w", err)
		libOpentelemetry.HandleSpanError(span, "failed to find schemas by connection id", wrappedErr)
		logger.With(libLog.Any("error", wrappedErr.Error())).Log(ctx, libLog.LevelError, "failed to find schemas by connection id")

		return nil, wrappedErr
	}

	return result, nil
}

// DeleteByConnectionID removes all schemas associated with a connection.
func (repo *Repository) DeleteByConnectionID(ctx context.Context, connectionID uuid.UUID) error {
	if repo == nil || repo.provider == nil {
		return ErrRepoNotInitialized
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "postgres.delete_schemas_by_connection_id")
	defer span.End()

	_, err := pgcommon.WithTenantTxProvider(ctx, repo.provider, func(tx *sql.Tx) (bool, error) {
		return repo.executeDeleteByConnectionID(ctx, tx, connectionID)
	})
	if err != nil {
		wrappedErr := fmt.Errorf("delete schemas by connection id: %w", err)
		libOpentelemetry.HandleSpanError(span, "failed to delete schemas by connection id", wrappedErr)
		logger.With(libLog.Any("error", wrappedErr.Error())).Log(ctx, libLog.LevelError, "failed to delete schemas by connection id")

		return wrappedErr
	}

	return nil
}

// DeleteByConnectionIDWithTx removes schemas for a connection within an existing transaction.
func (repo *Repository) DeleteByConnectionIDWithTx(ctx context.Context, tx *sql.Tx, connectionID uuid.UUID) error {
	if repo == nil || repo.provider == nil {
		return ErrRepoNotInitialized
	}

	if tx == nil {
		return ErrTransactionRequired
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "postgres.delete_schemas_by_connection_id_with_tx")
	defer span.End()

	_, err := pgcommon.WithTenantTxOrExistingProvider(ctx, repo.provider, tx, func(innerTx *sql.Tx) (bool, error) {
		return repo.executeDeleteByConnectionID(ctx, innerTx, connectionID)
	})
	if err != nil {
		wrappedErr := fmt.Errorf("delete schemas by connection id with tx: %w", err)
		libOpentelemetry.HandleSpanError(span, "failed to delete schemas by connection id", wrappedErr)
		logger.With(libLog.Any("error", wrappedErr.Error())).Log(ctx, libLog.LevelError, "failed to delete schemas by connection id")

		return wrappedErr
	}

	return nil
}

// executeDeleteByConnectionID performs the actual schema deletion within a transaction.
func (repo *Repository) executeDeleteByConnectionID(ctx context.Context, tx *sql.Tx, connectionID uuid.UUID) (bool, error) {
	_, err := tx.ExecContext(
		ctx,
		"DELETE FROM "+tableName+" WHERE connection_id = $1",
		connectionID.String(),
	)
	if err != nil {
		return false, fmt.Errorf("execute delete schemas by connection id: %w", err)
	}

	return true, nil
}

// scanSchema scans a SQL row into a DiscoveredSchema domain entity.
func scanSchema(scanner interface{ Scan(dest ...any) error }) (*entities.DiscoveredSchema, error) {
	var model SchemaModel
	if err := scanner.Scan(
		&model.ID,
		&model.ConnectionID,
		&model.TableName,
		&model.Columns,
		&model.DiscoveredAt,
	); err != nil {
		return nil, err
	}

	return model.ToDomain()
}
