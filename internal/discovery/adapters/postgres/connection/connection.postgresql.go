package connection

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/Masterminds/squirrel"
	"github.com/google/uuid"

	libCommons "github.com/LerianStudio/lib-commons/v4/commons"
	libLog "github.com/LerianStudio/lib-commons/v4/commons/log"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v4/commons/opentelemetry"

	"github.com/LerianStudio/matcher/internal/discovery/domain/entities"
	"github.com/LerianStudio/matcher/internal/discovery/domain/repositories"
	pgcommon "github.com/LerianStudio/matcher/internal/shared/adapters/postgres/common"
	"github.com/LerianStudio/matcher/internal/shared/ports"
)

// Compile-time check that Repository implements ConnectionRepository.
var _ repositories.ConnectionRepository = (*Repository)(nil)

var psql = squirrel.StatementBuilder.PlaceholderFormat(squirrel.Dollar)

const (
	tableName      = "fetcher_connections"
	allColumns     = "id, fetcher_conn_id, config_name, database_type, host, port, database_name, product_name, status, last_seen_at, schema_discovered, created_at, updated_at"
	upsertConflict = "fetcher_conn_id"
)

// Repository provides PostgreSQL operations for FetcherConnection entities.
type Repository struct {
	provider ports.InfrastructureProvider
}

// NewRepository creates a new connection repository.
func NewRepository(provider ports.InfrastructureProvider) *Repository {
	return &Repository{provider: provider}
}

// Upsert creates or updates a FetcherConnection based on FetcherConnID.
func (repo *Repository) Upsert(ctx context.Context, conn *entities.FetcherConnection) error {
	if repo == nil || repo.provider == nil {
		return ErrRepoNotInitialized
	}

	if conn == nil {
		return ErrEntityRequired
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "postgres.upsert_fetcher_connection")
	defer span.End()

	_, err := pgcommon.WithTenantTxProvider(ctx, repo.provider, func(tx *sql.Tx) (bool, error) {
		if execErr := repo.executeUpsert(ctx, tx, conn); execErr != nil {
			return false, execErr
		}

		return true, nil
	})
	if err != nil {
		wrappedErr := fmt.Errorf("upsert fetcher connection: %w", err)
		libOpentelemetry.HandleSpanError(span, "failed to upsert fetcher connection", wrappedErr)
		logger.With(libLog.Any("error", wrappedErr.Error())).Log(ctx, libLog.LevelError, "failed to upsert fetcher connection")

		return wrappedErr
	}

	return nil
}

// UpsertWithTx creates or updates a FetcherConnection within an existing transaction.
func (repo *Repository) UpsertWithTx(ctx context.Context, tx *sql.Tx, conn *entities.FetcherConnection) error {
	if repo == nil || repo.provider == nil {
		return ErrRepoNotInitialized
	}

	if conn == nil {
		return ErrEntityRequired
	}

	if tx == nil {
		return ErrTransactionRequired
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "postgres.upsert_fetcher_connection_with_tx")
	defer span.End()

	_, err := pgcommon.WithTenantTxOrExistingProvider(ctx, repo.provider, tx, func(innerTx *sql.Tx) (bool, error) {
		if execErr := repo.executeUpsert(ctx, innerTx, conn); execErr != nil {
			return false, execErr
		}

		return true, nil
	})
	if err != nil {
		wrappedErr := fmt.Errorf("upsert fetcher connection with tx: %w", err)
		libOpentelemetry.HandleSpanError(span, "failed to upsert fetcher connection", wrappedErr)
		logger.With(libLog.Any("error", wrappedErr.Error())).Log(ctx, libLog.LevelError, "failed to upsert fetcher connection")

		return wrappedErr
	}

	return nil
}

// executeUpsert performs the actual upsert within a transaction.
func (repo *Repository) executeUpsert(ctx context.Context, tx *sql.Tx, conn *entities.FetcherConnection) error {
	model := FromDomain(conn)
	if model == nil {
		return ErrEntityRequired
	}

	query := `INSERT INTO ` + tableName + ` (` + allColumns + `)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
		ON CONFLICT (` + upsertConflict + `) DO UPDATE SET
			config_name = EXCLUDED.config_name,
			database_type = EXCLUDED.database_type,
			host = EXCLUDED.host,
			port = EXCLUDED.port,
			database_name = EXCLUDED.database_name,
			product_name = EXCLUDED.product_name,
			status = EXCLUDED.status,
			last_seen_at = EXCLUDED.last_seen_at,
			schema_discovered = EXCLUDED.schema_discovered,
			updated_at = EXCLUDED.updated_at`

	_, err := tx.ExecContext(ctx, query,
		model.ID,
		model.FetcherConnID,
		model.ConfigName,
		model.DatabaseType,
		model.Host,
		model.Port,
		model.DatabaseName,
		model.ProductName,
		model.Status,
		model.LastSeenAt,
		model.SchemaDiscovered,
		model.CreatedAt,
		model.UpdatedAt,
	)
	if err != nil {
		return fmt.Errorf("execute upsert fetcher connection: %w", err)
	}

	return nil
}

// FindAll returns all known FetcherConnections.
func (repo *Repository) FindAll(ctx context.Context) ([]*entities.FetcherConnection, error) {
	if repo == nil || repo.provider == nil {
		return nil, ErrRepoNotInitialized
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "postgres.find_all_fetcher_connections")
	defer span.End()

	result, err := pgcommon.WithTenantTxProvider(ctx, repo.provider, func(tx *sql.Tx) ([]*entities.FetcherConnection, error) {
		query, args, buildErr := psql.Select(allColumns).
			From(tableName).
			OrderBy("created_at ASC").
			ToSql()
		if buildErr != nil {
			return nil, fmt.Errorf("build find all connections query: %w", buildErr)
		}

		rows, queryErr := tx.QueryContext(ctx, query, args...)
		if queryErr != nil {
			return nil, fmt.Errorf("query all fetcher connections: %w", queryErr)
		}

		defer func() {
			_ = rows.Close()
		}()

		var connections []*entities.FetcherConnection

		for rows.Next() {
			entity, scanErr := scanConnection(rows)
			if scanErr != nil {
				return nil, scanErr
			}

			connections = append(connections, entity)
		}

		if err := rows.Err(); err != nil {
			return nil, fmt.Errorf("iterate fetcher connections: %w", err)
		}

		return connections, nil
	})
	if err != nil {
		wrappedErr := fmt.Errorf("find all fetcher connections: %w", err)
		libOpentelemetry.HandleSpanError(span, "failed to find all fetcher connections", wrappedErr)
		logger.With(libLog.Any("error", wrappedErr.Error())).Log(ctx, libLog.LevelError, "failed to find all fetcher connections")

		return nil, wrappedErr
	}

	return result, nil
}

// FindByID retrieves a FetcherConnection by its internal ID.
func (repo *Repository) FindByID(ctx context.Context, id uuid.UUID) (*entities.FetcherConnection, error) {
	if repo == nil || repo.provider == nil {
		return nil, ErrRepoNotInitialized
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "postgres.find_fetcher_connection_by_id")
	defer span.End()

	result, err := pgcommon.WithTenantTxProvider(ctx, repo.provider, func(tx *sql.Tx) (*entities.FetcherConnection, error) {
		row := tx.QueryRowContext(
			ctx,
			"SELECT "+allColumns+" FROM "+tableName+" WHERE id = $1",
			id.String(),
		)

		return scanConnection(row)
	})
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrConnectionNotFound
		}

		wrappedErr := fmt.Errorf("find fetcher connection by id: %w", err)
		libOpentelemetry.HandleSpanError(span, "failed to find fetcher connection by id", wrappedErr)
		logger.With(libLog.Any("error", wrappedErr.Error())).Log(ctx, libLog.LevelError, "failed to find fetcher connection by id")

		return nil, wrappedErr
	}

	return result, nil
}

// FindByFetcherID retrieves a FetcherConnection by its Fetcher-assigned external ID.
func (repo *Repository) FindByFetcherID(ctx context.Context, fetcherConnID string) (*entities.FetcherConnection, error) {
	if repo == nil || repo.provider == nil {
		return nil, ErrRepoNotInitialized
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "postgres.find_fetcher_connection_by_fetcher_id")
	defer span.End()

	result, err := pgcommon.WithTenantTxProvider(ctx, repo.provider, func(tx *sql.Tx) (*entities.FetcherConnection, error) {
		row := tx.QueryRowContext(
			ctx,
			"SELECT "+allColumns+" FROM "+tableName+" WHERE fetcher_conn_id = $1",
			fetcherConnID,
		)

		return scanConnection(row)
	})
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrConnectionNotFound
		}

		wrappedErr := fmt.Errorf("find fetcher connection by fetcher id: %w", err)
		libOpentelemetry.HandleSpanError(span, "failed to find fetcher connection by fetcher id", wrappedErr)
		logger.With(libLog.Any("error", wrappedErr.Error())).Log(ctx, libLog.LevelError, "failed to find fetcher connection by fetcher id")

		return nil, wrappedErr
	}

	return result, nil
}

// scanConnection scans a SQL row into a FetcherConnection domain entity.
func scanConnection(scanner interface{ Scan(dest ...any) error }) (*entities.FetcherConnection, error) {
	var model ConnectionModel
	if err := scanner.Scan(
		&model.ID,
		&model.FetcherConnID,
		&model.ConfigName,
		&model.DatabaseType,
		&model.Host,
		&model.Port,
		&model.DatabaseName,
		&model.ProductName,
		&model.Status,
		&model.LastSeenAt,
		&model.SchemaDiscovered,
		&model.CreatedAt,
		&model.UpdatedAt,
	); err != nil {
		return nil, err
	}

	return model.ToDomain(), nil
}
