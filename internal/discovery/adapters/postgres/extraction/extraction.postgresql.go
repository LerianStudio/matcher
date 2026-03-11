package extraction

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/google/uuid"

	libCommons "github.com/LerianStudio/lib-commons/v4/commons"
	libLog "github.com/LerianStudio/lib-commons/v4/commons/log"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v4/commons/opentelemetry"

	"github.com/LerianStudio/matcher/internal/discovery/domain/entities"
	"github.com/LerianStudio/matcher/internal/discovery/domain/repositories"
	pgcommon "github.com/LerianStudio/matcher/internal/shared/adapters/postgres/common"
	"github.com/LerianStudio/matcher/internal/shared/ports"
)

// Compile-time check that Repository implements ExtractionRepository.
var _ repositories.ExtractionRepository = (*Repository)(nil)

const (
	tableName  = "extraction_requests"
	allColumns = "id, connection_id, ingestion_job_id, fetcher_job_id, tables, filters, status, result_path, error_message, created_at, updated_at"
)

// Repository provides PostgreSQL operations for ExtractionRequest entities.
type Repository struct {
	provider ports.InfrastructureProvider
}

// NewRepository creates a new extraction repository.
func NewRepository(provider ports.InfrastructureProvider) *Repository {
	return &Repository{provider: provider}
}

// Create persists a new ExtractionRequest.
func (repo *Repository) Create(ctx context.Context, req *entities.ExtractionRequest) error {
	if repo == nil || repo.provider == nil {
		return ErrRepoNotInitialized
	}

	if req == nil {
		return ErrEntityRequired
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "postgres.create_extraction_request")
	defer span.End()

	_, err := pgcommon.WithTenantTxProvider(ctx, repo.provider, func(tx *sql.Tx) (bool, error) {
		if execErr := repo.executeCreate(ctx, tx, req); execErr != nil {
			return false, execErr
		}

		return true, nil
	})
	if err != nil {
		wrappedErr := fmt.Errorf("create extraction request: %w", err)
		libOpentelemetry.HandleSpanError(span, "failed to create extraction request", wrappedErr)
		logger.With(libLog.Any("error", wrappedErr.Error())).Log(ctx, libLog.LevelError, "failed to create extraction request")

		return wrappedErr
	}

	return nil
}

// CreateWithTx persists a new ExtractionRequest within an existing transaction.
func (repo *Repository) CreateWithTx(ctx context.Context, tx *sql.Tx, req *entities.ExtractionRequest) error {
	if repo == nil || repo.provider == nil {
		return ErrRepoNotInitialized
	}

	if req == nil {
		return ErrEntityRequired
	}

	if tx == nil {
		return ErrTransactionRequired
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "postgres.create_extraction_request_with_tx")
	defer span.End()

	_, err := pgcommon.WithTenantTxOrExistingProvider(ctx, repo.provider, tx, func(innerTx *sql.Tx) (bool, error) {
		if execErr := repo.executeCreate(ctx, innerTx, req); execErr != nil {
			return false, execErr
		}

		return true, nil
	})
	if err != nil {
		wrappedErr := fmt.Errorf("create extraction request with tx: %w", err)
		libOpentelemetry.HandleSpanError(span, "failed to create extraction request", wrappedErr)
		logger.With(libLog.Any("error", wrappedErr.Error())).Log(ctx, libLog.LevelError, "failed to create extraction request")

		return wrappedErr
	}

	return nil
}

// executeCreate performs the actual insertion within a transaction.
func (repo *Repository) executeCreate(ctx context.Context, tx *sql.Tx, req *entities.ExtractionRequest) error {
	model, err := FromDomain(req)
	if err != nil {
		return fmt.Errorf("convert extraction request to model: %w", err)
	}

	_, err = tx.ExecContext(ctx,
		`INSERT INTO `+tableName+` (`+allColumns+`)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)`,
		model.ID,
		model.ConnectionID,
		model.IngestionJobID,
		model.FetcherJobID,
		model.Tables,
		nullableJSON(model.Filters),
		model.Status,
		model.ResultPath,
		model.ErrorMessage,
		model.CreatedAt,
		model.UpdatedAt,
	)
	if err != nil {
		return fmt.Errorf("insert extraction request: %w", err)
	}

	return nil
}

// Update persists changes to an existing ExtractionRequest.
func (repo *Repository) Update(ctx context.Context, req *entities.ExtractionRequest) error {
	if repo == nil || repo.provider == nil {
		return ErrRepoNotInitialized
	}

	if req == nil {
		return ErrEntityRequired
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "postgres.update_extraction_request")
	defer span.End()

	_, err := pgcommon.WithTenantTxProvider(ctx, repo.provider, func(tx *sql.Tx) (bool, error) {
		if execErr := repo.executeUpdate(ctx, tx, req); execErr != nil {
			return false, execErr
		}

		return true, nil
	})
	if err != nil {
		wrappedErr := fmt.Errorf("update extraction request: %w", err)
		libOpentelemetry.HandleSpanError(span, "failed to update extraction request", wrappedErr)
		logger.With(libLog.Any("error", wrappedErr.Error())).Log(ctx, libLog.LevelError, "failed to update extraction request")

		return wrappedErr
	}

	return nil
}

// UpdateWithTx persists changes within an existing transaction.
func (repo *Repository) UpdateWithTx(ctx context.Context, tx *sql.Tx, req *entities.ExtractionRequest) error {
	if repo == nil || repo.provider == nil {
		return ErrRepoNotInitialized
	}

	if req == nil {
		return ErrEntityRequired
	}

	if tx == nil {
		return ErrTransactionRequired
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "postgres.update_extraction_request_with_tx")
	defer span.End()

	_, err := pgcommon.WithTenantTxOrExistingProvider(ctx, repo.provider, tx, func(innerTx *sql.Tx) (bool, error) {
		if execErr := repo.executeUpdate(ctx, innerTx, req); execErr != nil {
			return false, execErr
		}

		return true, nil
	})
	if err != nil {
		wrappedErr := fmt.Errorf("update extraction request with tx: %w", err)
		libOpentelemetry.HandleSpanError(span, "failed to update extraction request", wrappedErr)
		logger.With(libLog.Any("error", wrappedErr.Error())).Log(ctx, libLog.LevelError, "failed to update extraction request")

		return wrappedErr
	}

	return nil
}

// executeUpdate performs the actual update within a transaction.
func (repo *Repository) executeUpdate(ctx context.Context, tx *sql.Tx, req *entities.ExtractionRequest) error {
	model, err := FromDomain(req)
	if err != nil {
		return fmt.Errorf("convert extraction request to model: %w", err)
	}

	result, err := tx.ExecContext(ctx,
		`UPDATE `+tableName+` SET
			connection_id = $1,
			ingestion_job_id = $2,
			fetcher_job_id = $3,
			tables = $4,
			filters = $5,
			status = $6,
			result_path = $7,
			error_message = $8,
			updated_at = $9
		WHERE id = $10`,
		model.ConnectionID,
		model.IngestionJobID,
		model.FetcherJobID,
		model.Tables,
		nullableJSON(model.Filters),
		model.Status,
		model.ResultPath,
		model.ErrorMessage,
		model.UpdatedAt,
		model.ID,
	)
	if err != nil {
		return fmt.Errorf("update extraction request: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("get rows affected: %w", err)
	}

	if rowsAffected == 0 {
		return sql.ErrNoRows
	}

	return nil
}

func nullableJSON(data []byte) any {
	if data == nil {
		return nil
	}

	return data
}

// FindByID retrieves an ExtractionRequest by its internal ID.
func (repo *Repository) FindByID(ctx context.Context, id uuid.UUID) (*entities.ExtractionRequest, error) {
	if repo == nil || repo.provider == nil {
		return nil, ErrRepoNotInitialized
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "postgres.find_extraction_request_by_id")
	defer span.End()

	result, err := pgcommon.WithTenantTxProvider(ctx, repo.provider, func(tx *sql.Tx) (*entities.ExtractionRequest, error) {
		row := tx.QueryRowContext(
			ctx,
			"SELECT "+allColumns+" FROM "+tableName+" WHERE id = $1",
			id.String(),
		)

		return scanExtraction(row)
	})
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, repositories.ErrExtractionNotFound
		}

		wrappedErr := fmt.Errorf("find extraction request by id: %w", err)
		libOpentelemetry.HandleSpanError(span, "failed to find extraction request by id", wrappedErr)
		logger.With(libLog.Any("error", wrappedErr.Error())).Log(ctx, libLog.LevelError, "failed to find extraction request by id")

		return nil, wrappedErr
	}

	return result, nil
}

// scanExtraction scans a SQL row into an ExtractionRequest domain entity.
func scanExtraction(scanner interface{ Scan(dest ...any) error }) (*entities.ExtractionRequest, error) {
	var model ExtractionModel
	if err := scanner.Scan(
		&model.ID,
		&model.ConnectionID,
		&model.IngestionJobID,
		&model.FetcherJobID,
		&model.Tables,
		&model.Filters,
		&model.Status,
		&model.ResultPath,
		&model.ErrorMessage,
		&model.CreatedAt,
		&model.UpdatedAt,
	); err != nil {
		return nil, err
	}

	return model.ToDomain()
}
