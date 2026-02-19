// Package rate provides PostgreSQL persistence for fee rate entities.
package rate

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/google/uuid"

	libCommons "github.com/LerianStudio/lib-uncommons/v2/uncommons"
	libLog "github.com/LerianStudio/lib-uncommons/v2/uncommons/log"
	libOpentelemetry "github.com/LerianStudio/lib-uncommons/v2/uncommons/opentelemetry"

	matchingRepos "github.com/LerianStudio/matcher/internal/matching/domain/repositories"
	pgcommon "github.com/LerianStudio/matcher/internal/shared/adapters/postgres/common"
	"github.com/LerianStudio/matcher/internal/shared/domain/fee"
	"github.com/LerianStudio/matcher/internal/shared/ports"
)

const columns = "id, currency, structure_type, structure, created_at, updated_at"

// Repository persists fee rates in Postgres.
type Repository struct {
	provider ports.InfrastructureProvider
}

// NewRepository creates a new rate repository.
func NewRepository(provider ports.InfrastructureProvider) *Repository {
	return &Repository{provider: provider}
}

// GetByID retrieves a rate by its ID.
func (repo *Repository) GetByID(ctx context.Context, id uuid.UUID) (*fee.Rate, error) {
	if repo == nil || repo.provider == nil {
		return nil, ErrRepoNotInitialized
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	ctx, span := tracer.Start(ctx, "postgres.get_rate_by_id")

	defer span.End()

	result, err := pgcommon.WithTenantTxProvider(
		ctx,
		repo.provider,
		func(tx *sql.Tx) (*fee.Rate, error) {
			row := tx.QueryRowContext(ctx,
				"SELECT "+columns+" FROM rates WHERE id = $1",
				id.String(),
			)

			return scan(row)
		},
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrRateNotFound
		}

		wrappedErr := fmt.Errorf("get rate by id: %w", err)
		libOpentelemetry.HandleSpanError(span, "failed to get rate by id", wrappedErr)

		logger.With(libLog.Any("error", wrappedErr.Error())).Log(ctx, libLog.LevelError, "failed to get rate by id")

		return nil, wrappedErr
	}

	return result, nil
}

func scan(scanner interface{ Scan(dest ...any) error }) (*fee.Rate, error) {
	var model PostgreSQLModel
	if err := scanner.Scan(
		&model.ID,
		&model.Currency,
		&model.StructureType,
		&model.StructureData,
		&model.CreatedAt,
		&model.UpdatedAt,
	); err != nil {
		return nil, err
	}

	return model.ToEntity()
}

var _ matchingRepos.RateRepository = (*Repository)(nil)
