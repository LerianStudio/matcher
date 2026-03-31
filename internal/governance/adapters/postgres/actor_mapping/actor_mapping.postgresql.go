package actormapping

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/Masterminds/squirrel"

	libCommons "github.com/LerianStudio/lib-commons/v4/commons"
	libLog "github.com/LerianStudio/lib-commons/v4/commons/log"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v4/commons/opentelemetry"

	"github.com/LerianStudio/matcher/internal/governance/domain/entities"
	"github.com/LerianStudio/matcher/internal/governance/domain/repositories"
	pgcommon "github.com/LerianStudio/matcher/internal/shared/adapters/postgres/common"
	"github.com/LerianStudio/matcher/internal/shared/ports"
)

const tableName = "actor_mapping"

// psql is a reusable squirrel placeholder format for PostgreSQL.
var psql = squirrel.StatementBuilder.PlaceholderFormat(squirrel.Dollar)

// Repository persists actor mappings in PostgreSQL.
type Repository struct {
	provider ports.InfrastructureProvider
}

// NewRepository creates a new actor mapping repository.
func NewRepository(provider ports.InfrastructureProvider) *Repository {
	return &Repository{provider: provider}
}

// Upsert creates or updates an actor mapping using INSERT ... ON CONFLICT DO UPDATE ... RETURNING.
// Returns the persisted entity so callers can use it directly without a separate read.
func (repo *Repository) Upsert(ctx context.Context, mapping *entities.ActorMapping) (*entities.ActorMapping, error) {
	return repo.upsertInternal(ctx, nil, mapping)
}

// UpsertWithTx creates or updates an actor mapping within an existing transaction.
func (repo *Repository) UpsertWithTx(ctx context.Context, tx *sql.Tx, mapping *entities.ActorMapping) (*entities.ActorMapping, error) {
	return repo.upsertInternal(ctx, tx, mapping)
}

// upsertInternal contains the core upsert logic, optionally reusing an external transaction.
func (repo *Repository) upsertInternal(ctx context.Context, tx *sql.Tx, mapping *entities.ActorMapping) (*entities.ActorMapping, error) {
	if repo == nil || repo.provider == nil {
		return nil, ErrRepositoryNotInitialized
	}

	if mapping == nil {
		return nil, ErrActorMappingRequired
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	ctx, span := tracer.Start(ctx, "postgres.upsert_actor_mapping")

	defer span.End()

	result, err := pgcommon.WithTenantTxOrExistingProvider(
		ctx,
		repo.provider,
		tx,
		func(innerTx *sql.Tx) (*entities.ActorMapping, error) {
			query, args, err := psql.
				Insert(tableName).
				Columns("actor_id", "display_name", "email", "created_at", "updated_at").
				Values(mapping.ActorID, mapping.DisplayName, mapping.Email, mapping.CreatedAt, mapping.UpdatedAt).
				Suffix(fmt.Sprintf(
					"ON CONFLICT (actor_id) DO UPDATE SET display_name = COALESCE(EXCLUDED.display_name, %s.display_name), email = COALESCE(EXCLUDED.email, %s.email), updated_at = EXCLUDED.updated_at RETURNING actor_id, display_name, email, created_at, updated_at",
					tableName, tableName,
				)).
				ToSql()
			if err != nil {
				return nil, fmt.Errorf("building upsert query: %w", err)
			}

			row := innerTx.QueryRowContext(ctx, query, args...)

			return scanActorMapping(row)
		},
	)
	if err != nil {
		wrappedErr := fmt.Errorf("upsert actor mapping: %w", err)
		libOpentelemetry.HandleSpanError(span, "failed to upsert actor mapping", wrappedErr)

		logger.Log(ctx, libLog.LevelError, fmt.Sprintf("failed to upsert actor mapping: %v", wrappedErr))

		return nil, wrappedErr
	}

	return result, nil
}

// GetByActorID retrieves an actor mapping by its actor ID.
func (repo *Repository) GetByActorID(ctx context.Context, actorID string) (*entities.ActorMapping, error) {
	if repo == nil || repo.provider == nil {
		return nil, ErrRepositoryNotInitialized
	}

	if actorID == "" {
		return nil, ErrActorIDRequired
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	ctx, span := tracer.Start(ctx, "postgres.get_actor_mapping_by_id")

	defer span.End()

	result, err := pgcommon.WithTenantReadQuery(
		ctx,
		repo.provider,
		func(qe pgcommon.QueryExecutor) (*entities.ActorMapping, error) {
			query, args, err := psql.
				Select("actor_id", "display_name", "email", "created_at", "updated_at").
				From(tableName).
				Where(squirrel.Eq{"actor_id": actorID}).
				ToSql()
			if err != nil {
				return nil, fmt.Errorf("building select query: %w", err)
			}

			row := qe.QueryRowContext(ctx, query, args...)

			return scanActorMapping(row)
		},
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrActorMappingNotFound
		}

		wrappedErr := fmt.Errorf("get actor mapping by id: %w", err)
		libOpentelemetry.HandleSpanError(span, "failed to get actor mapping", wrappedErr)

		logger.Log(ctx, libLog.LevelError, fmt.Sprintf("failed to get actor mapping: %v", wrappedErr))

		return nil, wrappedErr
	}

	return result, nil
}

// Pseudonymize replaces PII fields with [REDACTED] for the given actor ID.
func (repo *Repository) Pseudonymize(ctx context.Context, actorID string) error {
	if repo == nil || repo.provider == nil {
		return ErrRepositoryNotInitialized
	}

	if actorID == "" {
		return ErrActorIDRequired
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	ctx, span := tracer.Start(ctx, "postgres.pseudonymize_actor_mapping")

	defer span.End()

	_, err := pgcommon.WithTenantTxProvider(
		ctx,
		repo.provider,
		func(tx *sql.Tx) (struct{}, error) {
			now := time.Now().UTC()

			query, args, err := psql.
				Update(tableName).
				Set("display_name", "[REDACTED]").
				Set("email", "[REDACTED]").
				Set("updated_at", now).
				Where(squirrel.Eq{"actor_id": actorID}).
				ToSql()
			if err != nil {
				return struct{}{}, fmt.Errorf("building pseudonymize query: %w", err)
			}

			result, err := tx.ExecContext(ctx, query, args...)
			if err != nil {
				return struct{}{}, fmt.Errorf("executing pseudonymize: %w", err)
			}

			rowsAffected, err := result.RowsAffected()
			if err != nil {
				return struct{}{}, fmt.Errorf("checking rows affected: %w", err)
			}

			if rowsAffected == 0 {
				return struct{}{}, ErrActorMappingNotFound
			}

			return struct{}{}, nil
		},
	)
	if err != nil {
		wrappedErr := fmt.Errorf("pseudonymize actor mapping: %w", err)
		libOpentelemetry.HandleSpanError(span, "failed to pseudonymize actor mapping", wrappedErr)

		logger.Log(ctx, libLog.LevelError, fmt.Sprintf("failed to pseudonymize actor mapping: %v", wrappedErr))

		return wrappedErr
	}

	return nil
}

// Delete removes an actor mapping entirely (GDPR right-to-erasure).
func (repo *Repository) Delete(ctx context.Context, actorID string) error {
	return repo.deleteInternal(ctx, nil, actorID)
}

// DeleteWithTx removes an actor mapping within an existing transaction (GDPR right-to-erasure).
func (repo *Repository) DeleteWithTx(ctx context.Context, tx *sql.Tx, actorID string) error {
	return repo.deleteInternal(ctx, tx, actorID)
}

// deleteInternal contains the core delete logic, optionally reusing an external transaction.
func (repo *Repository) deleteInternal(ctx context.Context, tx *sql.Tx, actorID string) error {
	if repo == nil || repo.provider == nil {
		return ErrRepositoryNotInitialized
	}

	if actorID == "" {
		return ErrActorIDRequired
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	ctx, span := tracer.Start(ctx, "postgres.delete_actor_mapping")

	defer span.End()

	_, err := pgcommon.WithTenantTxOrExistingProvider(
		ctx,
		repo.provider,
		tx,
		func(innerTx *sql.Tx) (struct{}, error) {
			query, args, err := psql.
				Delete(tableName).
				Where(squirrel.Eq{"actor_id": actorID}).
				ToSql()
			if err != nil {
				return struct{}{}, fmt.Errorf("building delete query: %w", err)
			}

			result, err := innerTx.ExecContext(ctx, query, args...)
			if err != nil {
				return struct{}{}, fmt.Errorf("executing delete: %w", err)
			}

			rowsAffected, err := result.RowsAffected()
			if err != nil {
				return struct{}{}, fmt.Errorf("checking rows affected: %w", err)
			}

			if rowsAffected == 0 {
				return struct{}{}, ErrActorMappingNotFound
			}

			return struct{}{}, nil
		},
	)
	if err != nil {
		wrappedErr := fmt.Errorf("delete actor mapping: %w", err)
		libOpentelemetry.HandleSpanError(span, "failed to delete actor mapping", wrappedErr)

		logger.Log(ctx, libLog.LevelError, fmt.Sprintf("failed to delete actor mapping: %v", wrappedErr))

		return wrappedErr
	}

	return nil
}

// Compile-time check that Repository implements ActorMappingRepository.
var _ repositories.ActorMappingRepository = (*Repository)(nil)
