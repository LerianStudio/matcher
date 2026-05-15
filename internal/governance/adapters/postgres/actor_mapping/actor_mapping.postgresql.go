// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package actormapping

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/Masterminds/squirrel"

	libCommons "github.com/LerianStudio/lib-commons/v5/commons"
	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v5/commons/opentelemetry"
	"github.com/LerianStudio/lib-commons/v5/commons/runtime"

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

// Upsert creates a new actor mapping or returns the existing one when the payload matches.
//
// Post-fix semantics (Taura Security pentest finding 28/04/2026):
//
//   - Identity fields (display_name, email) are append-only after first creation.
//   - The underlying SQL is INSERT ... ON CONFLICT (actor_id) DO NOTHING RETURNING.
//     On a fresh actor_id RETURNING yields the new row; on conflict it yields zero rows.
//   - When the conflict path is taken, the repository SELECTs the current row within
//     the same transaction and compares display_name and email to the payload:
//     identical → returns the existing entity (idempotent success); different →
//     returns ErrActorMappingImmutable and the transaction is rolled back.
//
// This design closes the pseudonymization-bypass vulnerability where the prior
// COALESCE-based UPDATE allowed plaintext PII to overwrite [REDACTED] values.
func (repo *Repository) Upsert(ctx context.Context, mapping *entities.ActorMapping) (*entities.ActorMapping, error) {
	return repo.upsertInternal(ctx, nil, mapping)
}

// UpsertWithTx applies the same semantics as Upsert within an existing transaction.
func (repo *Repository) UpsertWithTx(ctx context.Context, tx *sql.Tx, mapping *entities.ActorMapping) (*entities.ActorMapping, error) {
	return repo.upsertInternal(ctx, tx, mapping)
}

// upsertInternal contains the core append-only upsert logic, optionally reusing
// an external transaction.
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
			return repo.insertOrCompare(ctx, innerTx, mapping)
		},
	)
	if err != nil {
		// Immutability violations are business-level conflicts (client error),
		// not infrastructure failures — record as a span business event so
		// dashboards don't treat them as 5xx noise. The error is still wrapped
		// with the operation prefix so wrapcheck is satisfied and the call
		// stack stays diagnosable; errors.Is(...) on the sentinel still works.
		if errors.Is(err, ErrActorMappingImmutable) {
			wrappedImmutableErr := fmt.Errorf("upsert actor mapping: %w", err)
			libOpentelemetry.HandleSpanBusinessErrorEvent(span, "actor mapping identity immutable", wrappedImmutableErr)

			return nil, wrappedImmutableErr
		}

		wrappedErr := fmt.Errorf("upsert actor mapping: %w", err)
		libOpentelemetry.HandleSpanError(span, "failed to upsert actor mapping", wrappedErr)

		libLog.SafeError(logger, ctx, "failed to upsert actor mapping", wrappedErr, runtime.IsProductionMode())

		return nil, wrappedErr
	}

	return result, nil
}

// insertOrCompare executes the append-only INSERT and, on conflict, fetches and
// compares the existing row. The whole sequence runs inside the caller-provided
// transaction so a concurrent UPDATE cannot bypass the comparison.
func (repo *Repository) insertOrCompare(ctx context.Context, tx *sql.Tx, mapping *entities.ActorMapping) (*entities.ActorMapping, error) {
	query, args, err := psql.
		Insert(tableName).
		Columns("actor_id", "display_name", "email", "created_at", "updated_at").
		Values(mapping.ActorID, mapping.DisplayName, mapping.Email, mapping.CreatedAt, mapping.UpdatedAt).
		Suffix("ON CONFLICT (actor_id) DO NOTHING RETURNING actor_id, display_name, email, created_at, updated_at").
		ToSql()
	if err != nil {
		return nil, fmt.Errorf("building upsert query: %w", err)
	}

	row := tx.QueryRowContext(ctx, query, args...)

	inserted, scanErr := scanActorMapping(row)
	if scanErr == nil {
		return inserted, nil
	}

	if !errors.Is(scanErr, sql.ErrNoRows) {
		return nil, scanErr
	}

	// Conflict path: fetch the existing row inside the same transaction and
	// compare identity fields against the payload.
	existing, err := repo.selectForCompareWithTx(ctx, tx, mapping.ActorID)
	if err != nil {
		return nil, err
	}

	if actorMappingPIIDiffers(existing, mapping) {
		return nil, ErrActorMappingImmutable
	}

	return existing, nil
}

// selectForCompareWithTx reads the current row for an actor_id within an
// existing transaction. The transaction-scoped read prevents a concurrent
// UPDATE from racing between the INSERT...DO NOTHING and this SELECT.
func (repo *Repository) selectForCompareWithTx(ctx context.Context, tx *sql.Tx, actorID string) (*entities.ActorMapping, error) {
	query, args, err := psql.
		Select("actor_id", "display_name", "email", "created_at", "updated_at").
		From(tableName).
		Where(squirrel.Eq{"actor_id": actorID}).
		ToSql()
	if err != nil {
		return nil, fmt.Errorf("building select for compare query: %w", err)
	}

	row := tx.QueryRowContext(ctx, query, args...)

	return scanActorMapping(row)
}

// actorMappingPIIDiffers reports whether the payload's display_name or email
// differs from the persisted row. Both fields use pointer-to-string with
// nil-or-empty treated as semantically equivalent to match the existing
// constructor's NULL-handling.
func actorMappingPIIDiffers(existing, payload *entities.ActorMapping) bool {
	if existing == nil || payload == nil {
		return true
	}

	return !stringPtrEqual(existing.DisplayName, payload.DisplayName) ||
		!stringPtrEqual(existing.Email, payload.Email)
}

// stringPtrEqual treats nil and empty-string pointers as equivalent so a
// caller submitting "" for a field that is stored as NULL (or vice versa) is
// not flagged as a mutation attempt.
func stringPtrEqual(lhs, rhs *string) bool {
	lhsEmpty := lhs == nil || *lhs == ""
	rhsEmpty := rhs == nil || *rhs == ""

	if lhsEmpty && rhsEmpty {
		return true
	}

	if lhsEmpty != rhsEmpty {
		return false
	}

	return *lhs == *rhs
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

		libLog.SafeError(logger, ctx, "failed to get actor mapping", wrappedErr, runtime.IsProductionMode())

		return nil, wrappedErr
	}

	return result, nil
}

// PseudonymizeWithTx replaces PII fields with [REDACTED] for the given actor ID
// using the caller-owned transaction. This is the only entry point — production
// paths atomically couple the mutation with a streaming emit at the service layer
// (see governance/services/command/actor_mapping_commands.go).
func (repo *Repository) PseudonymizeWithTx(ctx context.Context, tx *sql.Tx, actorID string) error {
	return repo.pseudonymizeInternal(ctx, tx, actorID)
}

func (repo *Repository) pseudonymizeInternal(ctx context.Context, tx *sql.Tx, actorID string) error {
	if repo == nil || repo.provider == nil {
		return ErrRepositoryNotInitialized
	}

	if actorID == "" {
		return ErrActorIDRequired
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	ctx, span := tracer.Start(ctx, "postgres.pseudonymize_actor_mapping")

	defer span.End()

	_, err := pgcommon.WithTenantTxOrExistingProvider(
		ctx,
		repo.provider,
		tx,
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

		libLog.SafeError(logger, ctx, "failed to pseudonymize actor mapping", wrappedErr, runtime.IsProductionMode())

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

		libLog.SafeError(logger, ctx, "failed to delete actor mapping", wrappedErr, runtime.IsProductionMode())

		return wrappedErr
	}

	return nil
}

// Compile-time check that Repository implements ActorMappingRepository.
var _ repositories.ActorMappingRepository = (*Repository)(nil)
