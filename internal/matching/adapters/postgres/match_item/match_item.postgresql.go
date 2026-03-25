// Package match_item provides PostgreSQL adapter implementation for match item persistence.
package match_item

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/Masterminds/squirrel"
	"github.com/google/uuid"

	libCommons "github.com/LerianStudio/lib-commons/v4/commons"
	libLog "github.com/LerianStudio/lib-commons/v4/commons/log"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v4/commons/opentelemetry"

	matchingEntities "github.com/LerianStudio/matcher/internal/matching/domain/entities"
	matchingRepos "github.com/LerianStudio/matcher/internal/matching/domain/repositories"
	pgcommon "github.com/LerianStudio/matcher/internal/shared/adapters/postgres/common"
	"github.com/LerianStudio/matcher/internal/shared/ports"
)

const (
	columns = "id, match_group_id, transaction_id, allocated_amount, allocated_currency, expected_amount, allow_partial, created_at, updated_at"

	// defaultBatchCapacity is the default pre-allocation capacity for batch queries.
	defaultBatchCapacity = 32
)

// Repository implements match item persistence using PostgreSQL.
type Repository struct {
	provider ports.InfrastructureProvider
}

// NewRepository creates a new match item repository with the provided connection.
func NewRepository(provider ports.InfrastructureProvider) *Repository {
	return &Repository{provider: provider}
}

// CreateBatch persists multiple match items in a single transaction.
func (repo *Repository) CreateBatch(
	ctx context.Context,
	items []*matchingEntities.MatchItem,
) ([]*matchingEntities.MatchItem, error) {
	return repo.createBatch(ctx, nil, items)
}

// CreateBatchWithTx persists multiple match items using an existing transaction.
// The tx must be non-nil.
func (repo *Repository) CreateBatchWithTx(
	ctx context.Context,
	tx matchingRepos.Tx,
	items []*matchingEntities.MatchItem,
) ([]*matchingEntities.MatchItem, error) {
	if repo == nil || repo.provider == nil {
		return nil, ErrRepoNotInitialized
	}

	if tx == nil {
		return nil, ErrInvalidTx
	}

	return repo.createBatch(ctx, tx, items)
}

func (repo *Repository) createBatch(
	ctx context.Context,
	tx *sql.Tx,
	items []*matchingEntities.MatchItem,
) ([]*matchingEntities.MatchItem, error) {
	if repo == nil || repo.provider == nil {
		return nil, ErrRepoNotInitialized
	}

	if len(items) == 0 {
		return nil, nil
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	ctx, span := tracer.Start(ctx, "postgres.create_match_item_batch")

	defer span.End()

	result, err := pgcommon.WithTenantTxOrExistingProvider(
		ctx,
		repo.provider,
		tx,
		func(execTx *sql.Tx) ([]*matchingEntities.MatchItem, error) {
			stmt, err := execTx.PrepareContext(
				ctx,
				`INSERT INTO match_items (id, match_group_id, transaction_id, allocated_amount, allocated_currency, expected_amount, allow_partial, created_at, updated_at)
			 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)`,
			)
			if err != nil {
				return nil, fmt.Errorf("prepare insert match item: %w", err)
			}

			defer func() { _ = stmt.Close() }()

			for _, item := range items {
				model, err := NewPostgreSQLModel(item)
				if err != nil {
					return nil, err
				}

				if _, err := stmt.ExecContext(ctx,
					model.ID,
					model.MatchGroupID,
					model.TransactionID,
					model.AllocatedAmount,
					model.AllocatedCurrency,
					model.ExpectedAmount,
					model.AllowPartial,
					model.CreatedAt,
					model.UpdatedAt,
				); err != nil {
					return nil, fmt.Errorf("insert match item: %w", err)
				}
			}

			return items, nil
		},
	)
	if err != nil {
		wrappedErr := fmt.Errorf("create match item batch transaction: %w", err)
		libOpentelemetry.HandleSpanError(span, "failed to create match item batch", wrappedErr)

		logger.With(libLog.Any("error", wrappedErr.Error())).Log(ctx, libLog.LevelError, "failed to create match item batch")

		return nil, wrappedErr
	}

	return result, nil
}

// ListByMatchGroupID returns all match items for a given match group.
func (repo *Repository) ListByMatchGroupID(
	ctx context.Context,
	matchGroupID uuid.UUID,
) ([]*matchingEntities.MatchItem, error) {
	if repo == nil || repo.provider == nil {
		return nil, ErrRepoNotInitialized
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	ctx, span := tracer.Start(ctx, "postgres.list_match_items_by_group")

	defer span.End()

	result, err := pgcommon.WithTenantReadQuery(
		ctx,
		repo.provider,
		func(qe pgcommon.QueryExecutor) (items []*matchingEntities.MatchItem, err error) {
			rows, err := qe.QueryContext(
				ctx,
				"SELECT "+columns+" FROM match_items WHERE match_group_id=$1 ORDER BY created_at ASC",
				matchGroupID.String(),
			)
			if err != nil {
				return nil, fmt.Errorf("query match items: %w", err)
			}

			defer func() {
				if closeErr := rows.Close(); closeErr != nil && err == nil {
					err = closeErr
				}
			}()

			items = make([]*matchingEntities.MatchItem, 0, defaultBatchCapacity)

			for rows.Next() {
				entity, scanErr := scan(rows)
				if scanErr != nil {
					return nil, scanErr
				}

				items = append(items, entity)
			}

			if err := rows.Err(); err != nil {
				return nil, fmt.Errorf("iterate match item rows: %w", err)
			}

			return items, nil
		},
	)
	if err != nil {
		wrappedErr := fmt.Errorf("list match items by group: %w", err)
		libOpentelemetry.HandleSpanError(span, "failed to list match items by group", wrappedErr)

		logger.With(libLog.Any("error", wrappedErr.Error())).Log(ctx, libLog.LevelError, "failed to list match items by group")

		return nil, wrappedErr
	}

	return result, nil
}

// ListByMatchGroupIDs returns all match items for the given group IDs in a single query.
// Results are grouped by match_group_id for efficient batch association.
func (repo *Repository) ListByMatchGroupIDs(
	ctx context.Context,
	matchGroupIDs []uuid.UUID,
) (map[uuid.UUID][]*matchingEntities.MatchItem, error) {
	if repo == nil || repo.provider == nil {
		return nil, ErrRepoNotInitialized
	}

	if len(matchGroupIDs) == 0 {
		return make(map[uuid.UUID][]*matchingEntities.MatchItem), nil
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	ctx, span := tracer.Start(ctx, "postgres.list_match_items_by_group_ids")

	defer span.End()

	result, err := pgcommon.WithTenantReadQuery(
		ctx,
		repo.provider,
		func(qe pgcommon.QueryExecutor) (map[uuid.UUID][]*matchingEntities.MatchItem, error) {
			return repo.queryAndGroupItems(ctx, qe, matchGroupIDs)
		},
	)
	if err != nil {
		wrappedErr := fmt.Errorf("list match items by group ids: %w", err)
		libOpentelemetry.HandleSpanError(span, "failed to list match items by group ids", wrappedErr)

		logger.With(libLog.Any("error", wrappedErr.Error())).Log(ctx, libLog.LevelError, "failed to list match items by group ids")

		return nil, wrappedErr
	}

	return result, nil
}

// queryAndGroupItems builds and executes the batch query for match items,
// returning them grouped by match_group_id.
func (repo *Repository) queryAndGroupItems(
	ctx context.Context,
	qe pgcommon.QueryExecutor,
	matchGroupIDs []uuid.UUID,
) (map[uuid.UUID][]*matchingEntities.MatchItem, error) {
	idStrings := make([]string, len(matchGroupIDs))
	for i, id := range matchGroupIDs {
		idStrings[i] = id.String()
	}

	query, args, err := squirrel.Select(strings.Split(columns, ", ")...).
		From("match_items").
		Where(squirrel.Eq{"match_group_id": idStrings}).
		OrderBy("match_group_id", "created_at ASC").
		PlaceholderFormat(squirrel.Dollar).
		ToSql()
	if err != nil {
		return nil, fmt.Errorf("build match items query: %w", err)
	}

	rows, err := qe.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query match items by group ids: %w", err)
	}

	defer func() {
		if closeErr := rows.Close(); closeErr != nil && err == nil {
			err = closeErr
		}
	}()

	grouped := make(map[uuid.UUID][]*matchingEntities.MatchItem, len(matchGroupIDs))

	for rows.Next() {
		entity, scanErr := scan(rows)
		if scanErr != nil {
			return nil, scanErr
		}

		grouped[entity.MatchGroupID] = append(grouped[entity.MatchGroupID], entity)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate match item rows: %w", err)
	}

	return grouped, nil
}

func scan(scanner interface{ Scan(dest ...any) error }) (*matchingEntities.MatchItem, error) {
	var model PostgreSQLModel
	if err := scanner.Scan(
		&model.ID,
		&model.MatchGroupID,
		&model.TransactionID,
		&model.AllocatedAmount,
		&model.AllocatedCurrency,
		&model.ExpectedAmount,
		&model.AllowPartial,
		&model.CreatedAt,
		&model.UpdatedAt,
	); err != nil {
		return nil, fmt.Errorf("scan match item: %w", err)
	}

	return model.ToEntity()
}

var _ matchingRepos.MatchItemRepository = (*Repository)(nil)
