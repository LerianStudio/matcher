package match_rule

import (
	stdctx "context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/Masterminds/squirrel"
	"github.com/google/uuid"

	libCommons "github.com/LerianStudio/lib-commons/v4/commons"
	libLog "github.com/LerianStudio/lib-commons/v4/commons/log"
	libHTTP "github.com/LerianStudio/lib-commons/v4/commons/net/http"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v4/commons/opentelemetry"

	"github.com/LerianStudio/matcher/internal/configuration/adapters/postgres/common"
	"github.com/LerianStudio/matcher/internal/configuration/domain/entities"
	"github.com/LerianStudio/matcher/internal/configuration/domain/value_objects"
	"github.com/LerianStudio/matcher/internal/shared/constants"
	"github.com/LerianStudio/matcher/internal/shared/ports"
)

const (
	matchRuleColumns      = "id, context_id, priority, type, config, created_at, updated_at"
	reorderPriorityOffset = 1000
	argsPerRuleID         = 3
)

// Repository provides PostgreSQL operations for match rules.
type Repository struct {
	provider ports.InfrastructureProvider
}

// NewRepository creates a new match rule repository.
func NewRepository(provider ports.InfrastructureProvider) *Repository {
	return &Repository{provider: provider}
}

// Create inserts a new match rule into the database.
func (repo *Repository) Create(
	ctx stdctx.Context,
	entity *entities.MatchRule,
) (*entities.MatchRule, error) {
	if repo == nil || repo.provider == nil {
		return nil, ErrRepoNotInitialized
	}

	if entity == nil {
		return nil, ErrMatchRuleEntityRequired
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "postgres.create_match_rule")
	defer span.End()

	result, err := common.WithTenantTxProvider(
		ctx,
		repo.provider,
		func(tx *sql.Tx) (*entities.MatchRule, error) {
			return repo.executeCreate(ctx, tx, entity)
		},
	)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to create match rule", err)

		logger.With(
			libLog.Any("context.id", entity.ContextID.String()),
			libLog.Any("priority", entity.Priority),
			libLog.Any("error.message", err.Error()),
		).Log(ctx, libLog.LevelError, "failed to create match rule")

		return nil, fmt.Errorf("failed to create match rule: %w", err)
	}

	return result, nil
}

// CreateWithTx inserts a new match rule using the provided transaction.
func (repo *Repository) CreateWithTx(
	ctx stdctx.Context,
	tx *sql.Tx,
	entity *entities.MatchRule,
) (*entities.MatchRule, error) {
	if repo == nil || repo.provider == nil {
		return nil, ErrRepoNotInitialized
	}

	if entity == nil {
		return nil, ErrMatchRuleEntityRequired
	}

	if tx == nil {
		return nil, ErrTransactionRequired
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "postgres.create_match_rule_with_tx")
	defer span.End()

	result, err := common.WithTenantTxOrExistingProvider(
		ctx,
		repo.provider,
		tx,
		func(innerTx *sql.Tx) (*entities.MatchRule, error) {
			return repo.executeCreate(ctx, innerTx, entity)
		},
	)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to create match rule", err)

		logger.With(
			libLog.Any("context.id", entity.ContextID.String()),
			libLog.Any("priority", entity.Priority),
			libLog.Any("error.message", err.Error()),
		).Log(ctx, libLog.LevelError, "failed to create match rule")

		return nil, fmt.Errorf("failed to create match rule: %w", err)
	}

	return result, nil
}

// executeCreate performs the actual match rule creation within a transaction.
func (repo *Repository) executeCreate(
	ctx stdctx.Context,
	tx *sql.Tx,
	entity *entities.MatchRule,
) (*entities.MatchRule, error) {
	model, err := NewMatchRulePostgreSQLModel(entity)
	if err != nil {
		return nil, err
	}

	_, err = tx.ExecContext(
		ctx,
		`INSERT INTO match_rules (id, context_id, priority, type, config, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		model.ID,
		model.ContextID,
		model.Priority,
		model.Type,
		model.Config,
		model.CreatedAt,
		model.UpdatedAt,
	)
	if err != nil {
		return nil, err
	}

	return model.ToEntity()
}

// FindByID retrieves a match rule by context and rule ID.
func (repo *Repository) FindByID(
	ctx stdctx.Context,
	contextID, id uuid.UUID,
) (*entities.MatchRule, error) {
	if repo == nil || repo.provider == nil {
		return nil, ErrRepoNotInitialized
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "postgres.find_match_rule_by_id")
	defer span.End()

	connection, err := repo.provider.GetPostgresConnection(ctx)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to get postgres connection", err)
		return nil, fmt.Errorf("get postgres connection: %w", err)
	}

	result, err := common.WithTenantTx(
		ctx,
		connection,
		func(tx *sql.Tx) (*entities.MatchRule, error) {
			row := tx.QueryRowContext(
				ctx,
				"SELECT "+matchRuleColumns+" FROM match_rules WHERE context_id = $1 AND id = $2",
				contextID.String(),
				id.String(),
			)

			return scanMatchRule(row)
		},
	)
	if err != nil {
		if !errors.Is(err, sql.ErrNoRows) {
			libOpentelemetry.HandleSpanError(span, "failed to find match rule by id", err)

			logger.With(
				libLog.Any("context.id", contextID.String()),
				libLog.Any("match_rule.id", id.String()),
				libLog.Any("error.message", err.Error()),
			).Log(ctx, libLog.LevelError, "failed to find match rule by id")
		}

		return nil, fmt.Errorf("failed to find match rule by id: %w", err)
	}

	return result, nil
}

// FindByContextID retrieves all match rules for a context using cursor-based pagination.
func (repo *Repository) FindByContextID(
	ctx stdctx.Context,
	contextID uuid.UUID,
	cursor string,
	limit int,
) (entities.MatchRules, libHTTP.CursorPagination, error) {
	if repo == nil || repo.provider == nil {
		return nil, libHTTP.CursorPagination{}, ErrRepoNotInitialized
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "postgres.find_match_rules_by_context")
	defer span.End()

	connection, err := repo.provider.GetPostgresConnection(ctx)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to get postgres connection", err)
		return nil, libHTTP.CursorPagination{}, fmt.Errorf("get postgres connection: %w", err)
	}

	limit = libHTTP.ValidateLimit(limit, constants.DefaultPaginationLimit, constants.MaximumPaginationLimit)

	decodedCursor, cursorID, err := parseCursor(cursor)
	if err != nil {
		return nil, libHTTP.CursorPagination{}, err
	}

	var pagination libHTTP.CursorPagination

	result, err := common.WithTenantTx(
		ctx,
		connection,
		func(tx *sql.Tx) (rules entities.MatchRules, err error) {
			builder := squirrel.Select(strings.Split(matchRuleColumns, ", ")...).
				From("match_rules").
				Where(squirrel.Eq{"context_id": contextID.String()}).
				PlaceholderFormat(squirrel.Dollar)

			orderDirection := libHTTP.ValidateSortDirection("ASC")

			if cursor != "" {
				cond, cursorOrderDirection, buildErr := buildCursorConditions(ctx, tx, decodedCursor, cursorID, contextID)
				if buildErr != nil {
					return nil, buildErr
				}

				builder = builder.Where(cond)
				orderDirection = cursorOrderDirection
			}

			builder = builder.OrderBy("priority "+orderDirection, "id "+orderDirection).
				Limit(safeUint64(limit + 1))

			query, args, err := builder.ToSql()
			if err != nil {
				return nil, fmt.Errorf("build list match rules query: %w", err)
			}

			rules, err = executeMatchRulesQuery(ctx, tx, query, args)
			if err != nil {
				return nil, err
			}

			rules, pagination, err = paginateAndCalculateCursor(
				cursor,
				decodedCursor,
				rules,
				limit,
			)
			if err != nil {
				return nil, err
			}

			return rules, nil
		},
	)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to list match rules", err)

		logger.With(
			libLog.Any("context.id", contextID.String()),
			libLog.Any("error.message", err.Error()),
		).Log(ctx, libLog.LevelError, "failed to list match rules")

		return nil, libHTTP.CursorPagination{}, fmt.Errorf("failed to find match rules by context: %w", err)
	}

	return result, pagination, nil
}

// FindByContextIDAndType retrieves match rules for a context filtered by type using cursor-based pagination.
func (repo *Repository) FindByContextIDAndType(
	ctx stdctx.Context,
	contextID uuid.UUID,
	ruleType value_objects.RuleType,
	cursor string,
	limit int,
) (entities.MatchRules, libHTTP.CursorPagination, error) {
	if repo == nil || repo.provider == nil {
		return nil, libHTTP.CursorPagination{}, ErrRepoNotInitialized
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "postgres.find_match_rules_by_context_and_type")
	defer span.End()

	connection, err := repo.provider.GetPostgresConnection(ctx)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to get postgres connection", err)
		return nil, libHTTP.CursorPagination{}, fmt.Errorf("get postgres connection: %w", err)
	}

	limit = libHTTP.ValidateLimit(limit, constants.DefaultPaginationLimit, constants.MaximumPaginationLimit)

	decodedCursor, cursorID, err := parseCursor(cursor)
	if err != nil {
		return nil, libHTTP.CursorPagination{}, err
	}

	var pagination libHTTP.CursorPagination

	result, err := common.WithTenantTx(
		ctx,
		connection,
		func(tx *sql.Tx) (rules entities.MatchRules, err error) {
			builder := squirrel.Select(strings.Split(matchRuleColumns, ", ")...).
				From("match_rules").
				Where(squirrel.Eq{"context_id": contextID.String()}).
				Where(squirrel.Eq{"type": ruleType.String()}).
				PlaceholderFormat(squirrel.Dollar)

			orderDirection := libHTTP.ValidateSortDirection("ASC")

			if cursor != "" {
				cond, cursorOrderDirection, buildErr := buildCursorConditions(ctx, tx, decodedCursor, cursorID, contextID)
				if buildErr != nil {
					return nil, buildErr
				}

				builder = builder.Where(cond)
				orderDirection = cursorOrderDirection
			}

			builder = builder.OrderBy("priority "+orderDirection, "id "+orderDirection).
				Limit(safeUint64(limit + 1))

			query, args, err := builder.ToSql()
			if err != nil {
				return nil, fmt.Errorf("build list match rules by type query: %w", err)
			}

			rules, err = executeMatchRulesQuery(ctx, tx, query, args)
			if err != nil {
				return nil, err
			}

			rules, pagination, err = paginateAndCalculateCursor(
				cursor,
				decodedCursor,
				rules,
				limit,
			)
			if err != nil {
				return nil, err
			}

			return rules, nil
		},
	)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to list match rules", err)

		logger.With(
			libLog.Any("context.id", contextID.String()),
			libLog.Any("rule.type", ruleType.String()),
			libLog.Any("error.message", err.Error()),
		).Log(ctx, libLog.LevelError, "failed to list match rules")

		return nil, libHTTP.CursorPagination{}, fmt.Errorf("failed to find match rules by context and type: %w", err)
	}

	return result, pagination, nil
}

// FindByPriority retrieves a match rule by context and priority.
func (repo *Repository) FindByPriority(
	ctx stdctx.Context,
	contextID uuid.UUID,
	priority int,
) (*entities.MatchRule, error) {
	if repo == nil || repo.provider == nil {
		return nil, ErrRepoNotInitialized
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "postgres.find_match_rule_by_priority")
	defer span.End()

	connection, err := repo.provider.GetPostgresConnection(ctx)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to get postgres connection", err)
		return nil, fmt.Errorf("get postgres connection: %w", err)
	}

	result, err := common.WithTenantTx(
		ctx,
		connection,
		func(tx *sql.Tx) (*entities.MatchRule, error) {
			row := tx.QueryRowContext(
				ctx,
				"SELECT "+matchRuleColumns+" FROM match_rules WHERE context_id = $1 AND priority = $2",
				contextID.String(),
				priority,
			)

			return scanMatchRule(row)
		},
	)
	if err != nil {
		if !errors.Is(err, sql.ErrNoRows) {
			libOpentelemetry.HandleSpanError(span, "failed to find match rule by priority", err)

			logger.With(
				libLog.Any("context.id", contextID.String()),
				libLog.Any("priority", priority),
				libLog.Any("error.message", err.Error()),
			).Log(ctx, libLog.LevelError, "failed to find match rule by priority")
		}

		return nil, fmt.Errorf("failed to find match rule by priority: %w", err)
	}

	return result, nil
}

// Update modifies an existing match rule.
func (repo *Repository) Update(
	ctx stdctx.Context,
	entity *entities.MatchRule,
) (*entities.MatchRule, error) {
	if repo == nil || repo.provider == nil {
		return nil, ErrRepoNotInitialized
	}

	if entity == nil {
		return nil, ErrMatchRuleEntityRequired
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "postgres.update_match_rule")
	defer span.End()

	result, err := common.WithTenantTxProvider(
		ctx,
		repo.provider,
		func(tx *sql.Tx) (*entities.MatchRule, error) {
			return repo.executeUpdate(ctx, tx, entity)
		},
	)
	if err != nil {
		if !errors.Is(err, sql.ErrNoRows) {
			libOpentelemetry.HandleSpanError(span, "failed to update match rule", err)

			logger.With(
				libLog.Any("context.id", entity.ContextID.String()),
				libLog.Any("match_rule.id", entity.ID.String()),
				libLog.Any("error.message", err.Error()),
			).Log(ctx, libLog.LevelError, "failed to update match rule")
		}

		return nil, fmt.Errorf("failed to update match rule: %w", err)
	}

	return result, nil
}

// UpdateWithTx modifies an existing match rule using the provided transaction.
func (repo *Repository) UpdateWithTx(
	ctx stdctx.Context,
	tx *sql.Tx,
	entity *entities.MatchRule,
) (*entities.MatchRule, error) {
	if repo == nil || repo.provider == nil {
		return nil, ErrRepoNotInitialized
	}

	if entity == nil {
		return nil, ErrMatchRuleEntityRequired
	}

	if tx == nil {
		return nil, ErrTransactionRequired
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "postgres.update_match_rule_with_tx")
	defer span.End()

	result, err := common.WithTenantTxOrExistingProvider(
		ctx,
		repo.provider,
		tx,
		func(innerTx *sql.Tx) (*entities.MatchRule, error) {
			return repo.executeUpdate(ctx, innerTx, entity)
		},
	)
	if err != nil {
		if !errors.Is(err, sql.ErrNoRows) {
			libOpentelemetry.HandleSpanError(span, "failed to update match rule", err)

			logger.With(
				libLog.Any("context.id", entity.ContextID.String()),
				libLog.Any("match_rule.id", entity.ID.String()),
				libLog.Any("error.message", err.Error()),
			).Log(ctx, libLog.LevelError, "failed to update match rule")
		}

		return nil, fmt.Errorf("failed to update match rule: %w", err)
	}

	return result, nil
}

// executeUpdate performs the actual match rule update within a transaction.
func (repo *Repository) executeUpdate(
	ctx stdctx.Context,
	tx *sql.Tx,
	entity *entities.MatchRule,
) (*entities.MatchRule, error) {
	entity.UpdatedAt = time.Now().UTC()

	model, err := NewMatchRulePostgreSQLModel(entity)
	if err != nil {
		return nil, err
	}

	result, err := tx.ExecContext(
		ctx,
		`UPDATE match_rules SET priority = $1, type = $2, config = $3, updated_at = $4
		WHERE context_id = $5 AND id = $6`,
		model.Priority,
		model.Type,
		model.Config,
		model.UpdatedAt,
		model.ContextID,
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

// Delete removes a match rule from the database.
func (repo *Repository) Delete(ctx stdctx.Context, contextID, id uuid.UUID) error {
	if repo == nil || repo.provider == nil {
		return ErrRepoNotInitialized
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "postgres.delete_match_rule")
	defer span.End()

	_, err := common.WithTenantTxProvider(ctx, repo.provider, func(tx *sql.Tx) (bool, error) {
		return repo.executeDelete(ctx, tx, contextID, id)
	})
	if err != nil {
		if !errors.Is(err, sql.ErrNoRows) {
			libOpentelemetry.HandleSpanError(span, "failed to delete match rule", err)

			logger.With(
				libLog.Any("context.id", contextID.String()),
				libLog.Any("match_rule.id", id.String()),
				libLog.Any("error.message", err.Error()),
			).Log(ctx, libLog.LevelError, "failed to delete match rule")
		}

		return fmt.Errorf("failed to delete match rule: %w", err)
	}

	return nil
}

// DeleteWithTx removes a match rule using the provided transaction.
func (repo *Repository) DeleteWithTx(
	ctx stdctx.Context,
	tx *sql.Tx,
	contextID, id uuid.UUID,
) error {
	if repo == nil || repo.provider == nil {
		return ErrRepoNotInitialized
	}

	if tx == nil {
		return ErrTransactionRequired
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "postgres.delete_match_rule_with_tx")
	defer span.End()

	_, err := common.WithTenantTxOrExistingProvider(
		ctx,
		repo.provider,
		tx,
		func(innerTx *sql.Tx) (bool, error) {
			return repo.executeDelete(ctx, innerTx, contextID, id)
		},
	)
	if err != nil {
		if !errors.Is(err, sql.ErrNoRows) {
			libOpentelemetry.HandleSpanError(span, "failed to delete match rule", err)

			logger.With(
				libLog.Any("context.id", contextID.String()),
				libLog.Any("match_rule.id", id.String()),
				libLog.Any("error.message", err.Error()),
			).Log(ctx, libLog.LevelError, "failed to delete match rule")
		}

		return fmt.Errorf("failed to delete match rule: %w", err)
	}

	return nil
}

// executeDelete performs the actual match rule deletion within a transaction.
func (repo *Repository) executeDelete(
	ctx stdctx.Context,
	tx *sql.Tx,
	contextID, id uuid.UUID,
) (bool, error) {
	result, err := tx.ExecContext(
		ctx,
		"DELETE FROM match_rules WHERE context_id = $1 AND id = $2",
		contextID.String(),
		id.String(),
	)
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

// ReorderPriorities updates the priority of match rules in the specified order.
func (repo *Repository) ReorderPriorities(
	ctx stdctx.Context,
	contextID uuid.UUID,
	ruleIDs []uuid.UUID,
) error {
	if repo == nil || repo.provider == nil {
		return ErrRepoNotInitialized
	}

	if len(ruleIDs) == 0 {
		return ErrRuleIDsRequired
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "postgres.reorder_match_rule_priorities")
	defer span.End()

	connection, err := repo.provider.GetPostgresConnection(ctx)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to get postgres connection", err)
		return fmt.Errorf("get postgres connection: %w", err)
	}

	_, err = common.WithTenantTx(ctx, connection, func(tx *sql.Tx) (bool, error) {
		// Offset priorities to avoid unique constraint collisions during reorder.
		offset := len(ruleIDs) + reorderPriorityOffset

		_, err := tx.ExecContext(
			ctx,
			"UPDATE match_rules SET priority = priority + $1 WHERE context_id = $2",
			offset,
			contextID.String(),
		)
		if err != nil {
			return false, err
		}

		query, args := buildReorderQuery(contextID, ruleIDs)

		result, err := tx.ExecContext(ctx, query, args...)
		if err != nil {
			return false, err
		}

		rowsAffected, err := result.RowsAffected()
		if err != nil {
			return false, err
		}

		if rowsAffected != int64(len(ruleIDs)) {
			return false, sql.ErrNoRows
		}

		return true, nil
	})
	if err != nil {
		if !errors.Is(err, sql.ErrNoRows) {
			libOpentelemetry.HandleSpanError(span, "failed to reorder match rule priorities", err)

			logger.With(
				libLog.Any("context.id", contextID.String()),
				libLog.Any("rule_ids.count", len(ruleIDs)),
				libLog.Any("error.message", err.Error()),
			).Log(ctx, libLog.LevelError, "failed to reorder match rule priorities")
		}

		return fmt.Errorf("failed to reorder match rule priorities: %w", err)
	}

	return nil
}

func scanMatchRule(scanner interface{ Scan(dest ...any) error }) (*entities.MatchRule, error) {
	var model MatchRulePostgreSQLModel
	if err := scanner.Scan(
		&model.ID,
		&model.ContextID,
		&model.Priority,
		&model.Type,
		&model.Config,
		&model.CreatedAt,
		&model.UpdatedAt,
	); err != nil {
		return nil, err
	}

	return model.ToEntity()
}

func buildReorderQuery(contextID uuid.UUID, ruleIDs []uuid.UUID) (string, []any) {
	args := make([]any, 0, 1+(len(ruleIDs)*argsPerRuleID))
	args = append(args, contextID.String())

	var builder strings.Builder

	builder.WriteString("UPDATE match_rules SET priority = CASE id")

	paramIndex := 2

	for index, ruleID := range ruleIDs {
		fmt.Fprintf(&builder, " WHEN $%d THEN $%d::int", paramIndex, paramIndex+1)

		args = append(args, ruleID.String(), index+1)

		paramIndex += 2
	}

	builder.WriteString(" END WHERE context_id = $1 AND id IN (")

	for index, ruleID := range ruleIDs {
		if index > 0 {
			builder.WriteString(", ")
		}

		fmt.Fprintf(&builder, "$%d", paramIndex)

		args = append(args, ruleID.String())

		paramIndex++
	}

	builder.WriteString(")")

	return builder.String(), args
}

func fetchCursorPriority(ctx stdctx.Context, tx *sql.Tx, cursor, contextID uuid.UUID) (int, error) {
	var cursorPriority int

	cursorQuery := "SELECT priority FROM match_rules WHERE id = $1 AND context_id = $2"

	if err := tx.QueryRowContext(ctx, cursorQuery, cursor.String(), contextID.String()).Scan(&cursorPriority); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return 0, ErrCursorNotFound
		}

		return 0, fmt.Errorf("validating cursor: %w", err)
	}

	return cursorPriority, nil
}

func executeMatchRulesQuery(
	ctx stdctx.Context,
	tx *sql.Tx,
	query string,
	args []any,
) (rules entities.MatchRules, err error) {
	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}

	defer func() {
		if closeErr := rows.Close(); closeErr != nil && err == nil {
			err = closeErr
		}
	}()

	const defaultMatchRulesCapacity = 32

	rules = make(entities.MatchRules, 0, defaultMatchRulesCapacity)

	for rows.Next() {
		rule, err := scanMatchRule(rows)
		if err != nil {
			return nil, err
		}

		rules = append(rules, rule)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return rules, nil
}

// parseCursor decodes and parses a cursor string into its components.
func parseCursor(cursor string) (libHTTP.Cursor, uuid.UUID, error) {
	decodedCursor := libHTTP.Cursor{Direction: libHTTP.CursorDirectionNext}

	var cursorID uuid.UUID

	if cursor == "" {
		return decodedCursor, cursorID, nil
	}

	parsedCursor, err := libHTTP.DecodeCursor(cursor)
	if err != nil {
		return libHTTP.Cursor{}, uuid.Nil, fmt.Errorf("%w: %w", libHTTP.ErrInvalidCursor, err)
	}

	decodedCursor = parsedCursor

	parsedID, err := uuid.Parse(decodedCursor.ID)
	if err != nil {
		return libHTTP.Cursor{}, uuid.Nil, fmt.Errorf("%w: %w", libHTTP.ErrInvalidCursor, err)
	}

	return decodedCursor, parsedID, nil
}

// buildCursorConditions validates cursor data and returns filter conditions and ordering.
func buildCursorConditions(
	ctx stdctx.Context,
	tx *sql.Tx,
	decodedCursor libHTTP.Cursor,
	cursorID uuid.UUID,
	contextID uuid.UUID,
) (squirrel.Sqlizer, string, error) {
	cursorPriority, err := fetchCursorPriority(ctx, tx, cursorID, contextID)
	if err != nil {
		if errors.Is(err, ErrCursorNotFound) {
			return nil, "", fmt.Errorf("%w: %w", libHTTP.ErrInvalidCursor, err)
		}

		return nil, "", err
	}

	if decodedCursor.Direction == libHTTP.CursorDirectionNext {
		return squirrel.Or{
			squirrel.Gt{"priority": cursorPriority},
			squirrel.And{
				squirrel.Eq{"priority": cursorPriority},
				squirrel.Gt{"id": cursorID.String()},
			},
		}, "ASC", nil
	}

	return squirrel.Or{
		squirrel.Lt{"priority": cursorPriority},
		squirrel.And{
			squirrel.Eq{"priority": cursorPriority},
			squirrel.Lt{"id": cursorID.String()},
		},
	}, "DESC", nil
}

// safeUint64 safely converts an int to uint64, returning 0 for negative values.
func safeUint64(n int) uint64 {
	if n < 0 {
		return 0
	}

	return uint64(n)
}

// paginateAndCalculateCursor handles pagination logic and cursor calculation for match rules.
func paginateAndCalculateCursor(
	cursor string,
	decodedCursor libHTTP.Cursor,
	rules entities.MatchRules,
	limit int,
) (entities.MatchRules, libHTTP.CursorPagination, error) {
	var pagination libHTTP.CursorPagination

	hasPagination := len(rules) > limit
	isFirstPage := cursor == "" || (!hasPagination && decodedCursor.Direction == libHTTP.CursorDirectionPrev)

	rules = libHTTP.PaginateRecords(
		isFirstPage,
		hasPagination,
		decodedCursor.Direction,
		rules,
		limit,
	)

	if len(rules) > 0 {
		page, err := libHTTP.CalculateCursor(
			isFirstPage,
			hasPagination,
			decodedCursor.Direction,
			rules[0].ID.String(),
			rules[len(rules)-1].ID.String(),
		)
		if err != nil {
			return nil, libHTTP.CursorPagination{}, fmt.Errorf("calculate cursor: %w", err)
		}

		pagination = page
	}

	return rules, pagination, nil
}
