// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package match_rule

import (
	stdctx "context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/Masterminds/squirrel"
	"github.com/google/uuid"

	libCommons "github.com/LerianStudio/lib-commons/v5/commons"
	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"
	libHTTP "github.com/LerianStudio/lib-commons/v5/commons/net/http"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v5/commons/opentelemetry"

	"github.com/LerianStudio/matcher/internal/configuration/adapters/postgres/common"
	"github.com/LerianStudio/matcher/internal/configuration/domain/entities"
	"github.com/LerianStudio/matcher/internal/shared/constants"
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
)

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

	result, err := common.WithTenantTxProvider(
		ctx,
		repo.provider,
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
				libLog.Err(err),
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

	limit = libHTTP.ValidateLimit(limit, constants.DefaultPaginationLimit, constants.MaximumPaginationLimit)

	decodedCursor, cursorID, err := parseCursor(cursor)
	if err != nil {
		return nil, libHTTP.CursorPagination{}, err
	}

	var pagination libHTTP.CursorPagination

	result, err := common.WithTenantTxProvider(
		ctx,
		repo.provider,
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
			libLog.Err(err),
		).Log(ctx, libLog.LevelError, "failed to list match rules")

		return nil, libHTTP.CursorPagination{}, fmt.Errorf("failed to find match rules by context: %w", err)
	}

	return result, pagination, nil
}

// FindByContextIDWithTx retrieves all match rules for a context using cursor-based
// pagination within an existing transaction. This enables consistent snapshot reads
// when the caller already holds a transaction (e.g. clone operations).
func (repo *Repository) FindByContextIDWithTx(
	ctx stdctx.Context,
	tx *sql.Tx,
	contextID uuid.UUID,
	cursor string,
	limit int,
) (entities.MatchRules, libHTTP.CursorPagination, error) {
	if repo == nil || repo.provider == nil {
		return nil, libHTTP.CursorPagination{}, ErrRepoNotInitialized
	}

	if tx == nil {
		return nil, libHTTP.CursorPagination{}, ErrTransactionRequired
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "postgres.find_match_rules_by_context_with_tx")
	defer span.End()

	limit = libHTTP.ValidateLimit(limit, constants.DefaultPaginationLimit, constants.MaximumPaginationLimit)

	decodedCursor, cursorID, err := parseCursor(cursor)
	if err != nil {
		return nil, libHTTP.CursorPagination{}, err
	}

	var pagination libHTTP.CursorPagination

	result, err := common.WithTenantTxOrExistingProvider(
		ctx,
		repo.provider,
		tx,
		func(innerTx *sql.Tx) (rules entities.MatchRules, err error) {
			builder := squirrel.Select(strings.Split(matchRuleColumns, ", ")...).
				From("match_rules").
				Where(squirrel.Eq{"context_id": contextID.String()}).
				PlaceholderFormat(squirrel.Dollar)

			orderDirection := libHTTP.ValidateSortDirection("ASC")

			if cursor != "" {
				cond, cursorOrderDirection, buildErr := buildCursorConditions(ctx, innerTx, decodedCursor, cursorID, contextID)
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

			rules, err = executeMatchRulesQuery(ctx, innerTx, query, args)
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
		libOpentelemetry.HandleSpanError(span, "failed to list match rules with tx", err)

		logger.With(
			libLog.Any("context.id", contextID.String()),
			libLog.Err(err),
		).Log(ctx, libLog.LevelError, "failed to list match rules with tx")

		return nil, libHTTP.CursorPagination{}, fmt.Errorf("failed to find match rules by context with tx: %w", err)
	}

	return result, pagination, nil
}

// FindByContextIDAndType retrieves match rules for a context filtered by type using cursor-based pagination.
func (repo *Repository) FindByContextIDAndType(
	ctx stdctx.Context,
	contextID uuid.UUID,
	ruleType shared.RuleType,
	cursor string,
	limit int,
) (entities.MatchRules, libHTTP.CursorPagination, error) {
	if repo == nil || repo.provider == nil {
		return nil, libHTTP.CursorPagination{}, ErrRepoNotInitialized
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "postgres.find_match_rules_by_context_and_type")
	defer span.End()

	limit = libHTTP.ValidateLimit(limit, constants.DefaultPaginationLimit, constants.MaximumPaginationLimit)

	decodedCursor, cursorID, err := parseCursor(cursor)
	if err != nil {
		return nil, libHTTP.CursorPagination{}, err
	}

	var pagination libHTTP.CursorPagination

	result, err := common.WithTenantTxProvider(
		ctx,
		repo.provider,
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
			libLog.Err(err),
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

	result, err := common.WithTenantTxProvider(
		ctx,
		repo.provider,
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
				libLog.Err(err),
			).Log(ctx, libLog.LevelError, "failed to find match rule by priority")
		}

		return nil, fmt.Errorf("failed to find match rule by priority: %w", err)
	}

	return result, nil
}
