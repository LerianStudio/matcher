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

	"github.com/google/uuid"

	libCommons "github.com/LerianStudio/lib-commons/v5/commons"
	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v5/commons/opentelemetry"

	"github.com/LerianStudio/matcher/internal/configuration/adapters/postgres/common"
)

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

	_, err := common.WithTenantTxProvider(ctx, repo.provider, func(tx *sql.Tx) (bool, error) {
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
				libLog.Err(err),
			).Log(ctx, libLog.LevelError, "failed to reorder match rule priorities")
		}

		return fmt.Errorf("failed to reorder match rule priorities: %w", err)
	}

	return nil
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
