// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package fee_schedule

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/Masterminds/squirrel"
)

// scanScheduleRows iterates over sql.Rows and scans each row into a PostgreSQLModel.
func scanScheduleRows(rows *sql.Rows) ([]*PostgreSQLModel, error) {
	var models []*PostgreSQLModel

	for rows.Next() {
		model, scanErr := scanSchedule(rows)
		if scanErr != nil {
			return nil, scanErr
		}

		models = append(models, model)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return models, nil
}

func scanSchedule(scanner interface{ Scan(dest ...any) error }) (*PostgreSQLModel, error) {
	var model PostgreSQLModel
	if err := scanner.Scan(
		&model.ID,
		&model.TenantID,
		&model.Name,
		&model.Currency,
		&model.ApplicationOrder,
		&model.RoundingScale,
		&model.RoundingMode,
		&model.CreatedAt,
		&model.UpdatedAt,
	); err != nil {
		return nil, err
	}

	return &model, nil
}

func queryItems(ctx context.Context, tx *sql.Tx, scheduleID string) ([]ItemPostgreSQLModel, error) {
	rows, err := tx.QueryContext(ctx,
		"SELECT "+itemColumns+" FROM fee_schedule_items WHERE fee_schedule_id = $1 ORDER BY priority",
		scheduleID,
	)
	if err != nil {
		return nil, fmt.Errorf("query fee schedule items: %w", err)
	}

	defer func() {
		_ = rows.Close()
	}()

	var items []ItemPostgreSQLModel

	for rows.Next() {
		var item ItemPostgreSQLModel
		if err := rows.Scan(
			&item.ID,
			&item.FeeScheduleID,
			&item.Name,
			&item.Priority,
			&item.StructureType,
			&item.StructureData,
			&item.CreatedAt,
			&item.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan fee schedule item: %w", err)
		}

		items = append(items, item)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return items, nil
}

func queryItemsForSchedules(ctx context.Context, tx *sql.Tx, scheduleIDs []string) (map[string][]ItemPostgreSQLModel, error) {
	if len(scheduleIDs) == 0 {
		return make(map[string][]ItemPostgreSQLModel), nil
	}

	query, args, err := squirrel.StatementBuilder.
		PlaceholderFormat(squirrel.Dollar).
		Select(itemColumns).
		From("fee_schedule_items").
		Where(squirrel.Eq{"fee_schedule_id": scheduleIDs}).
		OrderBy("fee_schedule_id, priority").
		ToSql()
	if err != nil {
		return nil, fmt.Errorf("build fee schedule items batch query: %w", err)
	}

	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query fee schedule items batch: %w", err)
	}

	defer func() { _ = rows.Close() }()

	grouped := make(map[string][]ItemPostgreSQLModel)

	for rows.Next() {
		var item ItemPostgreSQLModel
		if err := rows.Scan(
			&item.ID,
			&item.FeeScheduleID,
			&item.Name,
			&item.Priority,
			&item.StructureType,
			&item.StructureData,
			&item.CreatedAt,
			&item.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan fee schedule item: %w", err)
		}

		key := item.FeeScheduleID.String()
		grouped[key] = append(grouped[key], item)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return grouped, nil
}
