// Package bootstrap contains startup wiring, migration orchestration, and CI-facing migration preflight helpers.
package bootstrap

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

var openMigrationDB = openMigrationDBImpl

const irreversibleMigration022Version = 22

var (
	// ErrMigration022BlockedLegacyContextRates indicates migration 022 cannot run until legacy context-level rate links are removed.
	ErrMigration022BlockedLegacyContextRates = errors.New("migration 022 blocked: backfill legacy context rates before cutover")
	// ErrMigration022BlockedLegacyFeeVariances indicates migration 022 cannot run until legacy fee variance rows are backfilled or archived.
	ErrMigration022BlockedLegacyFeeVariances = errors.New("migration 022 blocked: archive or backfill legacy fee variances before cutover")
	// ErrMigration022Irreversible indicates supported rollback tooling cannot cross migration 022.
	ErrMigration022Irreversible = errors.New("migration 022 is intentionally irreversible")
)

// PreflightMigrationUp validates supported migration-up execution paths before running migrate.Up().
func PreflightMigrationUp(ctx context.Context, dsn string) error {
	db, err := openMigrationDB(ctx, dsn)
	if err != nil {
		return err
	}
	defer db.Close()

	state, err := currentMigrationState(ctx, db)
	if err != nil {
		return err
	}

	if state.dirty {
		return formatDirtyMigrationStateError(state.version, state.previousVersion())
	}

	return preflightMigration022Apply(ctx, db, state.version)
}

// PreflightMigrationDownOne blocks supported rollback paths that would traverse the irreversible migration 022.
func PreflightMigrationDownOne(ctx context.Context, dsn string) error {
	db, err := openMigrationDB(ctx, dsn)
	if err != nil {
		return err
	}
	defer db.Close()

	state, err := currentMigrationState(ctx, db)
	if err != nil {
		return err
	}

	if state.dirty {
		return formatDirtyMigrationStateError(state.version, state.previousVersion())
	}

	if state.version == irreversibleMigration022Version {
		return fmt.Errorf("%w: current version %d cannot roll back to %d", ErrMigration022Irreversible, state.version, state.version-1)
	}

	return nil
}

// PreflightMigrationGoto blocks supported goto paths that would cross below irreversible migration 022.
func PreflightMigrationGoto(ctx context.Context, dsn string, targetVersion int) error {
	db, err := openMigrationDB(ctx, dsn)
	if err != nil {
		return err
	}
	defer db.Close()

	state, err := currentMigrationState(ctx, db)
	if err != nil {
		return err
	}

	if state.dirty {
		return formatDirtyMigrationStateError(state.version, state.previousVersion())
	}

	if state.version >= irreversibleMigration022Version && targetVersion < irreversibleMigration022Version {
		return fmt.Errorf("%w: current version %d cannot go to %d", ErrMigration022Irreversible, state.version, targetVersion)
	}

	if state.version < irreversibleMigration022Version && targetVersion >= irreversibleMigration022Version {
		return preflightMigration022Apply(ctx, db, state.version)
	}

	return nil
}

func preflightPendingMigrations(ctx context.Context, db *sql.DB) error {
	state, err := currentMigrationState(ctx, db)
	if err != nil {
		return err
	}

	if state.dirty {
		return formatDirtyMigrationStateError(state.version, state.previousVersion())
	}

	if err := preflightMigration022Apply(ctx, db, state.version); err != nil {
		return err
	}

	return nil
}

func preflightMigration022Apply(ctx context.Context, db *sql.DB, currentVersion int) error {
	if currentVersion >= irreversibleMigration022Version {
		return nil
	}

	hasVarianceTable, err := tableExistsForPreflight(ctx, db, "match_fee_variances")
	if err != nil {
		return err
	}

	hasContextRateID, err := columnExistsForPreflight(ctx, db, "reconciliation_contexts", "rate_id")
	if err != nil {
		return err
	}

	if hasContextRateID {
		legacyContextRateCount, err := countRows(ctx, db, `SELECT COUNT(*) FROM reconciliation_contexts WHERE rate_id IS NOT NULL`)
		if err != nil {
			return fmt.Errorf("check migration 022 legacy context rates: %w", err)
		}

		if legacyContextRateCount > 0 {
			return fmt.Errorf("%w: found %d context rows", ErrMigration022BlockedLegacyContextRates, legacyContextRateCount)
		}
	}

	if !hasVarianceTable {
		return nil
	}

	hasVarianceScheduleID, err := columnExistsForPreflight(ctx, db, "match_fee_variances", "fee_schedule_id")
	if err != nil {
		return err
	}

	legacyVarianceQuery := `SELECT COUNT(*) FROM match_fee_variances WHERE fee_schedule_id IS NULL`
	if !hasVarianceScheduleID {
		legacyVarianceQuery = `SELECT COUNT(*) FROM match_fee_variances`
	}

	legacyVarianceCount, err := countRows(ctx, db, legacyVarianceQuery)
	if err != nil {
		return fmt.Errorf("check migration 022 legacy fee variances: %w", err)
	}

	if legacyVarianceCount > 0 {
		return fmt.Errorf("%w: found %d variance rows", ErrMigration022BlockedLegacyFeeVariances, legacyVarianceCount)
	}

	return nil
}

func tableExistsForPreflight(ctx context.Context, db *sql.DB, tableName string) (bool, error) {
	var exists bool

	err := db.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT 1
			FROM information_schema.tables
			WHERE table_schema = current_schema()
			  AND table_name = $1
		)
	`, tableName).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("check table %s: %w", tableName, err)
	}

	return exists, nil
}

func columnExistsForPreflight(ctx context.Context, db *sql.DB, tableName, columnName string) (bool, error) {
	var exists bool

	err := db.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT 1
			FROM information_schema.columns
			WHERE table_schema = current_schema()
			  AND table_name = $1
			  AND column_name = $2
		)
	`, tableName, columnName).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("check column %s.%s: %w", tableName, columnName, err)
	}

	return exists, nil
}

func countRows(ctx context.Context, db *sql.DB, query string) (int, error) {
	var count int
	if err := db.QueryRowContext(ctx, query).Scan(&count); err != nil {
		return 0, err
	}

	return count, nil
}

type migrationState struct {
	version int
	dirty   bool
}

func (state migrationState) previousVersion() int {
	if state.version == 0 {
		return -1
	}

	return state.version - 1
}

func currentMigrationState(ctx context.Context, db *sql.DB) (migrationState, error) {
	var hasSchemaMigrations bool

	err := db.QueryRowContext(ctx, `SELECT to_regclass(current_schema() || '.schema_migrations') IS NOT NULL`).Scan(&hasSchemaMigrations)
	if err != nil {
		return migrationState{}, fmt.Errorf("check schema_migrations existence: %w", err)
	}

	if !hasSchemaMigrations {
		return migrationState{}, nil
	}

	state := migrationState{}
	if err := db.QueryRowContext(ctx, `SELECT version, dirty FROM schema_migrations LIMIT 1`).Scan(&state.version, &state.dirty); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return migrationState{}, nil
		}

		return migrationState{}, fmt.Errorf("read schema_migrations state: %w", err)
	}

	return state, nil
}
