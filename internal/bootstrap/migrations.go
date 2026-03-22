// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package bootstrap

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"

	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database/postgres"
	"github.com/golang-migrate/migrate/v4/source/iofs"

	libLog "github.com/LerianStudio/lib-commons/v4/commons/log"
	"github.com/LerianStudio/lib-commons/v4/commons/runtime"

	"github.com/LerianStudio/matcher/migrations"
)

// ErrDatabaseDirty indicates the database is in a dirty migration state requiring intervention.
var ErrDatabaseDirty = errors.New("database is in dirty migration state")

// ErrDirtyMigrationState indicates the database migration is in a dirty state requiring manual intervention.
var ErrDirtyMigrationState = errors.New("database migration dirty state requires manual intervention")

type migrationRunner interface {
	Version() (uint, bool, error)
	Force(int) error
	Up() error
	Close() (error, error)
}

// RunMigrations applies database migrations from the configured migrations path.
// It opens a dedicated connection for migrations to avoid issues with the connection pool.
// When allowDirtyRecovery is true (development), it will attempt to auto-recover from dirty state.
// In production (allowDirtyRecovery=false), dirty state requires manual intervention.
// Returns nil if migrations are successful or if there are no new migrations to apply.
func RunMigrations(ctx context.Context, dsn, dbName, migrationsPath string, logger libLog.Logger, allowDirtyRecovery bool) (retErr error) {
	if logger == nil {
		logger = &libLog.NopLogger{}
	}

	if migrationsPath == "" {
		logger.Log(ctx, libLog.LevelInfo, "Migrations path not configured, skipping auto-migration")
		return nil
	}

	logger.Log(ctx, libLog.LevelInfo, "Running database migrations from: "+migrationsPath)

	db, err := openMigrationDB(ctx, dsn)
	if err != nil {
		return err
	}
	defer db.Close()

	migrator, err := newMigrator(db, dbName, migrationsPath)
	if err != nil {
		return err
	}

	defer func() {
		closeErr := closeMigrator(migrator)
		if closeErr == nil {
			return
		}

		if retErr == nil {
			retErr = closeErr
			return
		}

		if logger != nil {
			logger.Log(ctx, libLog.LevelWarn, fmt.Sprintf("failed to close migrator: %v", closeErr))
		}
	}()

	if err := applyMigrations(ctx, migrator, logger, allowDirtyRecovery); err != nil {
		return err
	}

	return nil
}

func openMigrationDB(ctx context.Context, dsn string) (*sql.DB, error) {
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("migration context canceled before open: %w", err)
	}

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, fmt.Errorf("open migration connection: %w", err)
	}

	if err := db.PingContext(ctx); err != nil {
		if closeErr := db.Close(); closeErr != nil {
			return nil, fmt.Errorf("ping migration connection: %w (close connection: %w)", err, closeErr)
		}

		return nil, fmt.Errorf("ping migration connection: %w", err)
	}

	return db, nil
}

func newMigrator(db *sql.DB, dbName, _ string) (migrationRunner, error) {
	driver, err := postgres.WithInstance(db, &postgres.Config{
		DatabaseName:          dbName,
		MultiStatementEnabled: true,
	})
	if err != nil {
		return nil, fmt.Errorf("create migration driver: %w", err)
	}

	// Use embedded migrations from the migrations package.
	// This eliminates filesystem access requirements, enabling containers
	// to run with readOnlyRootFilesystem: true for enhanced security.
	source, err := iofs.New(migrations.FS, ".")
	if err != nil {
		return nil, fmt.Errorf("create migration source from embedded files: %w", err)
	}

	migrator, err := migrate.NewWithInstance("iofs", source, dbName, driver)
	if err != nil {
		return nil, fmt.Errorf("create migrator: %w", err)
	}

	return migrator, nil
}

func closeMigrator(migrator migrationRunner) error {
	sourceErr, dbErr := migrator.Close()
	if sourceErr == nil && dbErr == nil {
		return nil
	}

	if sourceErr != nil && dbErr != nil {
		return fmt.Errorf("close migrator: %w", errors.Join(sourceErr, dbErr))
	}

	if sourceErr != nil {
		return fmt.Errorf("close migrator source: %w", sourceErr)
	}

	return fmt.Errorf("close migrator db: %w", dbErr)
}

func applyMigrations(ctx context.Context, migrator migrationRunner, logger libLog.Logger, allowDirtyRecovery bool) error {
	if err := handleDirtyState(ctx, migrator, logger, allowDirtyRecovery); err != nil {
		return err
	}

	if err := runMigrationsUp(ctx, migrator, logger); err != nil {
		if errors.Is(err, migrate.ErrNoChange) {
			logger.Log(ctx, libLog.LevelInfo, "Database schema is up to date")
			return nil
		}

		return fmt.Errorf("apply migrations: %w", err)
	}

	return logMigrationVersion(ctx, migrator, logger)
}

func handleDirtyState(ctx context.Context, migrator migrationRunner, logger libLog.Logger, allowRecovery bool) error {
	version, dirty, err := migrator.Version()
	if err != nil {
		if errors.Is(err, migrate.ErrNilVersion) {
			return nil
		}

		return fmt.Errorf("check migration version: %w", err)
	}

	if !dirty {
		return nil
	}

	var previousVersion int

	if version == 0 {
		previousVersion = -1
	} else if version > uint(math.MaxInt) {
		return fmt.Errorf("migration version %d exceeds maximum safe value: %w", version, ErrDatabaseDirty)
	} else {
		previousVersion = int(version) - 1
	}

	if !allowRecovery {
		return fmt.Errorf(
			"%w: database is dirty at version %d. "+
				"This requires manual intervention:\n"+
				"  1. Inspect the database schema for partial changes\n"+
				"  2. Fix or rollback the incomplete migration\n"+
				"  3. Run: UPDATE schema_migrations SET version = %d, dirty = false;\n"+
				"  4. Restart the application",
			ErrDirtyMigrationState, version, previousVersion,
		)
	}

	logger.Log(ctx, libLog.LevelWarn, fmt.Sprintf("Database is dirty at version %d (development mode: attempting auto-recovery)", version))

	if err := migrator.Force(previousVersion); err != nil {
		return fmt.Errorf("force migration version %d: %w", previousVersion, err)
	}

	logger.Log(ctx, libLog.LevelInfo, fmt.Sprintf("Auto-recovered: forced migration to version %d, retrying migrations", previousVersion))

	return nil
}

func runMigrationsUp(ctx context.Context, migrator migrationRunner, logger libLog.Logger) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("migration context canceled before apply: %w", err)
	}

	migrateErrCh := make(chan error, 1)

	runtime.SafeGoWithContextAndComponent(
		ctx,
		logger,
		"bootstrap",
		"migrations-up",
		runtime.KeepRunning,
		func(ctx context.Context) {
			migrateErrCh <- migrator.Up()
		},
	)

	select {
	case <-ctx.Done():
		return fmt.Errorf("migration context canceled during apply: %w", ctx.Err())
	case err := <-migrateErrCh:
		return err
	}
}

func logMigrationVersion(ctx context.Context, migrator migrationRunner, logger libLog.Logger) error {
	version, dirty, err := migrator.Version()
	if err != nil && !errors.Is(err, migrate.ErrNilVersion) {
		return fmt.Errorf("get migration version: %w", err)
	}

	if dirty {
		logger.Log(ctx, libLog.LevelWarn, fmt.Sprintf("Migrations applied successfully (version: %d, dirty: %v)", version, dirty))
		return nil
	}

	logger.Log(ctx, libLog.LevelInfo, fmt.Sprintf("Migrations applied successfully (version: %d, dirty: %v)", version, dirty))

	return nil
}
