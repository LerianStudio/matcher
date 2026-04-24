// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package bootstrap

import (
	"context"
	"database/sql"
	"testing"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPreflightMigration022Apply(t *testing.T) {
	t.Parallel()

	t.Run("skips when legacy tables are absent", func(t *testing.T) {
		t.Parallel()

		db, mock, err := sqlmock.New()
		require.NoError(t, err)
		defer db.Close()

		mock.ExpectQuery("SELECT EXISTS").WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(false))
		mock.ExpectQuery("SELECT EXISTS").WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(false))

		err = preflightMigration022Apply(context.Background(), db, 0)
		require.NoError(t, err)
		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("blocks legacy context rates", func(t *testing.T) {
		t.Parallel()

		db, mock, err := sqlmock.New()
		require.NoError(t, err)
		defer db.Close()

		mock.ExpectQuery("SELECT EXISTS").WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(true))
		mock.ExpectQuery("SELECT EXISTS").WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(true))
		mock.ExpectQuery(`SELECT COUNT\(\*\) FROM reconciliation_contexts WHERE rate_id IS NOT NULL`).WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(2))

		err = preflightMigration022Apply(context.Background(), db, 21)
		require.ErrorIs(t, err, ErrMigration022BlockedLegacyContextRates)
		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("blocks legacy fee variances when fee_schedule_id exists", func(t *testing.T) {
		t.Parallel()

		db, mock, err := sqlmock.New()
		require.NoError(t, err)
		defer db.Close()

		mock.ExpectQuery("SELECT EXISTS").WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(true))
		mock.ExpectQuery("SELECT EXISTS").WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(true))
		mock.ExpectQuery(`SELECT COUNT\(\*\) FROM reconciliation_contexts WHERE rate_id IS NOT NULL`).WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))
		mock.ExpectQuery("SELECT EXISTS").WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(true))
		mock.ExpectQuery(`SELECT COUNT\(\*\) FROM match_fee_variances WHERE fee_schedule_id IS NULL`).WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(3))

		err = preflightMigration022Apply(context.Background(), db, 21)
		require.ErrorIs(t, err, ErrMigration022BlockedLegacyFeeVariances)
		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("blocks legacy fee variances before fee_schedule_id exists", func(t *testing.T) {
		t.Parallel()

		db, mock, err := sqlmock.New()
		require.NoError(t, err)
		defer db.Close()

		mock.ExpectQuery("SELECT EXISTS").WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(true))
		mock.ExpectQuery("SELECT EXISTS").WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(true))
		mock.ExpectQuery(`SELECT COUNT\(\*\) FROM reconciliation_contexts WHERE rate_id IS NOT NULL`).WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))
		mock.ExpectQuery("SELECT EXISTS").WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(false))
		mock.ExpectQuery(`SELECT COUNT\(\*\) FROM match_fee_variances`).WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(1))

		err = preflightMigration022Apply(context.Background(), db, 1)
		require.ErrorIs(t, err, ErrMigration022BlockedLegacyFeeVariances)
		require.NoError(t, mock.ExpectationsWereMet())
	})
}

func TestCurrentMigrationState(t *testing.T) {
	t.Parallel()

	t.Run("returns zero when schema_migrations is absent", func(t *testing.T) {
		t.Parallel()

		db, mock, err := sqlmock.New()
		require.NoError(t, err)
		defer db.Close()

		mock.ExpectQuery("SELECT to_regclass").WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(false))

		state, err := currentMigrationState(context.Background(), db)
		require.NoError(t, err)
		assert.Equal(t, 0, state.version)
		assert.False(t, state.dirty)
		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("returns version and dirty flag", func(t *testing.T) {
		t.Parallel()

		db, mock, err := sqlmock.New()
		require.NoError(t, err)
		defer db.Close()

		mock.ExpectQuery("SELECT to_regclass").WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(true))
		mock.ExpectQuery(`SELECT version, dirty FROM schema_migrations LIMIT 1`).WillReturnRows(sqlmock.NewRows([]string{"version", "dirty"}).AddRow(22, true))

		state, err := currentMigrationState(context.Background(), db)
		require.NoError(t, err)
		assert.Equal(t, 22, state.version)
		assert.True(t, state.dirty)
		require.NoError(t, mock.ExpectationsWereMet())
	})
}

func TestCurrentMigrationVersion(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	mock.ExpectQuery("SELECT to_regclass").WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(true))
	mock.ExpectQuery(`SELECT version, dirty FROM schema_migrations LIMIT 1`).WillReturnRows(sqlmock.NewRows([]string{"version", "dirty"}).AddRow(7, false))

	state, err := currentMigrationState(context.Background(), db)
	require.NoError(t, err)
	assert.Equal(t, 7, state.version)
	assert.False(t, state.dirty)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestPreflightMigrationGoto(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	mock.ExpectQuery("SELECT to_regclass").WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(true))
	mock.ExpectQuery(`SELECT version, dirty FROM schema_migrations LIMIT 1`).WillReturnRows(sqlmock.NewRows([]string{"version", "dirty"}).AddRow(21, false))
	mock.ExpectQuery("SELECT EXISTS").WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(true))
	mock.ExpectQuery("SELECT EXISTS").WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(true))
	mock.ExpectQuery(`SELECT COUNT\(\*\) FROM reconciliation_contexts WHERE rate_id IS NOT NULL`).WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(2))

	originalOpen := openMigrationDB
	openMigrationDB = func(context.Context, string) (*sql.DB, error) {
		return db, nil
	}
	defer func() { openMigrationDB = originalOpen }()

	err = PreflightMigrationGoto(context.Background(), "dsn", 22)
	require.ErrorIs(t, err, ErrMigration022BlockedLegacyContextRates)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestPreflightMigrationFunctions_BlockDirtyState(t *testing.T) {
	tests := []struct {
		name string
		run  func(context.Context, string) error
	}{
		{name: "up", run: PreflightMigrationUp},
		{name: "down", run: PreflightMigrationDownOne},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			require.NoError(t, err)
			defer db.Close()

			mock.ExpectQuery("SELECT to_regclass").WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(true))
			mock.ExpectQuery(`SELECT version, dirty FROM schema_migrations LIMIT 1`).WillReturnRows(sqlmock.NewRows([]string{"version", "dirty"}).AddRow(22, true))

			originalOpen := openMigrationDB
			openMigrationDB = func(context.Context, string) (*sql.DB, error) {
				return db, nil
			}
			defer func() { openMigrationDB = originalOpen }()

			err = testCase.run(context.Background(), "dsn")
			require.ErrorIs(t, err, ErrDirtyMigrationState)
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestPreflightMigrationGoto_BlocksDirtyState(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	mock.ExpectQuery("SELECT to_regclass").WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(true))
	mock.ExpectQuery(`SELECT version, dirty FROM schema_migrations LIMIT 1`).WillReturnRows(sqlmock.NewRows([]string{"version", "dirty"}).AddRow(21, true))

	originalOpen := openMigrationDB
	openMigrationDB = func(context.Context, string) (*sql.DB, error) {
		return db, nil
	}
	defer func() { openMigrationDB = originalOpen }()

	err = PreflightMigrationGoto(context.Background(), "dsn", 22)
	require.ErrorIs(t, err, ErrDirtyMigrationState)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestPreflightMigrationUp_PropagatesOpenError(t *testing.T) {
	originalOpen := openMigrationDB
	openMigrationDB = func(context.Context, string) (*sql.DB, error) {
		return nil, assert.AnError
	}
	defer func() { openMigrationDB = originalOpen }()

	err := PreflightMigrationUp(context.Background(), "dsn")
	require.ErrorIs(t, err, assert.AnError)
}
