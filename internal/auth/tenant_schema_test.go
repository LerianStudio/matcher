//go:build unit

package auth

import (
	"context"
	"database/sql"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/require"
)

func TestQuoteIdentifierEscapesQuotes(t *testing.T) {
	t.Parallel()

	require.Equal(t, "\"tenant\"", QuoteIdentifier("tenant"))
	require.Equal(t, "\"tenant\"\"schema\"", QuoteIdentifier("tenant\"schema"))
}

//nolint:paralleltest // This test modifies global state (default tenant settings)
func TestApplyTenantSchema_DefaultTenantNoOp(t *testing.T) {
	ctx := context.Background()
	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	require.NoError(t, SetDefaultTenantID(DefaultTenantID))
	require.NoError(t, ApplyTenantSchema(ctx, db))
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestApplyTenantSchema_NonTxExecutor(t *testing.T) {
	t.Parallel()

	ctx := context.WithValue(
		context.Background(),
		TenantIDKey,
		"550e8400-e29b-41d4-a716-446655440000",
	)
	db, _, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	require.Error(t, ApplyTenantSchema(ctx, db))
}

func TestApplyTenantSchema_InvalidTenantID(t *testing.T) {
	t.Parallel()

	ctx := context.WithValue(context.Background(), TenantIDKey, "not-a-uuid")
	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	mock.ExpectBegin()
	mock.ExpectRollback()

	tx, err := db.BeginTx(ctx, nil)
	require.NoError(t, err)

	defer func() {
		_ = tx.Rollback()
	}()

	require.Error(t, ApplyTenantSchema(ctx, tx))
	require.NoError(t, tx.Rollback())
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestApplyTenantSchema_UsesTransaction(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	ctx := context.WithValue(
		context.Background(),
		TenantIDKey,
		"550e8400-e29b-41d4-a716-446655440000",
	)

	mock.ExpectBegin()
	mock.ExpectExec("SET LOCAL search_path TO \"550e8400-e29b-41d4-a716-446655440000\", public").
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	tx, err := db.BeginTx(ctx, nil)
	require.NoError(t, err)

	defer func() {
		_ = tx.Rollback()
	}()

	require.NoError(t, ApplyTenantSchema(ctx, tx))
	require.NoError(t, tx.Commit())
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestApplyTenantSchema_RejectsConnExecutor(t *testing.T) {
	t.Parallel()

	db, _, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	ctx := context.WithValue(
		context.Background(),
		TenantIDKey,
		"550e8400-e29b-41d4-a716-446655440000",
	)

	conn, err := db.Conn(ctx)
	require.NoError(t, err)

	defer conn.Close()

	require.Error(t, ApplyTenantSchema(ctx, conn))
}

var (
	_ SQLExecutor = (*sql.Tx)(nil)
	_ SQLExecutor = (*sql.Conn)(nil)
)

func TestApplyTenantSchema_PreventsSQLInjection(t *testing.T) {
	t.Parallel()

	injectionPayloads := []struct {
		name     string
		tenantID string
	}{
		{"single quote escape", "'; DROP SCHEMA public; --"},
		{"union injection", "550e8400'; SELECT * FROM passwords; --"},
		{"comment injection", "tenant-- comment"},
		{"semicolon terminator", "tenant; DELETE FROM users; --"},
		{"newline injection", "tenant\n; DROP TABLE audit; --"},
		{"double quote injection", `"; DROP TABLE users; --`},
		{"uuid-like with injection", "550e8400-e29b-41d4-a716'; DROP TABLE--"},
	}

	for _, tt := range injectionPayloads {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			db, mock, err := sqlmock.New()
			require.NoError(t, err)

			defer db.Close()

			ctx := context.WithValue(context.Background(), TenantIDKey, tt.tenantID)

			mock.ExpectBegin()
			mock.ExpectRollback()

			tx, err := db.BeginTx(ctx, nil)
			require.NoError(t, err)

			err = ApplyTenantSchema(ctx, tx)
			require.Error(t, err, "should reject non-UUID tenant ID with SQL injection payload")

			_ = tx.Rollback()
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestQuoteIdentifier_EscapesSQLInjectionCharacters(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input    string
		expected string
	}{
		{`test"injection`, `"test""injection"`},
		{`'; DROP TABLE--`, `"'; DROP TABLE--"`},
		{`test; DELETE FROM`, `"test; DELETE FROM"`},
		{`a"b"c"d`, `"a""b""c""d"`},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			t.Parallel()
			result := QuoteIdentifier(tt.input)
			require.Equal(t, tt.expected, result)
		})
	}
}
