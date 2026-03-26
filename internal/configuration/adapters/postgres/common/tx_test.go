//go:build unit

package common

import (
	"context"
	"database/sql"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/shared/infrastructure/testutil"
)

func TestWithTenantTxProvider_NilProvider(t *testing.T) {
	t.Parallel()

	_, err := WithTenantTxProvider(context.Background(), nil, func(tx *sql.Tx) (int, error) {
		return 42, nil
	})
	require.Error(t, err)
}

func TestWithTenantTxOrExistingProvider_NilProvider(t *testing.T) {
	t.Parallel()

	_, err := WithTenantTxOrExistingProvider(context.Background(), nil, nil, func(tx *sql.Tx) (int, error) {
		return 42, nil
	})
	require.Error(t, err)
}

func TestWithTenantReadQuery_NilProvider(t *testing.T) {
	t.Parallel()

	_, err := WithTenantReadQuery(context.Background(), nil, func(qe QueryExecutor) (int, error) {
		return 42, nil
	})
	require.Error(t, err)
}

func TestWithTenantReadQuery_UsesProvider(t *testing.T) {
	t.Parallel()

	db, _, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	provider := testutil.NewMockProviderFromDB(t, db)

	result, err := WithTenantReadQuery(context.Background(), provider, func(qe QueryExecutor) (int, error) {
		return 1, nil
	})
	require.NoError(t, err)
	require.Equal(t, 1, result)
}
