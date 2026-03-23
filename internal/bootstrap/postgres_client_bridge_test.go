//go:build unit

package bootstrap

import (
	"context"
	"database/sql"
	"testing"

	"github.com/bxcodec/dbresolver/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewLibPostgresClientFromResolver_NilResolver(t *testing.T) {
	t.Parallel()

	client := newLibPostgresClientFromResolver(nil)
	require.NotNil(t, client)
}

func TestNewLibPostgresClientFromResolver_WithPrimaryDB(t *testing.T) {
	t.Parallel()

	db := new(sql.DB)
	resolver := dbresolver.New(dbresolver.WithPrimaryDBs(db))

	client := newLibPostgresClientFromResolver(resolver)
	require.NotNil(t, client)

	// Verify the resolver is accessible
	resolved, err := client.Resolver(context.Background())
	require.NoError(t, err)

	primaryDBs := resolved.PrimaryDBs()
	require.Len(t, primaryDBs, 1)
	assert.Same(t, db, primaryDBs[0])
}
