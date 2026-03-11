//go:build unit

package migrations

import (
	"io"
	"strings"
	"testing"

	"github.com/golang-migrate/migrate/v4/source/iofs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFS_ContainsSQLFiles(t *testing.T) {
	t.Parallel()

	entries, err := FS.ReadDir(".")
	require.NoError(t, err)
	assert.NotEmpty(t, entries, "embedded FS should contain SQL migration files")

	hasSQLFile := false

	for _, entry := range entries {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".sql") {
			hasSQLFile = true

			break
		}
	}

	assert.True(t, hasSQLFile, "embedded FS should contain at least one .sql file")
}

func TestFS_InitializesMigrationSource(t *testing.T) {
	t.Parallel()

	driver, err := iofs.New(FS, ".")
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, driver.Close())
	})

	firstVersion, err := driver.First()
	require.NoError(t, err)
	assert.NotZero(t, firstVersion)

	reader, identifier, err := driver.ReadUp(firstVersion)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, reader.Close())
	})

	body, err := io.ReadAll(reader)
	require.NoError(t, err)
	assert.NotEmpty(t, identifier)
	assert.NotEmpty(t, body)
}

func TestFS_MigrationFilesAreReadable(t *testing.T) {
	t.Parallel()

	entries, err := FS.ReadDir(".")
	require.NoError(t, err)

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		data, readErr := FS.ReadFile(entry.Name())
		assert.NoError(t, readErr, "should be able to read %s", entry.Name())
		assert.NotEmpty(t, data, "file %s should not be empty", entry.Name())
	}
}
