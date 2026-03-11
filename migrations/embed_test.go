//go:build unit

package migrations

import (
	"testing"

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
		if !entry.IsDir() && len(entry.Name()) > 4 && entry.Name()[len(entry.Name())-4:] == ".sql" {
			hasSQLFile = true

			break
		}
	}

	assert.True(t, hasSQLFile, "embedded FS should contain at least one .sql file")
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
