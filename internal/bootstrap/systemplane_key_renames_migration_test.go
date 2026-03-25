//go:build unit

package bootstrap

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSystemplaneKeyRenameMigrations_AreScopedAndCollisionChecked(t *testing.T) {
	t.Parallel()

	files := []string{
		filepath.Join("..", "..", "migrations", "000020_systemplane_key_renames.up.sql"),
		filepath.Join("..", "..", "migrations", "000020_systemplane_key_renames.down.sql"),
	}

	for _, file := range files {
		file := file
		t.Run(filepath.Base(file), func(t *testing.T) {
			t.Parallel()

			contents, err := os.ReadFile(file)
			require.NoError(t, err)

			text := string(contents)
			assert.Contains(t, text, "current_setting(")
			assert.Contains(t, text, "kind = 'config'")
			assert.Contains(t, text, "scope = 'global'")
			assert.Contains(t, text, "subject = ''")
		})
	}
}
