//go:build unit

package swagger

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSwaggerSpec_DoesNotContainLegacyAPIPrefix(t *testing.T) {
	t.Parallel()

	files := []string{"swagger.json", "swagger.yaml"}

	for _, file := range files {
		file := file

		t.Run(file, func(t *testing.T) {
			t.Parallel()

			data, err := os.ReadFile(file)
			require.NoError(t, err)

			content := string(data)

			// /api/v1 is the pre-v1.0 prefix that must never appear.
			assert.NotContains(t, content, "/api/v1", "legacy prefix leaked in %s", file)

			// /v1/config/ is the current configuration namespace for contexts,
			// sources, rules, field-maps, and fee-rules. Verify it exists.
			assert.Contains(t, content, "/v1/config/contexts", "expected /v1/config/contexts in %s", file)
		})
	}
}

func TestSwaggerSpec_ContainsRenamedBusinessPaths(t *testing.T) {
	t.Parallel()

	files := []string{"swagger.json", "swagger.yaml"}

	for _, file := range files {
		file := file

		t.Run(file, func(t *testing.T) {
			t.Parallel()

			data, err := os.ReadFile(file)
			require.NoError(t, err)

			content := string(data)

			// Configuration context routes under /v1/config/ namespace.
			assert.Contains(t, content, "/v1/config/contexts", "expected /v1/config/contexts in %s", file)
			assert.Contains(t, content, "/v1/config/field-maps", "expected /v1/config/field-maps in %s", file)
			assert.Contains(t, content, "/v1/config/fee-rules", "expected /v1/config/fee-rules in %s", file)

			// Standalone business routes.
			assert.Contains(t, content, "/v1/fee-schedules", "expected /v1/fee-schedules in %s", file)
			assert.Contains(t, content, "/v1/exceptions", "expected /v1/exceptions in %s", file)
			assert.Contains(t, content, "/v1/governance", "expected /v1/governance in %s", file)
		})
	}
}

func TestSwaggerSpec_ContainsSystemplanePaths(t *testing.T) {
	t.Parallel()

	files := []string{"swagger.json", "swagger.yaml"}

	for _, file := range files {
		data, err := os.ReadFile(file)
		require.NoError(t, err)

		content := string(data)

		assert.Contains(t, content, "/v1/system/configs", "expected systemplane config routes in %s", file)
		assert.Contains(t, content, "/v1/system/settings", "expected systemplane settings routes in %s", file)
	}
}
