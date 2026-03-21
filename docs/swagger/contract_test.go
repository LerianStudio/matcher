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
		data, err := os.ReadFile(file)
		require.NoError(t, err)

		content := string(data)
		assert.NotContains(t, content, "/api/v1", "legacy prefix leaked in %s", file)
		assert.NotContains(t, content, "/v1/config", "legacy config prefix leaked in %s", file)
		assert.Contains(t, content, "/v1/contexts", "expected v1 configuration routes in %s", file)
	}
}

func TestSwaggerSpec_ContainsRenamedBusinessPaths(t *testing.T) {
	t.Parallel()

	files := []string{"swagger.json", "swagger.yaml"}

	for _, file := range files {
		data, err := os.ReadFile(file)
		require.NoError(t, err)

		content := string(data)

		assert.Contains(t, content, "/v1/contexts", "expected /v1/contexts in %s", file)
		assert.Contains(t, content, "/v1/fee-schedules", "expected /v1/fee-schedules in %s", file)
		assert.Contains(t, content, "/v1/field-maps", "expected /v1/field-maps in %s", file)
		assert.Contains(t, content, "/v1/exceptions", "expected /v1/exceptions in %s", file)
		assert.Contains(t, content, "/v1/governance", "expected /v1/governance in %s", file)
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
