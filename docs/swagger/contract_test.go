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
