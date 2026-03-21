//go:build unit

package swagger

import (
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSwagger_FeeRuleEndpointsExist verifies that the generated Swagger spec
// contains the expected fee-rule API paths. This is a content-level smoke test
// that catches regressions where Swagger annotations are removed or paths change
// without updating the spec.
func TestSwagger_FeeRuleEndpointsExist(t *testing.T) {
	t.Parallel()

	data, err := os.ReadFile("swagger.yaml")
	require.NoError(t, err, "swagger.yaml must be readable from docs/swagger/")

	content := string(data)

	expectedPaths := []struct {
		path        string
		description string
	}{
		{
			path:        "/v1/config/contexts/{contextId}/fee-rules:",
			description: "context-scoped fee-rule collection (POST + GET)",
		},
		{
			path:        "/v1/config/fee-rules/{feeRuleId}:",
			description: "fee-rule resource (GET + PATCH + DELETE)",
		},
	}

	for _, ep := range expectedPaths {
		assert.True(t,
			strings.Contains(content, ep.path),
			"swagger.yaml should contain path %q (%s)", ep.path, ep.description,
		)
	}

	// Verify all five HTTP methods are present for fee-rule endpoints.
	// The context-scoped path should have post + get; the resource path should have get + patch + delete.
	expectedOperations := []struct {
		operation   string
		description string
	}{
		{operation: "operationId: createFeeRule", description: "POST create"},
		{operation: "operationId: listFeeRules", description: "GET list"},
		{operation: "operationId: getFeeRule", description: "GET single"},
		{operation: "operationId: updateFeeRule", description: "PATCH update"},
		{operation: "operationId: deleteFeeRule", description: "DELETE"},
	}

	for _, op := range expectedOperations {
		assert.True(t,
			strings.Contains(content, op.operation),
			"swagger.yaml should contain %s operation (%s)", op.operation, op.description,
		)
	}
}

// TestSwagger_FeeRuleSecurityAnnotations verifies that fee-rule endpoints
// require BearerAuth in the Swagger spec.
func TestSwagger_FeeRuleSecurityAnnotations(t *testing.T) {
	t.Parallel()

	data, err := os.ReadFile("swagger.yaml")
	require.NoError(t, err)

	content := string(data)

	// All fee-rule operations should be behind BearerAuth.
	// We verify this by checking that "BearerAuth" appears in the spec
	// (the handler annotations all include @Security BearerAuth).
	assert.True(t,
		strings.Contains(content, "BearerAuth"),
		"swagger.yaml should reference BearerAuth security scheme",
	)
}
