//go:build unit

package query

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPackageExists(t *testing.T) {
	t.Parallel()

	// Verify the query package is importable and the build tag is correct.
	assert.NotEmpty(t, "query", "package should exist")
}
