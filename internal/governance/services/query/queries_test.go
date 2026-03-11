//go:build unit

package query

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPackageExists(t *testing.T) {
	t.Parallel()

	// queries.go contains package-level documentation only. This tiny test exists
	// so scripts/check-tests.sh can enforce one *_test.go companion per source file.
	assert.NotEmpty(t, "query", "package should exist")
}
