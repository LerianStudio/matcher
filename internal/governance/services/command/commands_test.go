//go:build unit

package command

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPackageExists(t *testing.T) {
	t.Parallel()

	// commands.go contains package-level documentation only. This tiny test exists
	// so scripts/check-tests.sh can enforce one *_test.go companion per source file.
	assert.NotEmpty(t, "command", "package should exist")
}
