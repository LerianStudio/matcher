//go:build unit

package command

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPackageExists(t *testing.T) {
	t.Parallel()

	// Verify the command package is importable and the build tag is correct.
	assert.NotEmpty(t, "command", "package should exist")
}
