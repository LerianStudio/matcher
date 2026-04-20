//go:build unit

package repositories

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAdjustmentRepository_InterfaceDefined(t *testing.T) {
	t.Parallel()

	// Verify the interface is defined and can be referenced as a type.
	// The compile-time satisfaction check lives in the adapter package.
	var repo AdjustmentRepository
	assert.Nil(t, repo)
}
