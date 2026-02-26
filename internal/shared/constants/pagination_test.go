//go:build unit

package constants

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPaginationConstants(t *testing.T) {
	t.Parallel()

	assert.Equal(t, 20, DefaultPaginationLimit)
	assert.Equal(t, 200, MaximumPaginationLimit)
	assert.Less(t, DefaultPaginationLimit, MaximumPaginationLimit)
}
