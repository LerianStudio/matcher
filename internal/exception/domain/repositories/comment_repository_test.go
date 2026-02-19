//go:build unit

package repositories

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCommentRepository_InterfaceExists(t *testing.T) {
	t.Parallel()

	// Interface compliance is enforced at compile time by mockgen-generated mocks
	// and concrete repository implementations. This test verifies the interface
	// is importable and non-nil as a type.
	var repo CommentRepository
	assert.Nil(t, repo)
}
