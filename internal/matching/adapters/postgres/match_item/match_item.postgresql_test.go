//go:build unit

package match_item

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewRepository_NilProvider_ReturnsNonNil(t *testing.T) {
	t.Parallel()

	repo := NewRepository(nil)
	require.NotNil(t, repo)
}
