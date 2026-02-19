//go:build unit

package postgres

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestOutboxRepositoryNewRepository(t *testing.T) {
	t.Parallel()

	repo := NewRepository(nil)
	require.NotNil(t, repo)
}
