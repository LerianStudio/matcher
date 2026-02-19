//go:build unit

package dispute

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestErrNotFound_Message(t *testing.T) {
	t.Parallel()

	assert.Equal(t, "dispute not found", ErrNotFound.Error())
}

func TestErrNotFound_NotNil(t *testing.T) {
	t.Parallel()

	assert.NotNil(t, ErrNotFound)
}
