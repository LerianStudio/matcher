//go:build unit

package chanutil

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestClosed_OpenChannel(t *testing.T) {
	t.Parallel()

	ch := make(chan struct{})
	assert.False(t, Closed(ch), "open channel should not be reported as closed")
}

func TestClosed_ClosedChannel(t *testing.T) {
	t.Parallel()

	ch := make(chan struct{})
	close(ch)

	assert.True(t, Closed(ch), "closed channel should be reported as closed")
}

func TestClosed_NilChannel(t *testing.T) {
	t.Parallel()

	assert.True(t, Closed(nil), "nil channel should be reported as closed")
}
