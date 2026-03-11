//go:build unit

package chanutil

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestClosedSignalChannel_OpenChannel(t *testing.T) {
	t.Parallel()

	ch := make(chan struct{})
	assert.False(t, ClosedSignalChannel(ch), "open channel should not be reported as closed")
}

func TestClosedSignalChannel_ClosedChannel(t *testing.T) {
	t.Parallel()

	ch := make(chan struct{})
	close(ch)

	assert.True(t, ClosedSignalChannel(ch), "closed channel should be reported as closed")
}

func TestClosedSignalChannel_NilChannel(t *testing.T) {
	t.Parallel()

	assert.True(t, ClosedSignalChannel(nil), "nil channel should be reported as closed")
}

func TestClosedSignalChannel_BufferedReadableChannelDocumentsContract(t *testing.T) {
	t.Parallel()

	ch := make(chan struct{}, 1)
	ch <- struct{}{}

	assert.True(t, ClosedSignalChannel(ch), "readable open channels are invalid input for this close-only helper")
}
