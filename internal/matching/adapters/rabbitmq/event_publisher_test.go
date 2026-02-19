//go:build unit

package rabbitmq

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewEventPublisherFromChannel_NilReturnsError(t *testing.T) {
	t.Parallel()

	_, err := NewEventPublisherFromChannel(nil)
	require.Error(t, err)
}
