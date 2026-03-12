//go:build unit

package chaos

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDispatchOutboxUntilEmpty_AccumulatesProcessedItems(t *testing.T) {
	t.Parallel()

	sequence := []int{2, 3, 0, 99}
	index := 0
	total := dispatchOutboxUntilEmpty(10, nil, func() int {
		value := sequence[index]
		index++
		return value
	})

	assert.Equal(t, 5, total)
	assert.Equal(t, 3, index)
}
