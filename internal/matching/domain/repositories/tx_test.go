//go:build unit

package repositories

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTxType(t *testing.T) {
	t.Parallel()

	t.Run("Tx is alias for any", func(t *testing.T) {
		t.Parallel()

		var transaction Tx = "test"
		assert.NotNil(t, transaction)
		assert.Equal(t, "test", transaction)
	})

	t.Run("Tx can hold different types", func(t *testing.T) {
		t.Parallel()

		var integerValue Tx = 42

		var stringValue Tx = "transaction"

		var nilValue Tx

		assert.Equal(t, 42, integerValue)
		assert.Equal(t, "transaction", stringValue)
		assert.Nil(t, nilValue)
	})
}
