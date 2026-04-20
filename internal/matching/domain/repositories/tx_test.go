//go:build unit

package repositories

import (
	"database/sql"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTxType(t *testing.T) {
	t.Parallel()

	t.Run("Tx is alias for *sql.Tx", func(t *testing.T) {
		t.Parallel()

		transaction := new(sql.Tx)
		assert.NotNil(t, transaction)
		var tx Tx = transaction
		assert.Same(t, transaction, tx)
	})

	t.Run("Tx can be nil", func(t *testing.T) {
		t.Parallel()

		var nilValue Tx

		assert.Nil(t, nilValue)
	})
}
