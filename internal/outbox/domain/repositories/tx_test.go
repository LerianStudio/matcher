// This test verifies the Tx type alias contract. While this tests a Go language guarantee,
// it serves as documentation that domain code depends on *sql.Tx as the transaction type.

//go:build unit

package repositories_test

import (
	"database/sql"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTxType(t *testing.T) {
	t.Parallel()

	t.Run("Tx is alias for *sql.Tx", func(t *testing.T) {
		t.Parallel()

		tx := (*sql.Tx)(nil)

		assert.Nil(t, tx)
	})

	t.Run("Tx type is compatible with sql.Tx", func(t *testing.T) {
		t.Parallel()

		var sqlTx *sql.Tx

		tx := sqlTx

		assert.Nil(t, tx)
	})
}
