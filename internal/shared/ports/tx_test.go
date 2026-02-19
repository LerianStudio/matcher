//go:build unit

package ports

import (
	"database/sql"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTxTypeAlias(t *testing.T) {
	t.Parallel()

	var txVal Tx

	assert.Nil(t, txVal)
	assert.IsType(t, (*sql.Tx)(nil), txVal)
}
