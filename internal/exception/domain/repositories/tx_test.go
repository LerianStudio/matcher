//go:build unit

package repositories

import (
	"testing"

	"github.com/stretchr/testify/assert"

	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

func TestTxTypeAlias(t *testing.T) {
	t.Parallel()

	var txVal Tx

	assert.Nil(t, txVal)
	assert.IsType(t, sharedPorts.Tx(nil), txVal)
}
