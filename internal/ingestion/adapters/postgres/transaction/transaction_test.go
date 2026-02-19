//go:build unit

// Package transaction provides unit tests for transaction model conversion functions.
package transaction

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewTransactionPostgreSQLModel_NilEntityReturnsError(t *testing.T) {
	t.Parallel()

	model, err := NewTransactionPostgreSQLModel(nil)
	require.Error(t, err)
	require.Nil(t, model)
}
