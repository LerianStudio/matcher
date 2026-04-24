// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

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
