// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

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
