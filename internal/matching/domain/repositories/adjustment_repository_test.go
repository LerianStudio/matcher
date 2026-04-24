// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package repositories

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAdjustmentRepository_InterfaceDefined(t *testing.T) {
	t.Parallel()

	// Verify the interface is defined and can be referenced as a type.
	// The compile-time satisfaction check lives in the adapter package.
	var repo AdjustmentRepository
	assert.Nil(t, repo)
}
