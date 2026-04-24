// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package constants

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPaginationConstants(t *testing.T) {
	t.Parallel()

	assert.Equal(t, 20, DefaultPaginationLimit)
	assert.Equal(t, 200, MaximumPaginationLimit)
	assert.Less(t, DefaultPaginationLimit, MaximumPaginationLimit)
}
