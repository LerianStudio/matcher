// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package dispute

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestErrNotFound_Message(t *testing.T) {
	t.Parallel()

	assert.Equal(t, "dispute not found", ErrNotFound.Error())
}

func TestErrNotFound_NotNil(t *testing.T) {
	t.Parallel()

	assert.NotNil(t, ErrNotFound)
}
