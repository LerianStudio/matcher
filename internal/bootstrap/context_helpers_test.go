// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package bootstrap

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFallbackContext(t *testing.T) {
	t.Parallel()

	existing := context.TODO()
	assert.Equal(t, existing, fallbackContext(existing))
	assert.NotNil(t, fallbackContext(nil))
}
