// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package bootstrap

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestShouldEnableConfigAPIRoutes(t *testing.T) {
	t.Parallel()

	t.Run("nil config", func(t *testing.T) {
		t.Parallel()
		assert.False(t, shouldEnableConfigAPIRoutes(nil))
	})

	t.Run("auth disabled", func(t *testing.T) {
		t.Parallel()
		cfg := defaultConfig()
		cfg.Auth.Enabled = false
		assert.False(t, shouldEnableConfigAPIRoutes(cfg))
	})

	t.Run("auth enabled", func(t *testing.T) {
		t.Parallel()
		cfg := defaultConfig()
		cfg.Auth.Enabled = true
		assert.True(t, shouldEnableConfigAPIRoutes(cfg))
	})
}
