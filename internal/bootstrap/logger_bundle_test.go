// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package bootstrap

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestBuildLoggerBundle_Success asserts that a valid env + level produces a
// non-nil bundle with a non-nil underlying Logger.
func TestBuildLoggerBundle_Success(t *testing.T) {
	t.Parallel()

	bundle, err := buildLoggerBundle("development", "info")

	require.NoError(t, err)
	require.NotNil(t, bundle)
	assert.NotNil(t, bundle.Logger, "bundle.Logger must be wired")
}

// TestBuildLoggerBundle_ProductionEnv asserts the production environment path
// is exercised without error.
func TestBuildLoggerBundle_ProductionEnv(t *testing.T) {
	t.Parallel()

	bundle, err := buildLoggerBundle("production", "warn")

	require.NoError(t, err)
	require.NotNil(t, bundle)
	assert.NotNil(t, bundle.Logger)
}

// TestBuildLoggerBundle_InvalidLevelFallsBack asserts that an invalid level
// string does NOT surface as an error — ResolveLoggerLevel normalises it to
// "info" before handing it to libZap.New. This documents the "forgiving input"
// contract: callers can pass user-supplied env vars without pre-validation.
func TestBuildLoggerBundle_InvalidLevelFallsBack(t *testing.T) {
	t.Parallel()

	bundle, err := buildLoggerBundle("development", "definitely-not-a-level")

	require.NoError(t, err, "invalid level must fall back silently, not error")
	require.NotNil(t, bundle)
	assert.NotNil(t, bundle.Logger)
}

// TestBuildLoggerBundle_EmptyEnvAndLevel asserts that the zero-value path
// (empty strings) resolves to safe defaults rather than erroring.
func TestBuildLoggerBundle_EmptyEnvAndLevel(t *testing.T) {
	t.Parallel()

	bundle, err := buildLoggerBundle("", "")

	require.NoError(t, err)
	require.NotNil(t, bundle)
	assert.NotNil(t, bundle.Logger)
}

// TestBuildLoggerFromConfig_Success asserts the common wrapper returns a
// non-nil libLog.Logger for a well-formed Config.
func TestBuildLoggerFromConfig_Success(t *testing.T) {
	t.Parallel()

	cfg := &Config{
		App: AppConfig{
			EnvName:  "development",
			LogLevel: "debug",
		},
	}

	logger, err := buildLoggerFromConfig(cfg)

	require.NoError(t, err)
	assert.NotNil(t, logger)
}

// TestBuildLoggerFromConfig_NilConfig asserts nil config produces the
// sentinel ErrConfigNil so callers can detect misuse deterministically.
func TestBuildLoggerFromConfig_NilConfig(t *testing.T) {
	t.Parallel()

	logger, err := buildLoggerFromConfig(nil)

	require.Error(t, err)
	assert.Nil(t, logger)
	assert.True(t, errors.Is(err, ErrConfigNil),
		"expected ErrConfigNil, got: %v", err)
}

// TestBuildLoggerFromConfig_ProductionConfig asserts a production-shaped
// Config wires through buildLoggerBundle's production branch without error.
func TestBuildLoggerFromConfig_ProductionConfig(t *testing.T) {
	t.Parallel()

	cfg := &Config{
		App: AppConfig{
			EnvName:  "production",
			LogLevel: "error",
		},
	}

	logger, err := buildLoggerFromConfig(cfg)

	require.NoError(t, err)
	assert.NotNil(t, logger)
}
