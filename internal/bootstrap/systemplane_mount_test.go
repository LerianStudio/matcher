// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package bootstrap

import (
	"errors"
	"testing"

	"github.com/gofiber/fiber/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestMountSystemplaneAPI_NilAppReturnsError asserts that passing a nil fiber
// app is surfaced as an explicit error — the caller's bug should fail loudly,
// not be silently ignored.
func TestMountSystemplaneAPI_NilAppReturnsError(t *testing.T) {
	t.Parallel()

	err := MountSystemplaneAPI(nil, nil, nil, nil, nil)

	require.Error(t, err)
	assert.True(t, errors.Is(err, errMountSystemplaneAppRequired),
		"expected errMountSystemplaneAppRequired, got: %v", err)
}

// TestMountSystemplaneAPI_NilClientNoOp asserts that a nil systemplane.Client
// is a graceful no-op. This is the documented behavior for the case where
// systemplane initialization failed or was disabled — the bootstrap should
// continue without the admin API rather than refuse to start.
func TestMountSystemplaneAPI_NilClientNoOp(t *testing.T) {
	t.Parallel()

	app := fiber.New()
	defer func() { _ = app.Shutdown() }()

	err := MountSystemplaneAPI(app, nil, nil, nil, nil)

	require.NoError(t, err)

	// No /system routes should be registered when the client is nil.
	for _, r := range app.GetRoutes() {
		assert.NotContains(t, r.Path, "/system",
			"nil client must not register /system routes")
	}
}

// TestMountSystemplaneAPI_NilAppWithClient asserts the nil-app guard runs
// before the nil-client check so the error remains attributable to the app
// argument (defensive ordering).
//
// Full integration coverage — route registration with a live Client —
// requires a systemplane.Client backed by a real store (postgres or mongo)
// or a systemplane.NewForTesting store. That coverage lives in integration
// tests; this unit test only asserts the argument-validation fast path.
func TestMountSystemplaneAPI_NilAppWithClient(t *testing.T) {
	t.Parallel()

	err := MountSystemplaneAPI(nil, nil, nil, nil, nil)

	require.Error(t, err)
	assert.True(t, errors.Is(err, errMountSystemplaneAppRequired))
}
