//go:build unit

// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package bootstrap

import (
	"context"
	"testing"

	"github.com/gofiber/fiber/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"
)

// --- NewServer ---

func TestNewServer_AllFields(t *testing.T) {
	t.Parallel()

	cfg := &Config{}
	app := fiber.New()
	logger := &libLog.NopLogger{}

	srv := NewServer(cfg, app, logger, nil, nil, nil, nil)

	require.NotNil(t, srv)
	assert.Equal(t, cfg, srv.cfg)
	assert.Equal(t, app, srv.app)
	assert.Equal(t, logger, srv.logger)
	assert.Nil(t, srv.telemetry)
	assert.Nil(t, srv.postgres)
	assert.Nil(t, srv.redis)
	assert.Nil(t, srv.rabbitmq)
}

func TestNewServer_NilFields(t *testing.T) {
	t.Parallel()

	srv := NewServer(nil, nil, nil, nil, nil, nil, nil)

	require.NotNil(t, srv)
	assert.Nil(t, srv.cfg)
	assert.Nil(t, srv.app)
	assert.Nil(t, srv.logger)
}

// --- GetApp ---

func TestServer_GetApp_ReturnsApp(t *testing.T) {
	t.Parallel()

	app := fiber.New()
	srv := &Server{app: app}

	result := srv.GetApp()

	assert.Equal(t, app, result)
}

func TestServer_GetApp_NilServer(t *testing.T) {
	t.Parallel()

	var srv *Server

	result := srv.GetApp()

	assert.Nil(t, result)
}

// --- Run ---

func TestServer_Run_NilServer(t *testing.T) {
	t.Parallel()

	var srv *Server

	err := srv.Run(nil)

	require.Error(t, err)
	assert.ErrorIs(t, err, errServerNotInitialized)
}

func TestServer_Run_NilConfig(t *testing.T) {
	t.Parallel()

	srv := &Server{app: fiber.New()}

	err := srv.Run(nil)

	require.Error(t, err)
	assert.ErrorIs(t, err, errConfigNotInitialized)
}

func TestServer_Run_NilApp(t *testing.T) {
	t.Parallel()

	srv := &Server{
		cfg:    &Config{},
		logger: &libLog.NopLogger{},
	}

	err := srv.Run(nil)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "server run")
}

// --- Shutdown ---

func TestServer_Shutdown_Success(t *testing.T) {
	t.Parallel()

	app := fiber.New()
	srv := &Server{
		app:    app,
		logger: &libLog.NopLogger{},
	}

	err := srv.Shutdown(context.Background())

	require.NoError(t, err)
}

func TestServer_Shutdown_NilServer(t *testing.T) {
	t.Parallel()

	var srv *Server

	err := srv.Shutdown(context.Background())

	require.Error(t, err)
	assert.Contains(t, err.Error(), "server shutdown")
}

func TestServer_Shutdown_NilApp(t *testing.T) {
	t.Parallel()

	srv := &Server{
		logger: &libLog.NopLogger{},
	}

	err := srv.Shutdown(context.Background())

	require.Error(t, err)
	assert.Contains(t, err.Error(), "server shutdown")
}

func TestServer_Shutdown_NilLogger(t *testing.T) {
	t.Parallel()

	app := fiber.New()
	srv := &Server{app: app}

	// Must not panic with nil logger — the function creates a NopLogger fallback.
	err := srv.Shutdown(context.Background())

	require.NoError(t, err)
}
