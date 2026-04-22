// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package bootstrap

import (
	"context"
	"testing"

	"github.com/gofiber/fiber/v2"

	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

// TestInitExceptionModule_Signature locks the signature of initExceptionModule
// after the T-018 bootstrap split. Behaviour is exercised by the Service-level
// tests in init_test.go; this file exists so init_exception.go has a companion
// _test.go as required by scripts/check-tests.sh.
func TestInitExceptionModule_Signature(t *testing.T) {
	t.Parallel()

	var _ func(
		context.Context,
		*Config,
		func() *Config,
		*runtimeSettingsResolver,
		*Routes,
		sharedPorts.InfrastructureProvider,
		sharedPorts.OutboxRepository,
		fiber.Handler,
		*sharedRepositories,
		bool,
	) error = initExceptionModule
}
