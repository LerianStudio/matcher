// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package bootstrap

// Startup self-probe wiring used by InitServersWithOptions. Extracted from
// init.go to keep that file's size bounded — the sibling test file
// init_startup_probe_test.go exercises this code directly.

import (
	"context"
	"fmt"

	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"
)

// startupSelfProbeRunner matches RunSelfProbe's signature. Abstracted so tests
// can inject a deterministic runner without depending on real infra.
type startupSelfProbeRunner func(context.Context, *HealthDependencies, libLog.Logger) error

// runStartupSelfProbe drives the startup self-probe. A probe failure is logged
// but does NOT abort startup — K8s livenessProbe restarts the pod via /health
// 503 if the condition persists.
func runStartupSelfProbe(
	ctx context.Context,
	healthDeps *HealthDependencies,
	logger libLog.Logger,
	runner startupSelfProbeRunner,
) {
	if runner == nil {
		return
	}

	if logger == nil {
		logger = &libLog.NopLogger{}
	}

	if probeErr := runner(ctx, healthDeps, logger); probeErr != nil {
		logger.Log(ctx, libLog.LevelError,
			fmt.Sprintf("startup self-probe failed (service will return /health 503 until deps recover): %v", probeErr))
	}
}
