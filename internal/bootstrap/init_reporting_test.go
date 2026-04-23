// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package bootstrap

import (
	"testing"

	"github.com/LerianStudio/lib-commons/v5/commons/log"
	"github.com/LerianStudio/lib-commons/v5/commons/net/http/ratelimit"

	reportingWorker "github.com/LerianStudio/matcher/internal/reporting/services/worker"
	"github.com/LerianStudio/matcher/internal/shared/objectstorage"
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

// TestInitReportingModule_Signature locks the signature of initReportingModule
// after the T-018 bootstrap split. Behaviour is exercised by the Service-level
// tests in init_test.go; this file exists so init_reporting.go has a companion
// _test.go as required by scripts/check-tests.sh.
func TestInitReportingModule_Signature(t *testing.T) {
	t.Parallel()

	var _ func(
		*Routes,
		*Config,
		func() *Config,
		*runtimeSettingsResolver,
		sharedPorts.InfrastructureProvider,
		*objectstorage.Client,
		func() *ratelimit.RateLimiter,
		log.Logger,
		*sharedRepositories,
		bool,
	) (*reportingWorker.ExportWorker, *reportingWorker.CleanupWorker, error) = initReportingModule
}
