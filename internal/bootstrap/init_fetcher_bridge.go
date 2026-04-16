// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package bootstrap

import (
	"context"
	"errors"
	"fmt"

	libLog "github.com/LerianStudio/lib-commons/v4/commons/log"

	discoveryExtractionRepo "github.com/LerianStudio/matcher/internal/discovery/adapters/postgres/extraction"
	ingestionCommand "github.com/LerianStudio/matcher/internal/ingestion/services/command"
	crossAdapters "github.com/LerianStudio/matcher/internal/shared/adapters/cross"
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

// errFetcherBridgeMissingLogger indicates the bridge was wired without a
// logger dependency. It is a package-private sentinel because the function
// is only called from bootstrap where logger nil-ness reflects a bootstrap
// wiring bug, not a runtime condition callers should distinguish.
var errFetcherBridgeMissingLogger = errors.New(
	"fetcher bridge requires a logger",
)

// FetcherBridgeAdapters bundles the two cross adapters that form the
// Fetcher-to-ingestion trusted bridge. They live behind shared-kernel ports
// so discovery-side callers (in a later task) can depend on them without
// importing the ingestion or discovery adapter implementations directly.
type FetcherBridgeAdapters struct {
	Intake    sharedPorts.FetcherBridgeIntake
	LinkWrite sharedPorts.ExtractionLifecycleLinkWriter
}

// initFetcherBridgeAdapters constructs the two cross adapters that form the
// Fetcher trusted-stream bridge. T-001 only proves they are reachable; the
// T-003 worker task will take these adapters and drive them from discovery.
// The function returns nil (and logs a warning) when any prerequisite is
// missing so bootstrap stays tolerant of fetcher-disabled deployments.
func initFetcherBridgeAdapters(
	ctx context.Context,
	ingestionUseCase *ingestionCommand.UseCase,
	extractionRepo *discoveryExtractionRepo.Repository,
	logger libLog.Logger,
) (*FetcherBridgeAdapters, error) {
	if logger == nil {
		return nil, fmt.Errorf("init fetcher bridge adapters: %w", errFetcherBridgeMissingLogger)
	}

	if ingestionUseCase == nil {
		logger.Log(
			ctx,
			libLog.LevelWarn,
			"fetcher bridge not wired: ingestion command use case unavailable",
		)

		return nil, nil
	}

	if extractionRepo == nil {
		logger.Log(
			ctx,
			libLog.LevelWarn,
			"fetcher bridge not wired: discovery extraction repository unavailable",
		)

		return nil, nil
	}

	intake, err := crossAdapters.NewFetcherBridgeIntakeAdapter(ingestionUseCase)
	if err != nil {
		return nil, fmt.Errorf("create fetcher bridge intake adapter: %w", err)
	}

	linkWriter, err := crossAdapters.NewExtractionLifecycleLinkWriterAdapter(extractionRepo)
	if err != nil {
		return nil, fmt.Errorf("create extraction lifecycle link writer adapter: %w", err)
	}

	logger.Log(
		ctx,
		libLog.LevelInfo,
		"fetcher bridge adapters wired (intake + lifecycle link writer)",
	)

	return &FetcherBridgeAdapters{
		Intake:    intake,
		LinkWrite: linkWriter,
	}, nil
}
