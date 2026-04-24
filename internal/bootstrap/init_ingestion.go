// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

// interface-only:skip-check-tests
// Behaviour of initIngestionModule is exercised end-to-end by the
// Service-level tests in init_test.go. This file has no dedicated
// companion _test.go — scripts/check-tests.sh honours the marker above.

package bootstrap

import (
	"context"
	"fmt"
	"time"

	ingestionHTTP "github.com/LerianStudio/matcher/internal/ingestion/adapters/http"
	ingestionParser "github.com/LerianStudio/matcher/internal/ingestion/adapters/parsers"
	ingestionRedis "github.com/LerianStudio/matcher/internal/ingestion/adapters/redis"
	ingestionCommand "github.com/LerianStudio/matcher/internal/ingestion/services/command"
	ingestionQuery "github.com/LerianStudio/matcher/internal/ingestion/services/query"
	matchingCommand "github.com/LerianStudio/matcher/internal/matching/services/command"
	crossAdapters "github.com/LerianStudio/matcher/internal/shared/adapters/cross"
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

func initIngestionModule(
	cfg *Config,
	configGetter func() *Config,
	settingsResolver *runtimeSettingsResolver,
	routes *Routes,
	provider sharedPorts.InfrastructureProvider,
	outboxRepository sharedPorts.OutboxRepository,
	publisher sharedPorts.IngestionEventPublisher,
	matchingUseCase *matchingCommand.UseCase,
	repos *sharedRepositories,
	production bool,
) (*ingestionCommand.UseCase, error) {
	ingestionRegistry := ingestionParser.NewParserRegistry()
	ingestionRegistry.Register(ingestionParser.NewCSVParser())
	ingestionRegistry.Register(ingestionParser.NewJSONParser())
	ingestionRegistry.Register(ingestionParser.NewXMLParser())

	dedupeService := ingestionRedis.NewDedupeService(provider)

	fieldMapAdapter, err := crossAdapters.NewFieldMapRepositoryAdapter(repos.configFieldMap)
	if err != nil {
		return nil, fmt.Errorf("create field map repository adapter: %w", err)
	}

	sourceAdapter, err := crossAdapters.NewSourceRepositoryAdapter(repos.configSource)
	if err != nil {
		return nil, fmt.Errorf("create source repository adapter: %w", err)
	}

	contextAdapter := crossAdapters.NewContextAccessProviderAdapter(repos.configContext)

	// Auto-match on upload: create adapters to check context config and trigger matching
	autoMatchContextProvider, err := crossAdapters.NewAutoMatchContextProviderAdapter(repos.configContext)
	if err != nil {
		return nil, fmt.Errorf("create auto-match context provider adapter: %w", err)
	}

	// T-004 (K-06a): matchingUseCase satisfies sharedPorts.MatchTrigger
	// directly (TriggerMatchForContext lives on the UseCase). No adapter.
	// A typed-nil interface is also valid: ingestion's nil-check uses
	// sharedPorts.IsNilValue which handles both interface-nil and typed-nil.
	var matchTrigger sharedPorts.MatchTrigger
	if matchingUseCase != nil {
		matchTrigger = matchingUseCase
	}

	ingestionCommandUseCase, err := ingestionCommand.NewUseCase(ingestionCommand.UseCaseDeps{
		JobRepo:         repos.ingestionJob,
		TransactionRepo: repos.ingestionTx,
		Dedupe:          dedupeService,
		DedupeTTL:       cfg.DedupeTTL(),
		DedupeTTLResolver: func(ctx context.Context) time.Duration {
			return resolveDedupeTTL(ctx, cfg, configGetter, settingsResolver)
		},
		DedupeTTLGetter: func() time.Duration {
			runtimeCfg := configGetter()
			if runtimeCfg == nil {
				return cfg.DedupeTTL()
			}

			return runtimeCfg.DedupeTTL()
		},
		Publisher:       publisher,
		OutboxRepo:      outboxRepository,
		Parsers:         ingestionRegistry,
		FieldMapRepo:    fieldMapAdapter,
		SourceRepo:      sourceAdapter,
		MatchTrigger:    matchTrigger,
		ContextProvider: autoMatchContextProvider,
	})
	if err != nil {
		return nil, fmt.Errorf("create ingestion command use case: %w", err)
	}

	ingestionQueryUseCase, err := ingestionQuery.NewUseCase(
		repos.ingestionJob,
		repos.ingestionTx,
	)
	if err != nil {
		return nil, fmt.Errorf("create ingestion query use case: %w", err)
	}

	ingestionHandler, err := ingestionHTTP.NewHandlers(
		ingestionCommandUseCase,
		ingestionQueryUseCase,
		repos.ingestionJob,
		repos.ingestionTx,
		contextAdapter,
		production,
	)
	if err != nil {
		return nil, fmt.Errorf("create ingestion handler: %w", err)
	}

	if err := ingestionHTTP.RegisterRoutes(routes.Protected, ingestionHandler); err != nil {
		return nil, fmt.Errorf("register ingestion routes: %w", err)
	}

	return ingestionCommandUseCase, nil
}
