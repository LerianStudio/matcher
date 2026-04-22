// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package bootstrap

import (
	"fmt"

	matchingHTTP "github.com/LerianStudio/matcher/internal/matching/adapters/http"
	matchExceptionRepo "github.com/LerianStudio/matcher/internal/matching/adapters/postgres/exception_creator"
	matchFeeVarianceRepo "github.com/LerianStudio/matcher/internal/matching/adapters/postgres/fee_variance"
	matchGroupRepo "github.com/LerianStudio/matcher/internal/matching/adapters/postgres/match_group"
	matchItemRepo "github.com/LerianStudio/matcher/internal/matching/adapters/postgres/match_item"
	matchRunRepo "github.com/LerianStudio/matcher/internal/matching/adapters/postgres/match_run"
	matchLockManager "github.com/LerianStudio/matcher/internal/matching/adapters/redis"
	matchingCommand "github.com/LerianStudio/matcher/internal/matching/services/command"
	matchingQuery "github.com/LerianStudio/matcher/internal/matching/services/query"
	crossAdapters "github.com/LerianStudio/matcher/internal/shared/adapters/cross"
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

func initMatchingModule(
	routes *Routes,
	provider sharedPorts.InfrastructureProvider,
	outboxRepo sharedPorts.OutboxRepository,
	repos *sharedRepositories,
	production bool,
) (*matchingCommand.UseCase, error) {
	configProvider, err := crossAdapters.NewMatchingConfigurationProvider(
		repos.configContext,
		repos.configSource,
		repos.configMatchRule,
		repos.configFeeRule,
	)
	if err != nil {
		return nil, fmt.Errorf("create matching configuration provider: %w", err)
	}

	transactionAdapter, err := crossAdapters.NewTransactionRepositoryAdapterFromRepo(
		provider,
		repos.ingestionTx,
	)
	if err != nil {
		return nil, fmt.Errorf("create transaction adapter for matching: %w", err)
	}

	lockManager := matchLockManager.NewLockManager(provider)
	matchRunRepository := matchRunRepo.NewRepository(provider)
	matchGroupRepository := matchGroupRepo.NewRepository(provider)
	matchItemRepository := matchItemRepo.NewRepository(provider)
	exceptionCreator := matchExceptionRepo.NewRepository(provider)
	feeVarianceRepository := matchFeeVarianceRepo.NewRepository(provider)

	useCase, err := matchingCommand.New(matchingCommand.UseCaseDeps{
		ContextProvider:  configProvider,
		SourceProvider:   configProvider,
		RuleProvider:     configProvider,
		TxRepo:           transactionAdapter,
		LockManager:      lockManager,
		MatchRunRepo:     matchRunRepository,
		MatchGroupRepo:   matchGroupRepository,
		MatchItemRepo:    matchItemRepository,
		ExceptionCreator: exceptionCreator,
		OutboxRepo:       outboxRepo,
		FeeVarianceRepo:  feeVarianceRepository,
		AdjustmentRepo:   repos.adjustment,
		InfraProvider:    provider,
		AuditLogRepo:     repos.governanceAuditLog,
		FeeScheduleRepo:  repos.feeSchedule,
		FeeRuleProvider:  configProvider.FeeRuleProvider(),
	})
	if err != nil {
		return nil, fmt.Errorf("create matching command use case: %w", err)
	}

	matchingQueryUseCase, err := matchingQuery.NewUseCase(matchRunRepository, matchGroupRepository, matchItemRepository)
	if err != nil {
		return nil, fmt.Errorf("create matching query use case: %w", err)
	}

	matchingHandler, err := matchingHTTP.NewHandler(
		useCase,
		matchingQueryUseCase,
		configProvider,
		production,
	)
	if err != nil {
		return nil, fmt.Errorf("create matching handler: %w", err)
	}

	if err := matchingHTTP.RegisterRoutes(routes.Protected, matchingHandler); err != nil {
		return nil, fmt.Errorf("register matching routes: %w", err)
	}

	return useCase, nil
}
