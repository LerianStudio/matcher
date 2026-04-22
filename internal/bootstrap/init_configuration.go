// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package bootstrap

import (
	"fmt"

	configAudit "github.com/LerianStudio/matcher/internal/configuration/adapters/audit"
	configHTTP "github.com/LerianStudio/matcher/internal/configuration/adapters/http"
	configScheduleRepo "github.com/LerianStudio/matcher/internal/configuration/adapters/postgres/schedule"
	configCommand "github.com/LerianStudio/matcher/internal/configuration/services/command"
	configQuery "github.com/LerianStudio/matcher/internal/configuration/services/query"
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

func initConfigurationModule(
	routes *Routes,
	provider sharedPorts.InfrastructureProvider,
	outboxRepository sharedPorts.OutboxRepository,
	repos *sharedRepositories,
	production bool,
) error {
	// Create outbox-based audit publisher for configuration module
	// This decouples configuration from governance via the outbox pattern
	auditPublisher, err := configAudit.NewOutboxPublisher(outboxRepository)
	if err != nil {
		return fmt.Errorf("create config audit publisher: %w", err)
	}

	scheduleRepository := configScheduleRepo.NewRepository(provider)

	configCommandUseCase, err := configCommand.NewUseCase(
		repos.configContext,
		repos.configSource,
		repos.configFieldMap,
		repos.configMatchRule,
		configCommand.WithAuditPublisher(auditPublisher),
		configCommand.WithFeeScheduleRepository(repos.feeSchedule),
		configCommand.WithFeeRuleRepository(repos.configFeeRule),
		configCommand.WithScheduleRepository(scheduleRepository),
		configCommand.WithInfrastructureProvider(provider),
	)
	if err != nil {
		return fmt.Errorf("create config command use case: %w", err)
	}

	configQueryUseCase, err := configQuery.NewUseCase(
		repos.configContext,
		repos.configSource,
		repos.configFieldMap,
		repos.configMatchRule,
		configQuery.WithScheduleRepository(scheduleRepository),
	)
	if err != nil {
		return fmt.Errorf("create config query use case: %w", err)
	}

	configHandler, err := configHTTP.NewHandler(
		configCommandUseCase,
		configQueryUseCase,
		repos.configContext,
		repos.configSource,
		repos.configMatchRule,
		repos.configFieldMap,
		repos.configFeeRule,
		repos.feeSchedule,
		scheduleRepository,
		production,
	)
	if err != nil {
		return fmt.Errorf("create config handler: %w", err)
	}

	if err := configHTTP.RegisterRoutes(routes.Protected, configHandler); err != nil {
		return fmt.Errorf("register configuration routes: %w", err)
	}

	return nil
}
