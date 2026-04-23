// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package bootstrap

import (
	"fmt"

	governanceHTTP "github.com/LerianStudio/matcher/internal/governance/adapters/http"
	actorMappingRepoAdapter "github.com/LerianStudio/matcher/internal/governance/adapters/postgres/actor_mapping"
	governanceCommand "github.com/LerianStudio/matcher/internal/governance/services/command"
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

func initGovernanceModule(routes *Routes, repos *sharedRepositories, provider sharedPorts.InfrastructureProvider, production bool) error {
	governanceHandler, err := governanceHTTP.NewHandler(repos.governanceAuditLog, production)
	if err != nil {
		return fmt.Errorf("create governance handler: %w", err)
	}

	if err := governanceHTTP.RegisterRoutes(routes.Protected, governanceHandler); err != nil {
		return fmt.Errorf("register governance routes: %w", err)
	}

	// Actor mapping CRUD
	actorMappingRepo := actorMappingRepoAdapter.NewRepository(provider)

	actorMappingCommandUC, err := governanceCommand.NewActorMappingUseCase(actorMappingRepo)
	if err != nil {
		return fmt.Errorf("create actor mapping command use case: %w", err)
	}

	actorMappingHandler, err := governanceHTTP.NewActorMappingHandler(actorMappingCommandUC, actorMappingRepo, production)
	if err != nil {
		return fmt.Errorf("create actor mapping handler: %w", err)
	}

	if err := governanceHTTP.RegisterActorMappingRoutes(routes.Protected, actorMappingHandler); err != nil {
		return fmt.Errorf("register actor mapping routes: %w", err)
	}

	return nil
}
