// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package bootstrap

import (
	"context"
	"fmt"
	"time"

	"github.com/gofiber/fiber/v2"

	exceptionAdapters "github.com/LerianStudio/matcher/internal/exception/adapters"
	exceptionAudit "github.com/LerianStudio/matcher/internal/exception/adapters/audit"
	exceptionHTTP "github.com/LerianStudio/matcher/internal/exception/adapters/http"
	exceptionConnectors "github.com/LerianStudio/matcher/internal/exception/adapters/http/connectors"
	exceptionCommentRepo "github.com/LerianStudio/matcher/internal/exception/adapters/postgres/comment"
	exceptionDisputeRepo "github.com/LerianStudio/matcher/internal/exception/adapters/postgres/dispute"
	exceptionExceptionRepo "github.com/LerianStudio/matcher/internal/exception/adapters/postgres/exception"
	exceptionRedis "github.com/LerianStudio/matcher/internal/exception/adapters/redis"
	exceptionResolution "github.com/LerianStudio/matcher/internal/exception/adapters/resolution"
	exceptionCommand "github.com/LerianStudio/matcher/internal/exception/services/command"
	exceptionQuery "github.com/LerianStudio/matcher/internal/exception/services/query"
	crossAdapters "github.com/LerianStudio/matcher/internal/shared/adapters/cross"
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

func initExceptionModule(
	ctx context.Context,
	cfg *Config,
	configGetter func() *Config,
	settingsResolver *runtimeSettingsResolver,
	routes *Routes,
	provider sharedPorts.InfrastructureProvider,
	outboxRepository sharedPorts.OutboxRepository,
	dispatchLimiter fiber.Handler,
	repos *sharedRepositories,
	production bool,
) error {
	// Exception-specific repositories (not shared across modules)
	exceptionRepository := exceptionExceptionRepo.NewRepository(provider)
	disputeRepository := exceptionDisputeRepo.NewRepository(provider)
	commentRepository := exceptionCommentRepo.NewRepository(provider)

	deps, err := initExceptionDependencies(outboxRepository, exceptionRepository, repos)
	if err != nil {
		return err
	}

	useCases, err := initExceptionUseCases(
		ctx,
		cfg,
		configGetter,
		settingsResolver,
		provider,
		exceptionRepository,
		disputeRepository,
		commentRepository,
		deps,
		repos,
	)
	if err != nil {
		return err
	}

	// HTTP Handlers
	exceptionHandlers, err := exceptionHTTP.NewHandlers(
		useCases.command,
		useCases.query,
		commentRepository,
		exceptionRepository,
		disputeRepository,
		production,
	)
	if err != nil {
		return fmt.Errorf("create exception handlers: %w", err)
	}

	if err := exceptionHTTP.RegisterRoutes(routes.Protected, exceptionHandlers, dispatchLimiter); err != nil {
		return fmt.Errorf("register exception routes: %w", err)
	}

	return nil
}

// exceptionModuleDeps holds cross-cutting adapters used by exception use cases.
type exceptionModuleDeps struct {
	auditPublisher     *exceptionAudit.OutboxPublisher
	actorExtractor     *exceptionAdapters.AuthActorExtractor
	resolutionExecutor *exceptionResolution.Executor
}

// exceptionUseCases holds the merged command use case plus the read-only
// query use case for the exception bounded context. The five previously
// separate write-side use cases (resolution, disputes, dispatch, comments,
// callbacks) are now fused into a single ExceptionUseCase with optional
// dependencies wired via UseCaseOption. Comment reads are served directly
// from the comment repository at the handler layer.
type exceptionUseCases struct {
	command *exceptionCommand.ExceptionUseCase
	query   *exceptionQuery.UseCase
}

// initExceptionDependencies creates the cross-cutting adapters for the exception module:
// audit publisher, actor extractor, merged exception-matching bridge, and resolution executor.
func initExceptionDependencies(
	outboxRepository sharedPorts.OutboxRepository,
	exceptionRepository *exceptionExceptionRepo.Repository,
	repos *sharedRepositories,
) (*exceptionModuleDeps, error) {
	auditPublisher, err := exceptionAudit.NewOutboxPublisher(outboxRepository)
	if err != nil {
		return nil, fmt.Errorf("create audit publisher: %w", err)
	}

	actorExtractor := exceptionAdapters.NewAuthActorExtractor()

	matchingGateway, err := crossAdapters.NewExceptionMatchingGateway(
		repos.adjustment,
		repos.ingestionTx,
		repos.ingestionJob,
		repos.configSource,
	)
	if err != nil {
		return nil, fmt.Errorf("create matching gateway: %w", err)
	}

	resolutionExecutor, err := exceptionResolution.NewExecutor(
		exceptionRepository,
		matchingGateway,
		actorExtractor,
	)
	if err != nil {
		return nil, fmt.Errorf("create resolution executor: %w", err)
	}

	return &exceptionModuleDeps{
		auditPublisher:     auditPublisher,
		actorExtractor:     actorExtractor,
		resolutionExecutor: resolutionExecutor,
	}, nil
}

// initExceptionUseCases creates the merged exception command use case plus
// the read-only query use cases. The merged command use case hosts every
// write operation on the exception bounded context (resolution, disputes,
// dispatch, comments, callbacks); optional dependencies are wired via
// UseCaseOption.
func initExceptionUseCases(
	ctx context.Context,
	cfg *Config,
	configGetter func() *Config,
	settingsResolver *runtimeSettingsResolver,
	provider sharedPorts.InfrastructureProvider,
	exceptionRepository *exceptionExceptionRepo.Repository,
	disputeRepository *exceptionDisputeRepo.Repository,
	commentRepository *exceptionCommentRepo.Repository,
	deps *exceptionModuleDeps,
	repos *sharedRepositories,
) (*exceptionUseCases, error) {
	queryUseCase, err := exceptionQuery.NewUseCase(
		exceptionRepository,
		disputeRepository,
		repos.governanceAuditLog,
		&tenantExtractorAdapter{},
	)
	if err != nil {
		return nil, fmt.Errorf("create exception query use case: %w", err)
	}

	httpConnector, err := newExceptionHTTPConnector(ctx, cfg, configGetter, settingsResolver)
	if err != nil {
		return nil, err
	}

	callbackRateLimiter, callbackIdempotencyRepo, err := newCallbackInfra(cfg, configGetter, settingsResolver, provider)
	if err != nil {
		return nil, err
	}

	commandUseCase, err := exceptionCommand.NewExceptionUseCase(
		exceptionRepository,
		deps.actorExtractor,
		deps.auditPublisher,
		provider,
		exceptionCommand.WithResolutionExecutor(deps.resolutionExecutor),
		exceptionCommand.WithDisputeRepository(disputeRepository),
		exceptionCommand.WithCommentRepository(commentRepository),
		exceptionCommand.WithExternalConnector(httpConnector),
		exceptionCommand.WithIdempotencyRepository(callbackIdempotencyRepo),
		exceptionCommand.WithCallbackRateLimiter(callbackRateLimiter),
	)
	if err != nil {
		return nil, fmt.Errorf("create exception use case: %w", err)
	}

	return &exceptionUseCases{
		command: commandUseCase,
		query:   queryUseCase,
	}, nil
}

// newExceptionHTTPConnector builds the HTTP connector used by the dispatch
// path with production-hardened defaults and optional runtime-config
// resolvers for webhook timeout.
//
// SEC-27: RequireSignedPayloads defaults to true in production so an
// unsigned webhook configuration fails validation at startup rather than
// silently dispatching unsigned payloads to downstream systems.
// Development and test environments retain the permissive default (false)
// so local tooling can exercise the dispatch path without a shared secret.
// Operators who deploy webhook dispatch to production without a signing
// secret must explicitly opt out in code; there is no runtime toggle
// because the whole point of the default is that misconfiguration is
// visible from the first run.
func newExceptionHTTPConnector(
	ctx context.Context,
	cfg *Config,
	configGetter func() *Config,
	settingsResolver *runtimeSettingsResolver,
) (*exceptionConnectors.HTTPConnector, error) {
	webhookDispatchTimeout := configuredWebhookTimeout(ctx, cfg)
	isProduction := IsProductionEnvironment(cfg.App.EnvName)

	httpConnector, err := exceptionConnectors.NewHTTPConnector(
		exceptionConnectors.ConnectorConfig{
			Webhook: &exceptionConnectors.WebhookConnectorConfig{
				BaseConnectorConfig: exceptionConnectors.BaseConnectorConfig{
					Timeout: &webhookDispatchTimeout,
				},
				RequireSignedPayloads: isProduction,
			},
		},
	)
	if err != nil {
		return nil, fmt.Errorf("create http connector: %w", err)
	}

	if configGetter != nil || settingsResolver != nil {
		httpConnector.SetWebhookTimeoutResolver(func(ctx context.Context) time.Duration {
			return resolveWebhookTimeout(ctx, cfg, configGetter, settingsResolver)
		})
	}

	return httpConnector, nil
}

// newCallbackInfra builds the rate limiter and idempotency repository used
// by the callback path, with runtime-config resolvers where available.
func newCallbackInfra(
	cfg *Config,
	configGetter func() *Config,
	settingsResolver *runtimeSettingsResolver,
	provider sharedPorts.InfrastructureProvider,
) (*exceptionRedis.CallbackRateLimiter, *exceptionRedis.IdempotencyRepository, error) {
	callbackRateLimiter, err := exceptionRedis.NewCallbackRateLimiter(
		provider,
		cfg.CallbackRateLimitPerMinute(),
		time.Minute,
	)
	if err != nil {
		return nil, nil, fmt.Errorf("create callback rate limiter: %w", err)
	}

	if configGetter != nil || settingsResolver != nil {
		callbackRateLimiter.SetRuntimeLimitResolver(func(ctx context.Context) int {
			return resolveCallbackRateLimit(ctx, cfg, configGetter, settingsResolver)
		})
	}

	callbackIdempotencyRepo, err := exceptionRedis.NewIdempotencyRepositoryWithConfig(
		provider,
		cfg.IdempotencyRetryWindow(),
		cfg.IdempotencySuccessTTL(),
		cfg.Idempotency.HMACSecret,
	)
	if err != nil {
		return nil, nil, fmt.Errorf("create callback idempotency repository: %w", err)
	}

	if configGetter != nil || settingsResolver != nil {
		callbackIdempotencyRepo.SetRuntimeConfigResolvers(
			func(ctx context.Context) time.Duration {
				return resolveIdempotencyRetryWindow(ctx, cfg, configGetter, settingsResolver)
			},
			func(ctx context.Context) time.Duration {
				return resolveIdempotencySuccessTTL(ctx, cfg, configGetter, settingsResolver)
			},
			func(ctx context.Context) string {
				return resolveIdempotencyHMACSecret(ctx, cfg, configGetter, settingsResolver)
			},
		)
	}

	return callbackRateLimiter, callbackIdempotencyRepo, nil
}
