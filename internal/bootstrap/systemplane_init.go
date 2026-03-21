// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package bootstrap

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	libLog "github.com/LerianStudio/lib-commons/v4/commons/log"
	"github.com/LerianStudio/lib-commons/v4/commons/runtime"

	spBootstrap "github.com/LerianStudio/matcher/pkg/systemplane/bootstrap"
	"github.com/LerianStudio/matcher/pkg/systemplane/bootstrap/builtin"
	"github.com/LerianStudio/matcher/pkg/systemplane/domain"
	"github.com/LerianStudio/matcher/pkg/systemplane/ports"
	"github.com/LerianStudio/matcher/pkg/systemplane/registry"
	"github.com/LerianStudio/matcher/pkg/systemplane/service"
)

// defaultSystemplaneBackend is the backend used when SYSTEMPLANE_BACKEND is
// not set. Postgres is the default because Matcher already requires a
// PostgreSQL instance for its own data.
const (
	defaultSystemplaneBackend = "postgres"
	changeFeedRetryDelay      = time.Second
)

// wellKnownDevMasterKey is the development-mode default for the systemplane
// secret master key. It is committed in docker-compose.yml for local
// convenience, but MUST be rejected in production environments.
const wellKnownDevMasterKey = "+PnwgNy8bL3HGT1rOXp47PqyGcPywXH/epgmSVwPkL0="

// Sentinel errors for systemplane initialization and bundle extraction.
var (
	errChangeFeedSupervisorRequired  = errors.New("start change feed: supervisor is required")
	errSystemplaneSecretMasterKey    = errors.New("validate systemplane config: SYSTEMPLANE_SECRET_MASTER_KEY is required")
	errSystemplaneDevMasterKeyInProd = errors.New("validate systemplane config: SYSTEMPLANE_SECRET_MASTER_KEY must not use the well-known development default in production")
	errRateLimitRequiredProduction   = errors.New("RATE_LIMIT_ENABLED must remain true in production")
	errFetcherPrivateIPsProduction   = errors.New("FETCHER_ALLOW_PRIVATE_IPS must remain false in production")
	errArchivalEndpointRequired      = errors.New("OBJECT_STORAGE_ENDPOINT is required when ARCHIVAL_WORKER_ENABLED=true")
)

// SystemplaneComponents holds all systemplane components created during
// initialization. Callers use these references to wire the manager into HTTP
// handlers and to shut down the backend when the application stops.
type SystemplaneComponents struct {
	ChangeFeed ports.ChangeFeed
	Supervisor service.Supervisor
	Manager    service.Manager
	Backend    io.Closer // for shutdown
}

// ExtractBootstrapOnlyConfig extracts the bootstrap-only keys from a full
// Config into the reduced BootstrapOnlyConfig that persists across bundle
// rebuilds. Returns an error if cfg is nil.
func ExtractBootstrapOnlyConfig(cfg *Config) (*BootstrapOnlyConfig, error) {
	if cfg == nil {
		return nil, fmt.Errorf("extract bootstrap config: %w", ErrConfigNil)
	}

	return &BootstrapOnlyConfig{
		EnvName:                    cfg.App.EnvName,
		ServerAddress:              cfg.Server.Address,
		TLSCertFile:                cfg.Server.TLSCertFile,
		TLSKeyFile:                 cfg.Server.TLSKeyFile,
		TLSTerminatedUpstream:      cfg.Server.TLSTerminatedUpstream,
		TrustedProxies:             cfg.Server.TrustedProxies,
		AuthEnabled:                cfg.Auth.Enabled,
		AuthHost:                   cfg.Auth.Host,
		AuthTokenSecret:            cfg.Auth.TokenSecret,
		TelemetryEnabled:           cfg.Telemetry.Enabled,
		TelemetryServiceName:       cfg.Telemetry.ServiceName,
		TelemetryLibraryName:       cfg.Telemetry.LibraryName,
		TelemetryServiceVersion:    cfg.Telemetry.ServiceVersion,
		TelemetryDeploymentEnv:     cfg.Telemetry.DeploymentEnv,
		TelemetryCollectorEndpoint: cfg.Telemetry.CollectorEndpoint,
		TelemetryDBMetricsInterval: cfg.Telemetry.DBMetricsIntervalSec,
	}, nil
}

// LoadSystemplaneBackendConfig reads systemplane backend configuration from
// environment variables. Falls back to using the application's primary Postgres
// DSN if no explicit systemplane backend is configured.
func LoadSystemplaneBackendConfig(appCfg *Config) (*spBootstrap.BootstrapConfig, error) {
	backendStr := os.Getenv("SYSTEMPLANE_BACKEND")
	if backendStr == "" {
		backendStr = defaultSystemplaneBackend
	}

	backend, err := domain.ParseBackendKind(backendStr)
	if err != nil {
		return nil, fmt.Errorf("parse systemplane backend: %w", err)
	}

	cfg := &spBootstrap.BootstrapConfig{
		Backend: backend,
		Secrets: &spBootstrap.SecretStoreConfig{MasterKey: loadSystemplaneSecretMasterKey()},
	}

	switch backend {
	case domain.BackendPostgres:
		cfg.Postgres = loadSystemplanePostgresConfig(appCfg)
	case domain.BackendMongoDB:
		cfg.MongoDB = loadSystemplaneMongoConfig()
	}

	cfg.ApplyDefaults()

	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("validate systemplane config: %w", err)
	}

	if cfg.Secrets != nil && cfg.Secrets.MasterKey == "" {
		return nil, errSystemplaneSecretMasterKey
	}

	if cfg.Secrets != nil && appCfg != nil &&
		IsProductionEnvironment(appCfg.App.EnvName) &&
		cfg.Secrets.MasterKey == wellKnownDevMasterKey {
		return nil, errSystemplaneDevMasterKeyInProd
	}

	return cfg, nil
}

func loadSystemplaneSecretMasterKey() string {
	if value := strings.TrimSpace(os.Getenv("SYSTEMPLANE_SECRET_MASTER_KEY")); value != "" {
		return value
	}

	return ""
}

// loadSystemplanePostgresConfig builds a PostgresBootstrapConfig from env vars,
// falling back to the application's primary postgres DSN when
// SYSTEMPLANE_POSTGRES_DSN is not set.
func loadSystemplanePostgresConfig(appCfg *Config) *spBootstrap.PostgresBootstrapConfig {
	dsn := os.Getenv(spBootstrap.EnvPostgresDSN)
	if dsn == "" && appCfg != nil {
		hostPort := net.JoinHostPort(appCfg.Postgres.PrimaryHost, appCfg.Postgres.PrimaryPort)
		query := url.Values{}
		query.Set("sslmode", appCfg.Postgres.PrimarySSLMode)
		dsn = (&url.URL{
			Scheme:   "postgres",
			User:     url.UserPassword(appCfg.Postgres.PrimaryUser, appCfg.Postgres.PrimaryPassword),
			Host:     hostPort,
			Path:     "/" + appCfg.Postgres.PrimaryDB,
			RawQuery: query.Encode(),
		}).String()
	}

	pgCfg := &spBootstrap.PostgresBootstrapConfig{
		DSN: dsn,
	}

	if v := os.Getenv(spBootstrap.EnvPostgresSchema); v != "" {
		pgCfg.Schema = v
	}

	if v := os.Getenv(spBootstrap.EnvPostgresEntriesTable); v != "" {
		pgCfg.EntriesTable = v
	}

	if v := os.Getenv(spBootstrap.EnvPostgresHistoryTable); v != "" {
		pgCfg.HistoryTable = v
	}

	if v := os.Getenv(spBootstrap.EnvPostgresRevisionTable); v != "" {
		pgCfg.RevisionTable = v
	}

	if v := os.Getenv(spBootstrap.EnvPostgresNotifyChannel); v != "" {
		pgCfg.NotifyChannel = v
	}

	return pgCfg
}

// loadSystemplaneMongoConfig builds a MongoBootstrapConfig entirely from env
// vars. There is no fallback to the application config because Matcher does
// not require MongoDB for its own data.
func loadSystemplaneMongoConfig() *spBootstrap.MongoBootstrapConfig {
	mongoCfg := &spBootstrap.MongoBootstrapConfig{}

	if v := os.Getenv(spBootstrap.EnvMongoURI); v != "" {
		mongoCfg.URI = v
	}

	if v := os.Getenv(spBootstrap.EnvMongoDatabase); v != "" {
		mongoCfg.Database = v
	}

	if v := os.Getenv(spBootstrap.EnvMongoEntriesCollection); v != "" {
		mongoCfg.EntriesCollection = v
	}

	if v := os.Getenv(spBootstrap.EnvMongoHistoryCollection); v != "" {
		mongoCfg.HistoryCollection = v
	}

	if v := os.Getenv(spBootstrap.EnvMongoWatchMode); v != "" {
		mongoCfg.WatchMode = v
	}

	if v := os.Getenv(spBootstrap.EnvMongoPollIntervalSec); v != "" {
		if seconds, err := strconv.Atoi(v); err == nil && seconds > 0 {
			mongoCfg.PollInterval = time.Duration(seconds) * time.Second
		}
	}

	return mongoCfg
}

func closeSystemplaneBackend(backend io.Closer) {
	if backend != nil {
		_ = backend.Close()
	}
}

func abortSystemplaneInit(
	ctx context.Context,
	configManager *ConfigManager,
	supervisor service.Supervisor,
	backend io.Closer,
) {
	if configManager != nil {
		configManager.leaveSeedMode()
	}

	if !domain.IsNilValue(supervisor) {
		_ = supervisor.Stop(ctx)
	}

	closeSystemplaneBackend(backend)
}

func configureBackendWithRegistry(cfg *spBootstrap.BootstrapConfig, reg registry.Registry) {
	if cfg.Secrets != nil {
		cfg.Secrets.SecretKeys = systemplaneSecretKeys(reg)
	}

	cfg.ApplyBehaviors = systemplaneApplyBehaviors(reg)
	if cfg.Postgres != nil {
		cfg.Postgres.ApplyBehaviors = cfg.ApplyBehaviors
	}

	if cfg.MongoDB != nil {
		cfg.MongoDB.ApplyBehaviors = cfg.ApplyBehaviors
	}
}

func validateRuntimeCandidateConfig(
	cfg *Config,
	configManager *ConfigManager,
	snap domain.Snapshot,
) error {
	baseCfg := cfg

	if configManager != nil {
		if currentCfg := configManager.Get(); currentCfg != nil {
			baseCfg = currentCfg
		}
	}

	candidateCfg := snapshotToFullConfig(snap, baseCfg)
	if err := candidateCfg.Validate(); err != nil {
		return err
	}

	if !IsProductionEnvironment(candidateCfg.App.EnvName) {
		return nil
	}

	if !candidateCfg.RateLimit.Enabled {
		return errRateLimitRequiredProduction
	}

	if candidateCfg.Fetcher.AllowPrivateIPs {
		return errFetcherPrivateIPsProduction
	}

	if candidateCfg.Archival.Enabled && strings.TrimSpace(candidateCfg.ObjectStorage.Endpoint) == "" {
		return errArchivalEndpointRequired
	}

	return nil
}

func syncConfigManagerFromSnapshot(configManager *ConfigManager, snap domain.Snapshot) error {
	if configManager == nil {
		return nil
	}

	return configManager.UpdateFromSystemplane(snap)
}

func newSystemplaneManager(
	cfg *Config,
	configManager *ConfigManager,
	logger libLog.Logger,
	reg registry.Registry,
	backend *spBootstrap.BackendResources,
	supervisor service.Supervisor,
	builder *service.SnapshotBuilder,
) (service.Manager, error) {
	manager, err := service.NewManager(service.ManagerConfig{
		Registry:   reg,
		Store:      backend.Store,
		History:    backend.History,
		Supervisor: supervisor,
		Builder:    builder,
		StateSync: func(syncCtx context.Context, snap domain.Snapshot) {
			if configManager == nil {
				return
			}

			//nolint:contextcheck // ConfigManager runtime bridge does not expose a context-aware variant.
			if err := syncConfigManagerFromSnapshot(configManager, snap); err != nil && logger != nil {
				logger.Log(syncCtx, libLog.LevelWarn, "systemplane config bridge sync failed",
					libLog.String("error", err.Error()))
			}
		},
		ConfigWriteValidator: func(_ context.Context, snap domain.Snapshot) error {
			//nolint:contextcheck // Config validation API is not context-aware.
			return validateRuntimeCandidateConfig(cfg, configManager, snap)
		},
	})
	if err != nil {
		return nil, fmt.Errorf("create systemplane manager: %w", err)
	}

	return manager, nil
}

func performInitialSystemplaneReload(
	ctx context.Context,
	configManager *ConfigManager,
	supervisor service.Supervisor,
	backend io.Closer,
) error {
	if configManager != nil {
		configManager.enterSeedMode()
	}

	if err := supervisor.Reload(ctx, "initial-bootstrap"); err != nil {
		abortSystemplaneInit(ctx, configManager, supervisor, backend)
		return fmt.Errorf("initial reload: %w", err)
	}

	return nil
}

// InitSystemplane creates and wires all systemplane components. This is
// designed to be called during bootstrap, after env vars are loaded but before
// the Fiber app is constructed. The supervisor performs an initial reload that
// builds the first snapshot and bundle from registry defaults + any persisted
// store overrides.
//
// On any error the function cleans up partially-created resources before
// returning so the caller does not need to track intermediate state.
func InitSystemplane(
	ctx context.Context,
	cfg *Config,
	configManager *ConfigManager,
	workerManager *WorkerManager,
	logger libLog.Logger,
	observer func(service.ReloadEvent),
) (*SystemplaneComponents, error) {
	if cfg == nil {
		return nil, fmt.Errorf("init systemplane: %w", ErrConfigNil)
	}

	bootstrapCfg, err := ExtractBootstrapOnlyConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("init systemplane: %w", err)
	}

	backendCfg, err := LoadSystemplaneBackendConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("init systemplane: %w", err)
	}

	reg := registry.New()
	if err := RegisterMatcherKeys(reg); err != nil {
		return nil, fmt.Errorf("init systemplane: %w", err)
	}

	configureBackendWithRegistry(backendCfg, reg)

	backend, err := builtin.NewBackendFromConfig(ctx, backendCfg)
	if err != nil {
		return nil, fmt.Errorf("init systemplane: create backend: %w", err)
	}

	builder, err := service.NewSnapshotBuilder(reg, backend.Store)
	if err != nil {
		closeSystemplaneBackend(backend.Closer)
		return nil, fmt.Errorf("init systemplane: %w", err)
	}

	factory, err := NewMatcherBundleFactory(bootstrapCfg)
	if err != nil {
		closeSystemplaneBackend(backend.Closer)
		return nil, fmt.Errorf("init systemplane: %w", err)
	}

	if err := seedStoreForInitialReload(ctx, configManager, backend.Store, reg); err != nil {
		closeSystemplaneBackend(backend.Closer)
		return nil, fmt.Errorf("init systemplane: %w", err)
	}

	reconcilers, err := buildReconcilers(workerManager, logger)
	if err != nil {
		closeSystemplaneBackend(backend.Closer)
		return nil, fmt.Errorf("init systemplane: %w", err)
	}

	supervisor, err := service.NewSupervisor(service.SupervisorConfig{
		Builder:     builder,
		Factory:     factory,
		Reconcilers: reconcilers,
		Observer:    composeReloadObservers(reloadObserver(ctx, logger), observer),
	})
	if err != nil {
		closeSystemplaneBackend(backend.Closer)
		return nil, fmt.Errorf("init systemplane: %w", err)
	}

	if err := performInitialSystemplaneReload(ctx, configManager, supervisor, backend.Closer); err != nil {
		return nil, fmt.Errorf("init systemplane: %w", err)
	}

	manager, err := newSystemplaneManager(cfg, configManager, logger, reg, backend, supervisor, builder)
	if err != nil {
		abortSystemplaneInit(ctx, configManager, supervisor, backend.Closer)
		return nil, fmt.Errorf("init systemplane: %w", err)
	}

	return &SystemplaneComponents{
		ChangeFeed: backend.ChangeFeed,
		Supervisor: supervisor,
		Manager:    manager,
		Backend:    backend.Closer,
	}, nil
}

func seedStoreForInitialReload(
	ctx context.Context,
	configManager *ConfigManager,
	store ports.Store,
	reg registry.Registry,
) error {
	if configManager == nil {
		return nil
	}

	seedErr := configManager.SeedStore(ctx, store, reg)
	if seedErr == nil {
		return nil
	}

	// Revision mismatch means data already exists in store (common restart path).
	// Keep going and force seed mode so config bridge reconciler can apply
	// snapshots during reload.
	if errors.Is(seedErr, domain.ErrRevisionMismatch) {
		if refreshErr := configManager.RefreshBootstrapSeedValues(ctx, store, reg); refreshErr != nil {
			return fmt.Errorf("refresh bootstrap seed values: %w", refreshErr)
		}

		configManager.enterSeedMode()

		return nil
	}

	return fmt.Errorf("seed store: %w", seedErr)
}

// reloadObserver returns a callback that logs each reload event with the build
// strategy used. Returns nil if the logger is nil (observer is optional).
func reloadObserver(ctx context.Context, logger libLog.Logger) func(service.ReloadEvent) {
	if logger == nil {
		return nil
	}

	logCtx := detachedContext(ctx)

	return func(event service.ReloadEvent) {
		logger.Log(logCtx, libLog.LevelInfo, "systemplane reload completed",
			libLog.String("strategy", string(event.Strategy)),
			libLog.String("reason", event.Reason))
	}
}

func composeReloadObservers(observers ...func(service.ReloadEvent)) func(service.ReloadEvent) {
	filtered := make([]func(service.ReloadEvent), 0, len(observers))
	for _, observer := range observers {
		if observer != nil {
			filtered = append(filtered, observer)
		}
	}

	if len(filtered) == 0 {
		return nil
	}

	return func(event service.ReloadEvent) {
		for _, observer := range filtered {
			observer(event)
		}
	}
}

func systemplaneSecretKeys(reg registry.Registry) []string {
	defs := reg.List(domain.KindConfig)

	secretKeys := make([]string, 0, len(defs))
	for _, def := range defs {
		if def.Secret {
			secretKeys = append(secretKeys, def.Key)
		}
	}

	return secretKeys
}

func systemplaneApplyBehaviors(reg registry.Registry) map[string]domain.ApplyBehavior {
	defs := reg.List(domain.KindConfig)

	behaviors := make(map[string]domain.ApplyBehavior, len(defs))
	for _, def := range defs {
		behaviors[def.Key] = def.ApplyBehavior
	}

	for _, def := range reg.List(domain.KindSetting) {
		behaviors[def.Key] = def.ApplyBehavior
	}

	return behaviors
}

// buildReconcilers creates the set of BundleReconcilers that the supervisor
// uses on each reload. Execution order is determined by each reconciler's
// Phase() — StateSync runs first, then Validation, then SideEffect.
// Registration order within the same phase is preserved (stable sort).
func buildReconcilers(workerManager *WorkerManager, logger libLog.Logger) ([]ports.BundleReconciler, error) {
	var reconcilers []ports.BundleReconciler

	reconcilers = append(reconcilers, NewHTTPPolicyReconciler())
	reconcilers = append(reconcilers, NewPublisherReconciler(logger))

	if workerManager != nil {
		workerReconciler, err := NewWorkerReconciler(workerManager)
		if err != nil {
			return nil, fmt.Errorf("create worker reconciler: %w", err)
		}

		reconcilers = append(reconcilers, workerReconciler)
	}

	return reconcilers, nil
}

func applyChangeFeedSignal(
	ctx context.Context,
	signal ports.ChangeSignal,
	supervisor service.Supervisor,
	applySignal func(context.Context, ports.ChangeSignal) error,
) error {
	if applySignal != nil {
		return applySignal(ctx, signal)
	}

	if err := supervisor.Reload(ctx, "changefeed"); err != nil {
		return fmt.Errorf("reload supervisor from changefeed: %w", err)
	}

	return nil
}

func waitForChangeFeedRetry(ctx context.Context) bool {
	select {
	case <-ctx.Done():
		return false
	case <-time.After(changeFeedRetryDelay):
		return true
	}
}

func logChangeFeedError(ctx context.Context, logger libLog.Logger, message string, err error) {
	if logger == nil || err == nil {
		return
	}

	logger.Log(ctx, libLog.LevelWarn, message, libLog.String("error", err.Error()))
}

func handleChangeFeedSubscription(
	feedCtx context.Context,
	changeFeed ports.ChangeFeed,
	supervisor service.Supervisor,
	logger libLog.Logger,
	applySignal func(context.Context, ports.ChangeSignal) error,
) (error, error) {
	subscribeCtx, cancelSubscribe := context.WithCancel(feedCtx)
	defer cancelSubscribe()

	var applyErr error

	err := changeFeed.Subscribe(subscribeCtx, func(signal ports.ChangeSignal) {
		applyErr = applyChangeFeedSignal(feedCtx, signal, supervisor, applySignal)
		if applyErr == nil {
			return
		}

		logChangeFeedError(feedCtx, logger, "systemplane changefeed reload failed", applyErr)
		cancelSubscribe()
	})
	if err != nil {
		return fmt.Errorf("subscribe to changefeed: %w", err), applyErr
	}

	return nil, applyErr
}

func runChangeFeedSubscriber(
	feedCtx context.Context,
	changeFeed ports.ChangeFeed,
	supervisor service.Supervisor,
	logger libLog.Logger,
	applySignal func(context.Context, ports.ChangeSignal) error,
) {
	for {
		err, applyErr := handleChangeFeedSubscription(feedCtx, changeFeed, supervisor, logger, applySignal)
		if applyErr != nil {
			if !waitForChangeFeedRetry(feedCtx) {
				return
			}

			continue
		}

		if err == nil || errors.Is(err, context.Canceled) {
			return
		}

		logChangeFeedError(feedCtx, logger, "systemplane changefeed subscriber stopped", err)

		if !waitForChangeFeedRetry(feedCtx) {
			return
		}
	}
}

// StartChangeFeed starts the change feed subscriber that triggers supervisor
// reloads when store entries change. Returns a cancel function to stop the
// feed. If changeFeed is nil (e.g., in-memory store for testing), the
// returned cancel function is a no-op.
func startChangeFeed(
	ctx context.Context,
	changeFeed ports.ChangeFeed,
	supervisor service.Supervisor,
	logger libLog.Logger,
	applySignal func(context.Context, ports.ChangeSignal) error,
) (context.CancelFunc, error) {
	if domain.IsNilValue(changeFeed) {
		return func() {}, nil
	}

	if domain.IsNilValue(supervisor) {
		return nil, errChangeFeedSupervisorRequired
	}

	feedCtx, cancel := context.WithCancel(ctx)

	runtime.SafeGoWithContextAndComponent(
		feedCtx,
		logger,
		"systemplane",
		"changefeed.subscriber",
		runtime.KeepRunning,
		func(_ context.Context) {
			runChangeFeedSubscriber(feedCtx, changeFeed, supervisor, logger, applySignal)
		},
	)

	return cancel, nil
}
