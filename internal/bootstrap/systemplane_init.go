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
	"os"

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
const defaultSystemplaneBackend = "postgres"

// Sentinel errors for systemplane initialization and bundle extraction.
var (
	errSupervisorNil                = errors.New("extract bundle: supervisor is nil")
	errNoCurrentBundle              = errors.New("extract bundle: supervisor has no current bundle")
	errUnexpectedBundleType         = errors.New("extract bundle: unexpected bundle type")
	errChangeFeedSupervisorRequired = errors.New("start change feed: supervisor is required")
)

// SystemplaneComponents holds all systemplane components created during
// initialization. Callers use these references to wire the manager into HTTP
// handlers and to shut down the backend when the application stops.
type SystemplaneComponents struct {
	Registry   registry.Registry
	Store      ports.Store
	History    ports.HistoryStore
	ChangeFeed ports.ChangeFeed
	Builder    *service.SnapshotBuilder
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

	return cfg, nil
}

// loadSystemplanePostgresConfig builds a PostgresBootstrapConfig from env vars,
// falling back to the application's primary postgres DSN when
// SYSTEMPLANE_POSTGRES_DSN is not set.
func loadSystemplanePostgresConfig(appCfg *Config) *spBootstrap.PostgresBootstrapConfig {
	dsn := os.Getenv("SYSTEMPLANE_POSTGRES_DSN")
	if dsn == "" && appCfg != nil {
		hostPort := net.JoinHostPort(appCfg.Postgres.PrimaryHost, appCfg.Postgres.PrimaryPort)
		dsn = fmt.Sprintf("postgres://%s:%s@%s/%s?sslmode=%s",
			appCfg.Postgres.PrimaryUser,
			appCfg.Postgres.PrimaryPassword,
			hostPort,
			appCfg.Postgres.PrimaryDB,
			appCfg.Postgres.PrimarySSLMode,
		)
	}

	pgCfg := &spBootstrap.PostgresBootstrapConfig{
		DSN: dsn,
	}

	if v := os.Getenv("SYSTEMPLANE_POSTGRES_SCHEMA"); v != "" {
		pgCfg.Schema = v
	}

	if v := os.Getenv("SYSTEMPLANE_POSTGRES_NOTIFY_CHANNEL"); v != "" {
		pgCfg.NotifyChannel = v
	}

	return pgCfg
}

// loadSystemplaneMongoConfig builds a MongoBootstrapConfig entirely from env
// vars. There is no fallback to the application config because Matcher does
// not require MongoDB for its own data.
func loadSystemplaneMongoConfig() *spBootstrap.MongoBootstrapConfig {
	mongoCfg := &spBootstrap.MongoBootstrapConfig{}

	if v := os.Getenv("SYSTEMPLANE_MONGO_URI"); v != "" {
		mongoCfg.URI = v
	}

	if v := os.Getenv("SYSTEMPLANE_MONGO_DATABASE"); v != "" {
		mongoCfg.Database = v
	}

	if v := os.Getenv("SYSTEMPLANE_MONGO_WATCH_MODE"); v != "" {
		mongoCfg.WatchMode = v
	}

	return mongoCfg
}

// InitSystemplane creates and wires all systemplane components. This is
// designed to be called during bootstrap, after env vars are loaded but before
// the Fiber app is constructed. The supervisor performs an initial reload that
// builds the first snapshot and bundle from registry defaults + any persisted
// store overrides.
//
// On any error the function cleans up partially-created resources before
// returning so the caller does not need to track intermediate state.
func InitSystemplane(ctx context.Context, cfg *Config, configManager *ConfigManager, workerManager *WorkerManager, logger libLog.Logger) (*SystemplaneComponents, error) {
	if cfg == nil {
		return nil, fmt.Errorf("init systemplane: %w", ErrConfigNil)
	}

	// 1. Extract bootstrap-only config.
	bootstrapCfg, err := ExtractBootstrapOnlyConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("init systemplane: %w", err)
	}

	// 2. Load systemplane backend config from env vars.
	backendCfg, err := LoadSystemplaneBackendConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("init systemplane: %w", err)
	}

	// 3. Create backend resources (store, history, changefeed).
	backend, err := builtin.NewBackendFromConfig(ctx, backendCfg)
	if err != nil {
		return nil, fmt.Errorf("init systemplane: create backend: %w", err)
	}

	// 4. Create registry and register all matcher keys.
	reg := registry.New()
	if err := RegisterMatcherKeys(reg); err != nil {
		_ = backend.Closer.Close()

		return nil, fmt.Errorf("init systemplane: %w", err)
	}

	// 5. Create snapshot builder.
	builder, err := service.NewSnapshotBuilder(reg, backend.Store)
	if err != nil {
		_ = backend.Closer.Close()

		return nil, fmt.Errorf("init systemplane: %w", err)
	}

	// 6. Create bundle factory.
	factory, err := NewMatcherBundleFactory(bootstrapCfg)
	if err != nil {
		_ = backend.Closer.Close()

		return nil, fmt.Errorf("init systemplane: %w", err)
	}

	// 7. Seed the store before the first reload so initial snapshot + bundle
	// include persisted/env overrides, not just registry defaults.
	if err := seedStoreForInitialReload(ctx, configManager, backend.Store, reg); err != nil {
		_ = backend.Closer.Close()

		return nil, fmt.Errorf("init systemplane: %w", err)
	}

	// 8. Create reconcilers.
	reconcilers, err := buildReconcilers(configManager, workerManager)
	if err != nil {
		_ = backend.Closer.Close()

		return nil, fmt.Errorf("init systemplane: %w", err)
	}

	// 9. Create supervisor.
	supervisor, err := service.NewSupervisor(service.SupervisorConfig{
		Builder:     builder,
		Factory:     factory,
		Reconcilers: reconcilers,
		Observer:    reloadObserver(ctx, logger),
	})
	if err != nil {
		_ = backend.Closer.Close()

		return nil, fmt.Errorf("init systemplane: %w", err)
	}

	// 10. Initial reload (builds first snapshot + bundle).
	if err := supervisor.Reload(ctx, "initial-bootstrap"); err != nil {
		_ = supervisor.Stop(ctx)
		_ = backend.Closer.Close()

		return nil, fmt.Errorf("init systemplane: initial reload: %w", err)
	}

	// 11. Create manager.
	manager, err := service.NewManager(service.ManagerConfig{
		Registry:   reg,
		Store:      backend.Store,
		History:    backend.History,
		Supervisor: supervisor,
		Builder:    builder,
	})
	if err != nil {
		_ = supervisor.Stop(ctx)
		_ = backend.Closer.Close()

		return nil, fmt.Errorf("init systemplane: %w", err)
	}

	return &SystemplaneComponents{
		Registry:   reg,
		Store:      backend.Store,
		History:    backend.History,
		ChangeFeed: backend.ChangeFeed,
		Builder:    builder,
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
		configManager.enterSeedMode()

		return nil
	}

	return fmt.Errorf("seed store: %w", seedErr)
}

// reloadObserver returns a callback that logs each reload event with the build
// strategy used. Returns nil if the logger is nil (observer is optional).
// The callback uses context.Background() rather than capturing the startup ctx,
// which may be cancelled before subsequent reloads occur.
func reloadObserver(_ context.Context, logger libLog.Logger) func(service.ReloadEvent) {
	if logger == nil {
		return nil
	}

	return func(event service.ReloadEvent) {
		logger.Log(context.Background(), libLog.LevelInfo, "systemplane reload completed",
			libLog.String("strategy", string(event.Strategy)),
			libLog.String("reason", event.Reason))
	}
}

// buildReconcilers creates the set of BundleReconcilers that the supervisor
// uses on each reload. Execution order is determined by each reconciler's
// Phase() — StateSync runs first, then Validation, then SideEffect.
// Registration order within the same phase is preserved (stable sort).
func buildReconcilers(configManager *ConfigManager, workerManager *WorkerManager) ([]ports.BundleReconciler, error) {
	var reconcilers []ports.BundleReconciler

	// Config bridge (PhaseStateSync) — must be present so configManager.Get()
	// is up-to-date before downstream reconcilers run.
	if configManager != nil {
		configBridge, err := NewConfigBridgeReconciler(configManager)
		if err != nil {
			return nil, fmt.Errorf("create config bridge reconciler: %w", err)
		}

		reconcilers = append(reconcilers, configBridge)
	}

	reconcilers = append(reconcilers, NewHTTPPolicyReconciler())

	if workerManager != nil {
		workerReconciler, err := NewWorkerReconciler(workerManager)
		if err != nil {
			return nil, fmt.Errorf("create worker reconciler: %w", err)
		}

		reconcilers = append(reconcilers, workerReconciler)
	}

	return reconcilers, nil
}

// StartChangeFeed starts the change feed subscriber that triggers supervisor
// reloads when store entries change. Returns a cancel function to stop the
// feed. If changeFeed is nil (e.g., in-memory store for testing), the
// returned cancel function is a no-op.
func StartChangeFeed(ctx context.Context, changeFeed ports.ChangeFeed, supervisor service.Supervisor) (context.CancelFunc, error) {
	if changeFeed == nil {
		return func() {}, nil
	}

	if supervisor == nil {
		return nil, errChangeFeedSupervisorRequired
	}

	feedCtx, cancel := context.WithCancel(ctx)

	runtime.SafeGoWithContextAndComponent(
		feedCtx,
		nil, // logger — nil-safe in SafeGoWithContextAndComponent
		"systemplane",
		"changefeed.subscriber",
		runtime.KeepRunning,
		func(_ context.Context) {
			_ = changeFeed.Subscribe(feedCtx, func(_ ports.ChangeSignal) {
				_ = supervisor.Reload(feedCtx, "changefeed")
			})
		},
	)

	return cancel, nil
}

// ExtractBundleFromSupervisor safely type-asserts the supervisor's current
// RuntimeBundle to a *MatcherBundle. Returns an error if the supervisor has
// no current bundle or the bundle is an unexpected type.
func ExtractBundleFromSupervisor(supervisor service.Supervisor) (*MatcherBundle, error) {
	if supervisor == nil {
		return nil, errSupervisorNil
	}

	current := supervisor.Current()
	if current == nil {
		return nil, errNoCurrentBundle
	}

	bundle, ok := current.(*MatcherBundle)
	if !ok {
		return nil, fmt.Errorf("%w: got %T", errUnexpectedBundleType, current)
	}

	return bundle, nil
}
