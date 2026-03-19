// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package bootstrap

import (
	"context"
	"errors"
	"fmt"
	"sort"

	libLog "github.com/LerianStudio/lib-commons/v4/commons/log"
	libZap "github.com/LerianStudio/lib-commons/v4/commons/zap"

	"github.com/LerianStudio/matcher/internal/shared/constants"
	"github.com/LerianStudio/matcher/pkg/systemplane/domain"
	"github.com/LerianStudio/matcher/pkg/systemplane/ports"
)

// Compile-time interface checks.
var (
	_ ports.BundleFactory            = (*MatcherBundleFactory)(nil)
	_ ports.IncrementalBundleFactory = (*MatcherBundleFactory)(nil)
)

// MatcherBundleFactory creates MatcherBundle instances from snapshot configuration.
// It holds references to long-lived dependencies that remain constant across config
// changes (e.g., bootstrap-only keys extracted once at startup).
type MatcherBundleFactory struct {
	bootstrapCfg *BootstrapOnlyConfig
}

// BootstrapOnlyConfig holds the keys that are marked ApplyBootstrapOnly in the
// systemplane key definitions. These are extracted once at startup and never
// change at runtime.
type BootstrapOnlyConfig struct {
	// App
	EnvName string

	// Server
	ServerAddress         string
	TLSCertFile           string
	TLSKeyFile            string
	TLSTerminatedUpstream bool
	TrustedProxies        string

	// Auth
	AuthEnabled bool
	AuthHost    string
	// AuthTokenSecret holds the JWT signing secret. Stored as string for
	// consistency with the Config.Auth.TokenSecret field. Consider migrating
	// to []byte for future secret-zeroing on shutdown.
	AuthTokenSecret string

	// Telemetry
	TelemetryEnabled           bool
	TelemetryServiceName       string
	TelemetryLibraryName       string
	TelemetryServiceVersion    string
	TelemetryDeploymentEnv     string
	TelemetryCollectorEndpoint string
	TelemetryDBMetricsInterval int
}

// ErrBootstrapConfigNil indicates a nil bootstrap config was provided to the factory.
var ErrBootstrapConfigNil = errors.New("new matcher bundle factory: bootstrap config is required")

// NewMatcherBundleFactory creates a new factory with the given bootstrap config.
func NewMatcherBundleFactory(bootstrapCfg *BootstrapOnlyConfig) (*MatcherBundleFactory, error) {
	if bootstrapCfg == nil {
		return nil, ErrBootstrapConfigNil
	}

	return &MatcherBundleFactory{bootstrapCfg: bootstrapCfg}, nil
}

// Build creates a new MatcherBundle by reading config values from the snapshot
// and constructing infrastructure clients. On partial failure, already-constructed
// clients are closed before returning the error.
func (factory *MatcherBundleFactory) Build(ctx context.Context, snap domain.Snapshot) (domain.RuntimeBundle, error) {
	loggerBundle, err := factory.buildLogger(snap)
	if err != nil {
		return nil, fmt.Errorf("build logger bundle: %w", err)
	}

	infra, err := factory.buildInfra(ctx, snap, loggerBundle.Logger)
	if err != nil {
		// Best-effort sync the logger before returning.
		_ = loggerBundle.Logger.Sync(ctx)

		return nil, fmt.Errorf("build infra bundle: %w", err)
	}

	httpPolicy := factory.buildHTTPPolicy(snap)

	return &MatcherBundle{
		Infra:  infra,
		HTTP:   httpPolicy,
		Logger: loggerBundle,
	}, nil
}

// allComponents enumerates every infrastructure component the factory manages,
// plus the ComponentNone sentinel for keys that require no rebuild.
// Used by BuildIncremental to detect full-rebuild fallback.
// Immutable after init — do not modify at runtime.
var allComponents = map[string]struct{}{
	"postgres":           {},
	"redis":              {},
	"rabbitmq":           {},
	"s3":                 {},
	"http":               {},
	"logger":             {},
	domain.ComponentNone: {},
}

// keyComponentMap is computed once from matcherKeyDefs(). It maps each config
// key that has a non-empty Component field to its component name.
// Immutable after init — do not modify at runtime.
var keyComponentMap = buildKeyComponentMap()

// buildKeyComponentMap iterates matcherKeyDefs() and returns a map from config
// key to its declared Component. Keys with empty Component are omitted — they
// are cross-cutting and do not affect incremental rebuilds.
func buildKeyComponentMap() map[string]string {
	defs := matcherKeyDefs()
	keyToComponent := make(map[string]string, len(defs))

	for _, def := range defs {
		if def.Component != "" {
			if _, known := allComponents[def.Component]; !known {
				// Crash at init time rather than silently degrading at runtime.
				// A typo here would cause the key to be treated as cross-cutting,
				// forcing unnecessary full rebuilds on every config change.
				panic(fmt.Sprintf("systemplane: key %q declares unknown Component %q; valid components: %v",
					def.Key, def.Component, allComponentNames()))
			}

			keyToComponent[def.Key] = def.Component
		}
	}

	return keyToComponent
}

// allComponentNames returns sorted component names for error messages.
func allComponentNames() []string {
	names := make([]string, 0, len(allComponents))
	for name := range allComponents {
		names = append(names, name)
	}

	sort.Strings(names)

	return names
}

// BuildIncremental creates a new MatcherBundle, reusing unchanged components
// from previous. It diffs prevSnap.Configs vs snap.Configs to identify which
// keys changed, maps those keys to infrastructure components via
// keyComponentMap, and only rebuilds affected components.
//
// The factory nil-outs transferred component pointers in previous so that
// previous.Close() only tears down replaced components.
//
// If the diff reveals that ALL components are affected, or if the previous
// bundle is not a *MatcherBundle, it falls back to a full Build.
//
//nolint:gocyclo,cyclop // Sequential per-component build/transfer is inherently branchy; splitting hurts readability.
func (factory *MatcherBundleFactory) BuildIncremental(
	ctx context.Context,
	snap domain.Snapshot,
	previous domain.RuntimeBundle,
	prevSnap domain.Snapshot,
) (domain.RuntimeBundle, error) {
	prev, ok := previous.(*MatcherBundle)
	if !ok || prev.Infra == nil || prev.Logger == nil {
		// Structurally incomplete previous bundle — fall back to a safe full
		// build rather than risk nil dereferences during component transfer.
		return factory.Build(ctx, snap)
	}

	// Diff: find set of affected component names.
	affected := factory.diffAffectedComponents(snap, prevSnap)

	// If every managed infrastructure component is affected, the full Build
	// path is simpler and avoids partial-transfer bookkeeping.
	// managedComponentCount excludes ComponentNone from the threshold since
	// ComponentNone keys don't trigger any rebuild.
	if len(affected) >= managedComponentCount() {
		return factory.Build(ctx, snap)
	}

	newBundle := &MatcherBundle{}

	// Track which fresh components we built so we can close them on rollback.
	var freshClosers []func() error

	rollback := func() {
		for i := len(freshClosers) - 1; i >= 0; i-- {
			_ = freshClosers[i]()
		}
	}

	// --- Logger ---
	if _, changed := affected["logger"]; changed {
		loggerBundle, err := factory.buildLogger(snap)
		if err != nil {
			rollback()
			return nil, fmt.Errorf("incremental build logger: %w", err)
		}

		newBundle.Logger = loggerBundle

		freshClosers = append(freshClosers, func() error {
			if loggerBundle.Logger != nil {
				return loggerBundle.Logger.Sync(ctx)
			}

			return nil
		})
	} else {
		// Transfer ownership from previous.
		newBundle.Logger = prev.Logger
		prev.Logger = nil
	}

	// We need a logger for infra builds. Use whichever we have.
	// Guard against a nil LoggerBundle — while the entry guard above ensures
	// prev.Logger is non-nil for the transfer path, and buildLogger returns
	// non-nil on success, this is a belt-and-suspenders safety net.
	var logger libLog.Logger
	if newBundle.Logger != nil {
		logger = newBundle.Logger.Logger
	}

	// --- Infrastructure components ---
	newBundle.Infra = &InfraBundle{}

	// Postgres
	if _, changed := affected["postgres"]; changed {
		pgClient, err := factory.buildPostgresClient(snap, logger)
		if err != nil {
			rollback()
			return nil, fmt.Errorf("incremental build postgres: %w", err)
		}

		newBundle.Infra.Postgres = pgClient

		if pgClient != nil {
			freshClosers = append(freshClosers, pgClient.Close)
		}
	} else {
		newBundle.Infra.Postgres = prev.Infra.Postgres
		prev.Infra.Postgres = nil
	}

	// Redis
	if _, changed := affected["redis"]; changed {
		redisClient, err := factory.buildRedisClient(ctx, snap, logger)
		if err != nil {
			rollback()
			return nil, fmt.Errorf("incremental build redis: %w", err)
		}

		newBundle.Infra.Redis = redisClient

		if redisClient != nil {
			freshClosers = append(freshClosers, redisClient.Close)
		}
	} else {
		newBundle.Infra.Redis = prev.Infra.Redis
		prev.Infra.Redis = nil
	}

	// RabbitMQ
	if _, changed := affected["rabbitmq"]; changed {
		rmqConn := factory.buildRabbitMQConnection(snap, logger)
		newBundle.Infra.RabbitMQ = rmqConn

		freshClosers = append(freshClosers, func() error {
			return closeRabbitMQ(rmqConn)
		})
	} else {
		newBundle.Infra.RabbitMQ = prev.Infra.RabbitMQ
		prev.Infra.RabbitMQ = nil
	}

	// S3 / Object Storage
	if _, changed := affected["s3"]; changed {
		s3Client, err := factory.buildObjectStorageClient(ctx, snap)
		if err != nil {
			rollback()
			return nil, fmt.Errorf("incremental build object storage: %w", err)
		}

		newBundle.Infra.ObjectStorage = s3Client

		if s3Client != nil {
			freshClosers = append(freshClosers, s3Client.Close)
		}
	} else {
		newBundle.Infra.ObjectStorage = prev.Infra.ObjectStorage
		prev.Infra.ObjectStorage = nil
	}

	// --- HTTP Policy ---
	if _, changed := affected["http"]; changed {
		newBundle.HTTP = factory.buildHTTPPolicy(snap)
	} else {
		newBundle.HTTP = prev.HTTP
		prev.HTTP = nil
	}

	return newBundle, nil
}

// diffAffectedComponents diffs snap.Configs against prevSnap.Configs and
// returns the set of component names whose config keys changed.
//
// Semantics:
//   - Keys mapped to a real component (postgres, redis, …) add that component.
//   - Keys mapped to ComponentNone are skipped — they require no rebuild.
//   - Unknown keys (not in keyComponentMap at all) force a full rebuild for safety.
func (factory *MatcherBundleFactory) diffAffectedComponents(
	snap domain.Snapshot,
	prevSnap domain.Snapshot,
) map[string]struct{} {
	// Nil maps mean no config data — treat as full rebuild for safety.
	if snap.Configs == nil || prevSnap.Configs == nil {
		return copyAllComponents()
	}

	affected := make(map[string]struct{})

	// Check keys present in new snapshot.
	for key, newEV := range snap.Configs {
		oldEV, existed := prevSnap.Configs[key]
		if !existed || !effectiveValuesEqual(oldEV, newEV) {
			comp, known := keyComponentMap[key]
			if !known {
				// Unknown key changed — force full rebuild for safety.
				return copyAllComponents()
			}

			if comp != domain.ComponentNone {
				affected[comp] = struct{}{}
			}
		}
	}

	// Check keys removed in the new snapshot (present in prev, absent in new).
	for key := range prevSnap.Configs {
		if _, stillExists := snap.Configs[key]; !stillExists {
			comp, known := keyComponentMap[key]
			if !known {
				return copyAllComponents()
			}

			if comp != domain.ComponentNone {
				affected[comp] = struct{}{}
			}
		}
	}

	return affected
}

// effectiveValuesEqual compares two EffectiveValue instances for equality on
// the fields that determine whether infrastructure needs rebuilding: Value and
// Override. Source, Default, and Key metadata are intentionally excluded — a
// change in config source (e.g., env-var → store) with the same effective value
// does not warrant an infrastructure rebuild.
// Uses valuesEquivalent for type-safe comparison that handles numeric coercion
// (e.g., int vs float64 from JSON deserialization) and falls back to
// reflect.DeepEqual for all other types.
func effectiveValuesEqual(a, b domain.EffectiveValue) bool {
	return valuesEquivalent(a.Value, b.Value) && valuesEquivalent(a.Override, b.Override)
}

// copyAllComponents returns a fresh copy of allComponents for use as the
// affected set, signaling that every component needs rebuilding.
func copyAllComponents() map[string]struct{} {
	cp := make(map[string]struct{}, len(allComponents))
	for k, v := range allComponents {
		cp[k] = v
	}

	return cp
}

// managedComponentCount returns the number of infrastructure components
// that require actual rebuilding (excludes ComponentNone).
func managedComponentCount() int {
	count := len(allComponents)
	if _, hasNone := allComponents[domain.ComponentNone]; hasNone {
		count--
	}

	return count
}

// buildHTTPPolicy extracts HTTP policy values from the snapshot with defaults.
func (factory *MatcherBundleFactory) buildHTTPPolicy(snap domain.Snapshot) *HTTPPolicyBundle {
	return &HTTPPolicyBundle{
		BodyLimitBytes:     snapInt(snap, "server.body_limit_bytes", defaultHTTPBodyLimitBytes),
		CORSAllowedOrigins: snapString(snap, "server.cors_allowed_origins", "http://localhost:3000"),
		CORSAllowedMethods: snapString(snap, "server.cors_allowed_methods", "GET,POST,PUT,PATCH,DELETE,OPTIONS"),
		CORSAllowedHeaders: snapString(snap, "server.cors_allowed_headers", "Origin,Content-Type,Accept,Authorization,X-Request-ID"),
		SwaggerEnabled:     snapBool(snap, "swagger.enabled", false),
		SwaggerHost:        snapString(snap, "swagger.host", ""),
		SwaggerSchemes:     snapString(snap, "swagger.schemes", "https"),
	}
}

// buildLogger constructs a new logger from the snapshot's log level and the
// bootstrap environment name.
func (factory *MatcherBundleFactory) buildLogger(snap domain.Snapshot) (*LoggerBundle, error) {
	level := snapString(snap, "app.log_level", defaultLoggerLevel)
	resolvedLevel := ResolveLoggerLevel(level)
	env := ResolveLoggerEnvironment(factory.bootstrapCfg.EnvName)

	logger, err := libZap.New(libZap.Config{
		Environment:     env,
		Level:           resolvedLevel,
		OTelLibraryName: constants.ApplicationName,
	})
	if err != nil {
		return nil, fmt.Errorf("create logger: %w", err)
	}

	return &LoggerBundle{
		Logger: logger,
		Level:  resolvedLevel,
	}, nil
}

// buildInfra constructs all infrastructure clients from the snapshot.
// On failure, already-constructed clients are closed in reverse order.
func (factory *MatcherBundleFactory) buildInfra(
	ctx context.Context,
	snap domain.Snapshot,
	logger libLog.Logger,
) (*InfraBundle, error) {
	pgClient, err := factory.buildPostgresClient(snap, logger)
	if err != nil {
		return nil, fmt.Errorf("build postgres: %w", err)
	}

	redisClient, err := factory.buildRedisClient(ctx, snap, logger)
	if err != nil {
		_ = pgClient.Close()

		return nil, fmt.Errorf("build redis: %w", err)
	}

	rmqConn := factory.buildRabbitMQConnection(snap, logger)

	s3Client, err := factory.buildObjectStorageClient(ctx, snap)
	if err != nil {
		_ = closeRabbitMQ(rmqConn)
		_ = redisClient.Close()
		_ = pgClient.Close()

		return nil, fmt.Errorf("build object storage: %w", err)
	}

	return &InfraBundle{
		Postgres:      pgClient,
		Redis:         redisClient,
		RabbitMQ:      rmqConn,
		ObjectStorage: s3Client,
	}, nil
}
