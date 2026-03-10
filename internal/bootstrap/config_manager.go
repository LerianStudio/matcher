package bootstrap

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/spf13/viper"

	libLog "github.com/LerianStudio/lib-commons/v4/commons/log"
	"github.com/LerianStudio/lib-commons/v4/commons/runtime"

	"github.com/LerianStudio/matcher/internal/shared/constants"
)

// errConfigNilAtomicLoad is returned when the atomic config pointer unexpectedly
// holds nil. This should never happen in practice (the constructor validates
// non-nil, and both reload and update only store validated configs).
var errConfigNilAtomicLoad = errors.New("current config is unexpectedly nil")

var (
	errConfigManagerInvalidPath        = errors.New("config manager: invalid config path")
	errConfigManagerInvalidExtension   = errors.New("config manager: config file must use .yaml or .yml extension")
	errConfigManagerPathOutsideWorkdir = errors.New("config manager: config path must be contained within working directory")
	errUnsafeConfigFilePath            = errors.New("unsafe config file path")
	errUnsafeConfigFileExtension       = errors.New("unsafe config file extension")
)

const (
	configUpdateSourceAPI           = "api"
	configUpdateSourceReload        = "reload"
	configUpdateSourceReloadAPI     = "reload_api"
	configUpdateSourceReloadWatcher = "reload_watcher"
)

// debounceDuration is the time window used to coalesce rapid file change events
// into a single reload. File editors often write multiple events (write → chmod →
// rename) for a single save — without debounce, each event would trigger a full
// config reload cycle. 500ms is long enough to coalesce editor saves while short
// enough to feel instantaneous to operators.
const debounceDuration = 500 * time.Millisecond

// ConfigManager manages configuration with hot-reload support.
//
// Thread-safety model:
//   - Readers call Get() which uses atomic.Pointer — lock-free, zero contention.
//   - Writers (Reload, Update) are serialized via mu to prevent concurrent YAML
//     reads/writes. The atomic swap happens inside the critical section, so readers
//     never block on the mutex.
//   - Subscribers are append-only under mu and invoked after the atomic swap.
//
// Lifecycle: NewConfigManager() → Get()/Subscribe()/Reload()/Update() → Stop().
type ConfigManager struct {
	config      atomic.Pointer[Config]
	mu          sync.Mutex // serializes writes (reload, update)
	viper       *viper.Viper
	filePath    string
	subscribers map[uint64]func(*Config)
	logger      libLog.Logger
	version     atomic.Uint64
	nextSubID   atomic.Uint64
	lastReload  atomic.Value // stores time.Time
	stopOnce    sync.Once
	watcherOnce sync.Once
	stopCh      chan struct{}

	// lastUpdateSource stores the origin of the most recent config change
	// ("api", "reload", "reload_api", "reload_watcher") so subscribers can discriminate. The API
	// handler publishes its own audit events, so the audit subscriber skips
	// when source == "api" or "reload_api" to prevent duplicates.
	lastUpdateSource atomic.Value // stores string

	// lastChanges stores the []ConfigChange from the most recent reload so
	// subscribers can access field-level diffs without re-computing them.
	lastChanges atomic.Value // stores []ConfigChange

	// debounceTimer is the active debounce timer for coalescing file events.
	// Protected by mu — only set/reset inside reloadDebounced which holds mu
	// or inside the debounce callback function itself.
	debounceTimer *time.Timer
}

// ReloadResult describes the outcome of a configuration reload.
type ReloadResult struct {
	Version         uint64         `json:"version"`
	ReloadedAt      time.Time      `json:"reloadedAt"`
	ChangesDetected int            `json:"changesDetected"`
	Changes         []ConfigChange `json:"changes,omitempty"`
}

// UpdateResult describes the outcome of a programmatic configuration update.
type UpdateResult struct {
	Applied  []ConfigChangeResult    `json:"applied,omitempty"`
	Rejected []ConfigChangeRejection `json:"rejected,omitempty"`
}

// ConfigChange captures a single key that changed between reloads.
type ConfigChange struct {
	Key      string `json:"key"`
	OldValue any    `json:"oldValue"`
	NewValue any    `json:"newValue"`
}

// ConfigChangeResult reports a successfully applied programmatic change.
type ConfigChangeResult struct {
	Key         string `json:"key"`
	OldValue    any    `json:"oldValue"`
	NewValue    any    `json:"newValue"`
	HotReloaded bool   `json:"hotReloaded"`
}

// ConfigChangeRejection reports a change that was not applied.
type ConfigChangeRejection struct {
	Key    string `json:"key"`
	Value  any    `json:"value"`
	Reason string `json:"reason"`
}

// mutableConfigKeys lists the YAML keys that may be changed via the Update API.
// Keys NOT in this set are considered immutable (env-only or infrastructure-bound)
// and will be rejected by Update(). This prevents operators from accidentally
// changing database hosts or auth secrets through the config API.
var mutableConfigKeys = map[string]bool{
	"app.log_level":                    true,
	"rate_limit.enabled":               true,
	"rate_limit.max":                   true,
	"rate_limit.expiry_sec":            true,
	"rate_limit.export_max":            true,
	"rate_limit.export_expiry_sec":     true,
	"rate_limit.dispatch_max":          true,
	"rate_limit.dispatch_expiry_sec":   true,
	"export_worker.enabled":            true,
	"export_worker.poll_interval_sec":  true,
	"export_worker.page_size":          true,
	"export_worker.presign_expiry_sec": true,
	"cleanup_worker.enabled":           true,
	"cleanup_worker.interval_sec":      true,
	"cleanup_worker.batch_size":        true,
	"cleanup_worker.grace_period_sec":  true,
	"scheduler.interval_sec":           true,
	"webhook.timeout_sec":              true,
	"callback_rate_limit.per_minute":   true,
	"deduplication.ttl_sec":            true,
	"idempotency.retry_window_sec":     true,
	"idempotency.success_ttl_hours":    true,
	"swagger.enabled":                  true,
	"fetcher.enabled":                  true,
	"fetcher.health_timeout_sec":       true,
	"fetcher.request_timeout_sec":      true,
	"fetcher.discovery_interval_sec":   true,
	"fetcher.schema_cache_ttl_sec":     true,
	"fetcher.extraction_poll_sec":      true,
	"fetcher.extraction_timeout_sec":   true,
	"archival.enabled":                 true,
	"archival.interval_hours":          true,
	"archival.batch_size":              true,
}

// NewConfigManager creates a ConfigManager that wraps the given initial config
// and sets up viper for YAML reading. The filePath should be the same path
// returned by resolveConfigFilePath(). If filePath is empty or the file doesn't
// exist, the manager still works — it just won't receive file-change events
// (env-only deployment mode).
//
// Call StartWatcher() after construction to enable automatic file-change
// detection via fsnotify. This is separated from the constructor because the
// file watcher launches a background goroutine that races with manual Reload()
// calls on viper's internal state, so callers that don't need automatic reload
// (e.g., unit tests) can skip it.
func NewConfigManager(cfg *Config, filePath string, logger libLog.Logger) (*ConfigManager, error) {
	if cfg == nil {
		return nil, ErrConfigNil
	}

	if logger == nil {
		logger = &libLog.NopLogger{}
	}

	filePath = filepath.Clean(strings.TrimSpace(filePath))
	if filePath == "." {
		filePath = ""
	}

	if filePath != "" {
		if err := validateManagerConfigPath(filePath); err != nil {
			return nil, err
		}
	}

	cm := &ConfigManager{
		filePath:    filePath,
		logger:      logger,
		stopCh:      make(chan struct{}),
		subscribers: make(map[uint64]func(*Config)),
	}

	cm.config.Store(cfg)
	cm.lastReload.Store(time.Now().UTC())

	// Create an isolated viper instance — no global state.
	viperCfg := viper.New()
	bindDefaults(viperCfg)

	viperCfg.SetEnvPrefix("MATCHER")
	viperCfg.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	viperCfg.AutomaticEnv()

	// Attempt to read the file. If it doesn't exist, that's fine — the
	// manager still serves the initial config and can Reload() later when
	// the file appears.
	if filePath != "" {
		viperCfg.SetConfigFile(filePath)

		if err := viperCfg.ReadInConfig(); err != nil && !isConfigFileNotFound(err) {
			return nil, fmt.Errorf("config manager: read initial YAML %s: %w", filePath, err)
		}
	}

	cm.viper = viperCfg

	return cm, nil
}

// StartWatcher enables automatic file-change detection via fsnotify. File
// changes are debounced (500ms) and trigger a full Reload() cycle. Safe to
// call only once. No-op if filePath is empty or the manager is already stopped.
//
// Uses a direct fsnotify.Watcher instead of viper.WatchConfig() to avoid a
// race: viper's watcher calls ReadInConfig() in its own goroutine before
// firing OnConfigChange, which races with our mu-protected reloadLocked().
// With a direct watcher, only our reloadLocked() (holding mu) calls
// ReadInConfig — eliminating concurrent viper access entirely.
func (cm *ConfigManager) StartWatcher() {
	if cm.filePath == "" {
		return
	}

	select {
	case <-cm.stopCh:
		return
	default:
	}

	cm.watcherOnce.Do(cm.startWatcher)
}

// Get returns the current configuration. This is the hot path — it uses an
// atomic load with zero locking overhead. Safe to call from any goroutine.
func (cm *ConfigManager) Get() *Config {
	return cm.config.Load()
}

// Subscribe registers a callback that will be invoked after every successful
// config reload. Callbacks receive the new *Config and run synchronously in
// the reload goroutine — keep them fast. Panics in callbacks are recovered
// and logged.
func (cm *ConfigManager) Subscribe(fn func(*Config)) {
	_ = cm.SubscribeWithUnsubscribe(fn)
}

// SubscribeWithUnsubscribe registers a callback and returns a function that
// removes the subscription. The returned function is idempotent and safe for
// repeated calls.
func (cm *ConfigManager) SubscribeWithUnsubscribe(fn func(*Config)) func() {
	if fn == nil {
		return func() {}
	}

	cm.mu.Lock()
	if cm.subscribers == nil {
		cm.subscribers = make(map[uint64]func(*Config))
	}

	id := cm.nextSubID.Add(1)
	cm.subscribers[id] = fn
	cm.mu.Unlock()

	var once sync.Once

	return func() {
		once.Do(func() {
			cm.mu.Lock()
			delete(cm.subscribers, id)
			cm.mu.Unlock()
		})
	}
}

// Reload force-reloads the configuration from disk. It re-reads the YAML file,
// applies environment variable overlays (backward compat), enforces production
// security defaults, and validates the result. If any step fails, the existing
// config is preserved and the error is returned.
//
// On success, the atomic pointer is swapped, version is incremented, and all
// subscribers are notified with the new config.
func (cm *ConfigManager) Reload() (*ReloadResult, error) {
	return cm.reload(configUpdateSourceReload)
}

// ReloadFromAPI force-reloads the configuration from disk, marking the source
// as an API-triggered reload so subscriber-based audit publishers can avoid
// duplicate event emission when handlers publish explicit audit entries.
func (cm *ConfigManager) ReloadFromAPI() (*ReloadResult, error) {
	return cm.reload(configUpdateSourceReloadAPI)
}

func (cm *ConfigManager) reload(source string) (*ReloadResult, error) {
	var (
		notifyCfg *Config
		callbacks []func(*Config)
	)

	cm.mu.Lock()
	defer func() {
		cm.mu.Unlock()

		if notifyCfg != nil {
			cm.notifySubscribers(notifyCfg, callbacks)
		}
	}()

	result, err := cm.reloadLocked(source)
	if err != nil {
		return nil, err
	}

	notifyCfg = cm.config.Load()
	callbacks = cm.snapshotSubscribersLocked()

	return result, nil
}

// reloadLocked performs the actual reload. Caller MUST hold cm.mu.
func (cm *ConfigManager) reloadLocked(source string) (*ReloadResult, error) {
	ctx := context.Background()

	if source == "" {
		source = configUpdateSourceReload
	}

	// Re-read the file into viper's store.
	if cm.filePath != "" {
		if err := cm.viper.ReadInConfig(); err != nil && !isConfigFileNotFound(err) {
			cm.logger.Log(ctx, libLog.LevelError, "config reload: failed to read YAML",
				libLog.String("path", cm.filePath), libLog.Err(err))

			return nil, fmt.Errorf("config reload: read YAML: %w", err)
		}
	}

	// Unmarshal into a fresh Config struct.
	newCfg := defaultConfig()
	if err := cm.viper.Unmarshal(newCfg); err != nil {
		cm.logger.Log(ctx, libLog.LevelError, "config reload: unmarshal failed", libLog.Err(err))

		return nil, fmt.Errorf("config reload: unmarshal: %w", err)
	}

	// Env-var overlay: preserves backward compat with direct env vars
	// (e.g., POSTGRES_HOST without MATCHER_ prefix).
	// loadConfigFromEnv uses SetConfigFromEnvVars which zeroes fields when
	// the corresponding env var is absent. We snapshot the viper-based config
	// BEFORE the overlay and restore any fields that got blanked.
	viperSnapshot := *newCfg
	if err := loadConfigFromEnv(newCfg); err != nil {
		cm.logger.Log(ctx, libLog.LevelError, "config reload: env overlay failed", libLog.Err(err))

		return nil, fmt.Errorf("config reload: env overlay: %w", err)
	}

	restoreZeroedFields(newCfg, &viperSnapshot)

	// Carry forward the logger from the current config (it's not YAML-managed).
	oldCfg := cm.config.Load()
	if oldCfg == nil {
		return nil, fmt.Errorf("config reload: %w", errConfigNilAtomicLoad)
	}

	newCfg.Logger = oldCfg.Logger
	newCfg.ShutdownGracePeriod = oldCfg.ShutdownGracePeriod

	// Enforce body limit default before production security defaults.
	if newCfg.Server.BodyLimitBytes <= 0 {
		newCfg.Server.BodyLimitBytes = defaultHTTPBodyLimitBytes
	}

	// Production security enforcement (silent corrections with warnings).
	newCfg.enforceProductionSecurityDefaults(cm.logger)

	// Validate — reject bad config before swapping.
	if err := newCfg.Validate(); err != nil {
		cm.logger.Log(ctx, libLog.LevelError, "config reload: validation failed", libLog.Err(err))

		return nil, fmt.Errorf("config reload: validation: %w", err)
	}

	// Compute diff before swap.
	changes := diffConfigs(oldCfg, newCfg)

	// Atomic swap — readers see the new config immediately.
	cm.config.Store(newCfg)

	now := time.Now().UTC()
	newVersion := cm.version.Add(1)
	cm.lastReload.Store(now)

	result := &ReloadResult{
		Version:         newVersion,
		ReloadedAt:      now,
		ChangesDetected: len(changes),
		Changes:         changes,
	}

	cm.logger.Log(ctx, libLog.LevelInfo, "config reloaded successfully",
		libLog.Int("version", safeUint64ToInt(newVersion)),
		libLog.Int("changes", len(changes)))

	// Store changes and source BEFORE notifying so subscribers can access them.
	cm.lastChanges.Store(changes)
	cm.lastUpdateSource.Store(source)

	return result, nil
}

// Update applies programmatic changes to the config. Each key is validated:
//   - Must be a known mutable key (in mutableConfigKeys)
//   - Must pass overall config validation after applying
//
// Changes are written to the YAML file via atomic rename, then Reload() is
// triggered to pick them up through the normal pipeline.
func (cm *ConfigManager) Update(changes map[string]any) (*UpdateResult, error) {
	var (
		notifyCfg *Config
		callbacks []func(*Config)
	)

	cm.mu.Lock()
	defer func() {
		cm.mu.Unlock()

		if notifyCfg != nil {
			cm.notifySubscribers(notifyCfg, callbacks)
		}
	}()

	ctx := context.Background()
	result := &UpdateResult{}

	if len(changes) == 0 {
		return result, nil
	}

	// Phase 1: classify changes as applicable or rejected.
	applicableChanges := classifyApplicableChanges(changes, result)

	if len(applicableChanges) == 0 {
		return result, nil
	}

	// Phase 1.5: validate value types against schema expectations.
	rejectTypeErrors(applicableChanges, result)

	if len(applicableChanges) == 0 {
		return result, nil
	}

	// Phase 2: apply to viper and compute old values.
	oldCfg := cm.config.Load()
	if oldCfg == nil {
		return nil, fmt.Errorf("config update: %w", errConfigNilAtomicLoad)
	}

	oldValues := make(map[string]any, len(applicableChanges))

	for _, key := range sortedChangeKeys(applicableChanges) {
		value := applicableChanges[key]
		oldValues[key] = cm.viper.Get(key)
		cm.viper.Set(key, value)
	}

	// Phase 3: build, overlay, and validate the candidate config.
	candidateCfg, err := cm.buildCandidateConfig(oldCfg)
	if err != nil {
		cm.rollbackViperKeys(applicableChanges, oldValues)

		return nil, fmt.Errorf("config update: %w", err)
	}

	// Phase 4: write YAML via atomic rename (temp file + rename).
	if cm.filePath != "" {
		if err := cm.writeConfigAtomically(); err != nil {
			cm.rollbackViperKeys(applicableChanges, oldValues)

			cm.logger.Log(ctx, libLog.LevelError, "config update: YAML write failed", libLog.Err(err))

			return nil, fmt.Errorf("config update: write YAML: %w", err)
		}
	}

	// Phase 5: do the swap (same as reload but we already have the validated config).
	cm.config.Store(candidateCfg)

	now := time.Now().UTC()
	newVersion := cm.version.Add(1)
	cm.lastReload.Store(now)

	// Build applied results. All mutable keys are hot-reloaded by design —
	// infrastructure-bound keys (server address, DB host, etc.) are excluded
	// from mutableConfigKeys, so they never reach this point.
	for _, key := range sortedChangeKeys(applicableChanges) {
		requested := applicableChanges[key]
		effectiveOld, _ := resolveConfigValue(oldCfg, key)
		effectiveNew, _ := resolveConfigValue(candidateCfg, key)
		hotReloaded := !valuesEquivalent(effectiveOld, effectiveNew)

		if !hotReloaded && !valuesEquivalent(requested, effectiveNew) {
			result.Rejected = append(result.Rejected, ConfigChangeRejection{
				Key:    key,
				Value:  redactIfSensitive(key, requested),
				Reason: "overridden by environment variable; value persisted but not effective at runtime",
			})

			continue
		}

		result.Applied = append(result.Applied, ConfigChangeResult{
			Key:         key,
			OldValue:    redactIfSensitive(key, effectiveOld),
			NewValue:    redactIfSensitive(key, effectiveNew),
			HotReloaded: hotReloaded,
		})
	}

	cm.logger.Log(ctx, libLog.LevelInfo, "config updated via API",
		libLog.Int("version", safeUint64ToInt(newVersion)),
		libLog.Int("applied", len(result.Applied)),
		libLog.Int("rejected", len(result.Rejected)))

	// Store changes for subscriber parity (even though API handler publishes its own audit,
	// this ensures any future non-API callers of Update() have changes available).
	var configChanges []ConfigChange

	for _, appliedChange := range result.Applied {
		key := appliedChange.Key
		configChanges = append(configChanges, ConfigChange{
			Key:      key,
			OldValue: appliedChange.OldValue,
			NewValue: appliedChange.NewValue,
		})
	}

	cm.lastChanges.Store(configChanges)

	// Mark source as API so the audit subscriber skips (API handler publishes its own event).
	cm.lastUpdateSource.Store(configUpdateSourceAPI)

	notifyCfg = candidateCfg
	callbacks = cm.snapshotSubscribersLocked()

	return result, nil
}

// Version returns the current config version. Starts at 0 and increments on
// each successful Reload() or Update(). Useful for cache invalidation and
// change detection by consumers.
func (cm *ConfigManager) Version() uint64 {
	return cm.version.Load()
}

// LastReloadAt returns the timestamp of the last successful config reload.
func (cm *ConfigManager) LastReloadAt() time.Time {
	if t, ok := cm.lastReload.Load().(time.Time); ok {
		return t
	}

	return time.Time{}
}

// Stop halts the file watcher and cleans up resources. Idempotent — safe to
// call multiple times. After Stop(), Reload() and Update() still work but no
// automatic file-driven reloads will occur.
func (cm *ConfigManager) Stop() {
	cm.stopOnce.Do(func() {
		close(cm.stopCh)

		cm.mu.Lock()
		if cm.debounceTimer != nil {
			cm.debounceTimer.Stop()
			cm.debounceTimer = nil
		}
		cm.mu.Unlock()
	})
}

// startWatcher uses a direct fsnotify.Watcher (instead of viper.WatchConfig)
// to detect file changes and trigger debounced reloads. This eliminates a race
// condition: viper.WatchConfig() internally calls ReadInConfig() in its own
// goroutine BEFORE firing OnConfigChange, which races with our mu-protected
// reloadLocked(). By owning the watcher directly, only our reloadLocked()
// (which holds mu) ever calls ReadInConfig — no concurrent viper access.
func (cm *ConfigManager) startWatcher() {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		cm.logger.Log(context.Background(), libLog.LevelError,
			"config file watcher: failed to create watcher", libLog.Err(err))

		return
	}

	if err := watcher.Add(filepath.Dir(cm.filePath)); err != nil {
		cm.logger.Log(context.Background(), libLog.LevelError,
			"config file watcher: failed to watch directory", libLog.Err(err))

		_ = watcher.Close()

		return
	}

	runtime.SafeGoWithContextAndComponent(
		context.Background(), cm.logger, constants.ApplicationName, "config.file_watcher",
		runtime.KeepRunning,
		func(_ context.Context) {
			defer func() { _ = watcher.Close() }()

			target := filepath.Base(cm.filePath)

			for {
				select {
				case <-cm.stopCh:
					return
				case event, ok := <-watcher.Events:
					if !ok {
						return
					}

					if filepath.Base(event.Name) == target && (event.Op&(fsnotify.Write|fsnotify.Create|fsnotify.Rename)) != 0 {
						//nolint:contextcheck // fire-and-forget log in background goroutine has no parent context
						cm.logger.Log(context.Background(), libLog.LevelDebug,
							"config file change detected, debouncing",
							libLog.String("event", event.Op.String()),
							libLog.String("path", event.Name))

						cm.reloadDebounced() //nolint:contextcheck // background goroutine — no parent context to propagate
					}
				case watchErr, ok := <-watcher.Errors:
					if !ok {
						return
					}

					//nolint:contextcheck // fire-and-forget log in background goroutine has no parent context
					cm.logger.Log(context.Background(), libLog.LevelError,
						"config file watcher error", libLog.Err(watchErr))
				}
			}
		},
	)
}

// reloadDebounced coalesces rapid file change events into a single reload.
// Each call resets the debounce timer. When the timer fires (no events for
// debounceDuration), Reload() is called.
func (cm *ConfigManager) reloadDebounced() {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	// Reset any existing timer.
	if cm.debounceTimer != nil {
		cm.debounceTimer.Stop()
	}

	cm.debounceTimer = time.AfterFunc(debounceDuration, func() {
		// Check if stopped before reloading.
		select {
		case <-cm.stopCh:
			return
		default:
		}

		if _, err := cm.reload(configUpdateSourceReloadWatcher); err != nil {
			cm.logger.Log(context.Background(), libLog.LevelError,
				"automatic config reload failed (file watcher)",
				libLog.Err(err))
		}
	})
}

// rejectTypeErrors validates value types against the config schema and rejects
// mismatched entries. Mutates applicableChanges (deletes rejected keys) and
// appends rejections to result.
func rejectTypeErrors(applicableChanges map[string]any, result *UpdateResult) {
	schema := buildConfigSchema()
	schemaByKey := make(map[string]configFieldDef, len(schema))

	for _, def := range schema {
		schemaByKey[def.Key] = def
	}

	for _, key := range sortedChangeKeys(applicableChanges) {
		value := applicableChanges[key]

		def, ok := schemaByKey[key]
		if !ok {
			continue // key not in schema — let viper handle it
		}

		if !isValueTypeCompatible(value, def.Type) {
			result.Rejected = append(result.Rejected, ConfigChangeRejection{
				Key:    key,
				Value:  value,
				Reason: "type mismatch: expected " + def.Type,
			})

			delete(applicableChanges, key)
		}
	}
}

func classifyApplicableChanges(changes map[string]any, result *UpdateResult) map[string]any {
	applicableChanges := make(map[string]any, len(changes))

	for _, key := range sortedChangeKeys(changes) {
		value := changes[key]
		if !mutableConfigKeys[key] {
			result.Rejected = append(result.Rejected, ConfigChangeRejection{
				Key:    key,
				Value:  value,
				Reason: "key is not mutable via API (env-only or infrastructure-bound)",
			})

			continue
		}

		applicableChanges[key] = value
	}

	return applicableChanges
}

func sortedChangeKeys(values map[string]any) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}

	sort.Strings(keys)

	return keys
}

// buildCandidateConfig unmarshals the current viper state into a new Config,
// applies env overlay + production security defaults, and validates. Returns
// the validated config or an error (caller should roll back viper keys on error).
func (cm *ConfigManager) buildCandidateConfig(oldCfg *Config) (*Config, error) {
	candidateCfg := defaultConfig()
	if err := cm.viper.Unmarshal(candidateCfg); err != nil {
		return nil, fmt.Errorf("unmarshal: %w", err)
	}

	candidateCfg.Logger = oldCfg.Logger
	candidateCfg.ShutdownGracePeriod = oldCfg.ShutdownGracePeriod

	if candidateCfg.Server.BodyLimitBytes <= 0 {
		candidateCfg.Server.BodyLimitBytes = defaultHTTPBodyLimitBytes
	}

	// Apply env overlay for backward compatibility. See reloadLocked comment
	// for why we snapshot and restore.
	candidateSnapshot := *candidateCfg
	if err := loadConfigFromEnv(candidateCfg); err != nil {
		return nil, fmt.Errorf("env overlay: %w", err)
	}

	restoreZeroedFields(candidateCfg, &candidateSnapshot)

	candidateCfg.enforceProductionSecurityDefaults(cm.logger)

	if err := candidateCfg.Validate(); err != nil {
		return nil, fmt.Errorf("validation failed: %w", err)
	}

	return candidateCfg, nil
}

func (cm *ConfigManager) snapshotSubscribersLocked() []func(*Config) {
	callbacks := make([]func(*Config), 0, len(cm.subscribers))
	for _, fn := range cm.subscribers {
		callbacks = append(callbacks, fn)
	}

	return callbacks
}

// notifySubscribers calls each registered subscriber with the new config.
// Panics in subscribers are recovered and logged.
func (cm *ConfigManager) notifySubscribers(cfg *Config, callbacks []func(*Config)) {
	ctx := context.Background()

	if len(callbacks) == 0 {
		return
	}

	for i, fn := range callbacks {
		func(idx int, callback func(*Config)) {
			defer func() {
				if r := recover(); r != nil {
					cm.logger.Log(ctx, libLog.LevelError,
						fmt.Sprintf("config subscriber %d panicked: %v", idx, r))
				}
			}()

			callback(cfg)
		}(i, fn)
	}
}

// writeConfigAtomically writes the current viper state to the config file
// using atomic rename: write to temp file in the same directory, then rename.
// This prevents partial-write corruption. The original file's permissions are
// preserved on the new file to avoid accidental permission changes.
func (cm *ConfigManager) writeConfigAtomically() error {
	path := filepath.Clean(strings.TrimSpace(cm.filePath))
	if err := validateAtomicWritePath(path); err != nil {
		return err
	}

	dir := filepath.Dir(path)
	base := filepath.Base(path)
	ext := filepath.Ext(base)
	stem := strings.TrimSuffix(base, ext)
	tmpPattern := base + ".tmp.*"

	if ext != "" && stem != "" {
		tmpPattern = stem + ".tmp.*" + ext
	}

	// Snapshot original file permissions before writing (best-effort).
	var origPerm os.FileMode

	if info, err := os.Stat(path); err == nil {
		origPerm = info.Mode().Perm()
	}

	tmpFile, err := os.CreateTemp(dir, tmpPattern)
	if err != nil {
		return fmt.Errorf("create temp config file: %w", err)
	}

	tmpPath := tmpFile.Name()

	// Clean up on failure.
	success := false

	defer func() {
		if !success {
			_ = os.Remove(tmpPath)
		}
	}()

	if err := cm.viper.WriteConfigAs(tmpPath); err != nil {
		_ = tmpFile.Close()

		return fmt.Errorf("write temp config file: %w", err)
	}

	if err := tmpFile.Close(); err != nil {
		return fmt.Errorf("close temp config file: %w", err)
	}

	// Restore original permissions on the temp file before rename, so the
	// atomic rename preserves them. Best-effort — if chmod fails, the file
	// keeps the default 0600 from CreateTemp (which is more restrictive).
	if origPerm != 0 {
		_ = os.Chmod(tmpPath, origPerm)
	}

	// When origPerm == 0 (original file doesn't exist or Stat failed), the temp
	// file retains the default 0600 permissions from os.CreateTemp. This is
	// intentionally more restrictive than typical config file permissions.

	if err := os.Rename(tmpPath, path); err != nil {
		return fmt.Errorf("atomic rename config file: %w", err)
	}

	success = true

	return nil
}

// rollbackViperKeys restores the given viper keys to their previous values.
// Used by Update() to undo partial changes when validation fails.
func (cm *ConfigManager) rollbackViperKeys(keys, oldValues map[string]any) {
	for key := range keys {
		cm.viper.Set(key, oldValues[key])
	}
}

// isValueTypeCompatible checks if a JSON-deserialized value is compatible with
// the expected schema type. JSON numbers can be float64 or json.Number; both
// are accepted for int fields since viper handles the conversion.
func isValueTypeCompatible(value any, expectedType string) bool {
	if value == nil {
		return false
	}

	switch expectedType {
	case "string":
		_, ok := value.(string)
		return ok
	case "int":
		switch value.(type) {
		case int, int64, float64: // JSON numbers deserialize as float64
			return true
		default:
			return false
		}
	case "bool":
		_, ok := value.(bool)
		return ok
	default:
		return true // unknown type — let viper handle it
	}
}

func valuesEquivalent(left, right any) bool {
	leftNumber, leftIsNumber := toFloat64(left)
	rightNumber, rightIsNumber := toFloat64(right)
	if leftIsNumber && rightIsNumber {
		return math.Abs(leftNumber-rightNumber) < 1e-9
	}

	return reflect.DeepEqual(left, right)
}

func toFloat64(value any) (float64, bool) {
	switch typed := value.(type) {
	case int:
		return float64(typed), true
	case int8:
		return float64(typed), true
	case int16:
		return float64(typed), true
	case int32:
		return float64(typed), true
	case int64:
		return float64(typed), true
	case uint:
		return float64(typed), true
	case uint8:
		return float64(typed), true
	case uint16:
		return float64(typed), true
	case uint32:
		return float64(typed), true
	case uint64:
		return float64(typed), true
	case float32:
		return float64(typed), true
	case float64:
		return typed, true
	default:
		return 0, false
	}
}

func validateManagerConfigPath(filePath string) error {
	if strings.ContainsRune(filePath, '\x00') {
		return errConfigManagerInvalidPath
	}

	if !hasYAMLExtension(filePath) {
		return errConfigManagerInvalidExtension
	}

	if !filepath.IsAbs(filePath) && !isPathContained(filePath) {
		return errConfigManagerPathOutsideWorkdir
	}

	return nil
}

func validateAtomicWritePath(path string) error {
	if path == "" || strings.ContainsRune(path, '\x00') {
		return errUnsafeConfigFilePath
	}

	if !hasYAMLExtension(path) {
		return errUnsafeConfigFileExtension
	}

	if !filepath.IsAbs(path) && !isPathContained(path) {
		return errUnsafeConfigFilePath
	}

	return nil
}
