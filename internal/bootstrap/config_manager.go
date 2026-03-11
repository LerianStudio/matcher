package bootstrap

import (
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/spf13/viper"

	libLog "github.com/LerianStudio/lib-commons/v4/commons/log"
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
