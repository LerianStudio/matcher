// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package bootstrap

import (
	"context"
	"path/filepath"
	"time"

	"github.com/fsnotify/fsnotify"

	libLog "github.com/LerianStudio/lib-commons/v4/commons/log"
	"github.com/LerianStudio/lib-commons/v4/commons/runtime"

	"github.com/LerianStudio/matcher/internal/shared/constants"
)

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

		if closeErr := watcher.Close(); closeErr != nil {
			cm.logger.Log(context.Background(), libLog.LevelWarn,
				"config file watcher: failed to close watcher after watch error", libLog.Err(closeErr))
		}

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
