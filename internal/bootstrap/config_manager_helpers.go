// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package bootstrap

import (
	"context"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"

	libLog "github.com/LerianStudio/lib-commons/v4/commons/log"
)

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

func (cm *ConfigManager) snapshotSubscribersLocked() []func(*Config) {
	ids := make([]uint64, 0, len(cm.subscribers))
	for id := range cm.subscribers {
		ids = append(ids, id)
	}

	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })

	callbacks := make([]func(*Config), 0, len(ids))
	for _, id := range ids {
		callbacks = append(callbacks, cm.subscribers[id])
	}

	return callbacks
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

// floatEqualityTolerance is the maximum absolute difference for two floating-point
// numbers to be considered equivalent in config value comparisons.
const floatEqualityTolerance = 1e-9

func valuesEquivalent(left, right any) bool {
	leftNumber, leftIsNumber := toFloat64(left)
	rightNumber, rightIsNumber := toFloat64(right)

	if leftIsNumber && rightIsNumber {
		return math.Abs(leftNumber-rightNumber) < floatEqualityTolerance
	}

	return reflect.DeepEqual(left, right)
}

// toFloat64 converts any numeric value to float64 using reflect.Kind grouping.
func toFloat64(value any) (float64, bool) {
	if value == nil {
		return 0, false
	}

	reflected := reflect.ValueOf(value)

	switch reflected.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return float64(reflected.Int()), true
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return float64(reflected.Uint()), true
	case reflect.Float32, reflect.Float64:
		return reflected.Float(), true
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
