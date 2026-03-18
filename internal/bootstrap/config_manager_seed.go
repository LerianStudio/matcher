// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package bootstrap

import (
	"context"
	"errors"
	"fmt"
	"reflect"

	libLog "github.com/LerianStudio/lib-commons/v4/commons/log"

	"github.com/LerianStudio/matcher/pkg/systemplane/domain"
	"github.com/LerianStudio/matcher/pkg/systemplane/ports"
	"github.com/LerianStudio/matcher/pkg/systemplane/registry"
)

// seedSource is the source label recorded in the systemplane store for
// entries created during the one-time seed migration.
const seedSource = "seed"

// seedActorID is the actor identity recorded for seed operations.
const seedActorID = "seed-migration"

// errSeedStoreConfigNil is returned when SeedStore is called with a nil config.
var errSeedStoreConfigNil = errors.New("seed store: config is nil")

// SeedStore seeds the systemplane store with non-default values from the
// current Config. This is a one-time migration: it reads the current Config,
// compares each mutable-at-runtime key against its registered default, and
// writes overrides for any non-default values.
//
// After seeding (regardless of whether any writes were needed), the
// ConfigManager enters seed mode (inert). In seed mode, hot-reload is
// disabled and callers should use the systemplane API instead.
//
// SeedStore is idempotent at the key level: only non-default values that
// differ from the registry default are written. Keys that already exist in the
// store are not checked here — the caller should verify that the store is empty
// (first startup) before calling SeedStore.
func (cm *ConfigManager) SeedStore(ctx context.Context, store ports.Store, reg registry.Registry) error {
	cfg := cm.Get()
	if cfg == nil {
		return errSeedStoreConfigNil
	}

	defs := reg.List(domain.KindConfig)

	ops := buildSeedOps(cfg, defs)

	if len(ops) == 0 {
		cm.logger.Log(ctx, libLog.LevelInfo,
			"seed store: no non-default values to seed, entering seed mode")
		cm.enterSeedMode()

		return nil
	}

	target, err := domain.NewTarget(domain.KindConfig, domain.ScopeGlobal, "")
	if err != nil {
		return fmt.Errorf("seed store: create target: %w", err)
	}

	_, err = store.Put(ctx, target, ops, domain.RevisionZero,
		domain.Actor{ID: seedActorID}, seedSource)
	if err != nil {
		return fmt.Errorf("seed store: put ops: %w", err)
	}

	cm.logger.Log(ctx, libLog.LevelInfo,
		"seed store: seeded systemplane store, entering seed mode",
		libLog.Int("keys_seeded", len(ops)))
	cm.enterSeedMode()

	return nil
}

// buildSeedOps computes the list of WriteOps for non-default, mutable-at-runtime
// config values. This is a pure function (no side-effects) that makes testing
// straightforward.
func buildSeedOps(cfg *Config, defs []domain.KeyDef) []ports.WriteOp {
	var ops []ports.WriteOp

	for _, def := range defs {
		if !def.MutableAtRuntime {
			continue // Skip bootstrap-only keys
		}

		currentVal := extractConfigValue(cfg, def.Key)
		if currentVal == nil {
			continue
		}

		if isEqualValue(currentVal, def.DefaultValue) {
			continue
		}

		ops = append(ops, ports.WriteOp{
			Key:   def.Key,
			Value: currentVal,
		})
	}

	return ops
}

// extractConfigValue extracts a value from the Config struct using the dotted
// key name (e.g., "app.log_level" -> cfg.App.LogLevel). It delegates to the
// existing reflection-based resolveConfigValue which uses mapstructure tags,
// ensuring the key mapping stays in sync with the Config struct definition.
//
// Returns nil if the key doesn't map to any Config field.
func extractConfigValue(cfg *Config, key string) any {
	if cfg == nil {
		return nil
	}

	val, ok := resolveConfigValue(cfg, key)
	if !ok {
		return nil
	}

	return val
}

// isEqualValue compares two config values for equality. It handles the common
// case where the Config struct stores typed values (int, bool, string) that
// need to be compared against default values that may be the same underlying
// type or a compatible type.
//
// Uses reflect.DeepEqual for robust cross-type comparison, but first tries
// a string-based comparison for simple scalar types where the types might
// differ (e.g., int vs int64 coming from different sources).
func isEqualValue(left, right any) bool {
	if left == nil && right == nil {
		return true
	}

	if left == nil || right == nil {
		return false
	}

	// Fast path: reflect.DeepEqual handles same-type comparisons perfectly.
	if reflect.DeepEqual(left, right) {
		return true
	}

	// Slow path: numeric types may differ (int vs int64 vs float64) but
	// represent the same value. Use the existing float64 converter.
	aFloat, aIsNum := toFloat64(left)
	bFloat, bIsNum := toFloat64(right)

	if aIsNum && bIsNum {
		return aFloat == bFloat
	}

	// Final fallback: string representation comparison for mixed types
	// (e.g., int vs string representation of the same number).
	return fmt.Sprintf("%v", left) == fmt.Sprintf("%v", right)
}
