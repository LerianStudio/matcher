// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package bootstrap

import (
	"time"

	"github.com/LerianStudio/lib-commons/v4/commons/systemplane/domain"
)

// defaultConfig returns a Config populated with sensible defaults derived from
// the systemplane key definitions in matcherKeyDefs(). The KeyDef slice is the
// SINGLE SOURCE OF TRUTH for all default values — this function builds a
// synthetic snapshot from those definitions and hydrates a *Config through the
// same configFromSnapshot path used at runtime.
//
// This eliminates the former triple-source-of-truth problem where defaults
// had to be kept in sync between defaultConfig(), matcherKeyDefs(), and
// envDefault struct tags. Now matcherKeyDefs() is the canonical source.
//
// Logger and ShutdownGracePeriod are left as zero values; they are set
// during bootstrap.
func defaultConfig() *Config {
	return configFromSnapshot(defaultSnapshotFromKeyDefs(matcherKeyDefs()))
}

// defaultSnapshotFromKeyDefs builds a synthetic Snapshot containing only
// registry default values. This snapshot is used to derive the initial
// *Config through snapshotToFullConfig, ensuring the default config is
// computed from the same key definitions used at runtime.
func defaultSnapshotFromKeyDefs(defs []domain.KeyDef) domain.Snapshot {
	configs := make(map[string]domain.EffectiveValue, len(defs))

	for _, def := range defs {
		// Include all keys regardless of Kind — snapshotToFullConfig reads
		// from snap.Configs for all runtime-managed fields.
		configs[def.Key] = domain.EffectiveValue{
			Key:     def.Key,
			Value:   def.DefaultValue,
			Default: def.DefaultValue,
			Source:  "registry-default",
		}
	}

	return domain.Snapshot{
		Configs: configs,
		BuiltAt: time.Now().UTC(),
	}
}
