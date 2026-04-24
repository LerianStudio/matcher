// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package bootstrap

import (
	"fmt"

	"github.com/LerianStudio/lib-commons/v5/commons/systemplane"
)

// systemplaneNamespace is the single namespace used for all Matcher
// runtime configuration keys. v5's Client uses (namespace, key) pairs;
// we use a flat namespace with dotted keys to preserve the existing key
// naming convention (e.g., "app.log_level").
const systemplaneNamespace = "matcher"

// RegisterMatcherKeys registers all runtime-mutable Matcher configuration
// keys on the v5 systemplane Client. Each key corresponds to a field in
// the Config struct and its sub-structs, using dotted mapstructure tag
// paths as key names.
//
// The registered default for each key is derived from `cfg`, which must be
// the env-resolved Config snapshot produced by LoadConfig/LoadConfigWithLogger.
// This seeds systemplane with operator intent so that env overrides like
// MATCHER_RATE_LIMIT_MAX=10000 propagate as the initial runtime value rather
// than being overridden by compile-time constants. Admin PUTs replace the
// stored value, and OnChange callbacks push updates back into *Config via
// applySystemplaneOverrides.
//
// Must be called before Client.Start().
func RegisterMatcherKeys(client *systemplane.Client, cfg *Config) error {
	if client == nil {
		return fmt.Errorf("register matcher keys: %w", ErrSystemplaneClientNil)
	}

	if cfg == nil {
		return fmt.Errorf("register matcher keys: %w", ErrConfigNil)
	}

	defs := matcherKeyDefs(cfg)
	for _, def := range defs {
		opts := []systemplane.KeyOption{
			systemplane.WithDescription(def.description),
		}

		if def.validator != nil {
			opts = append(opts, systemplane.WithValidator(def.validator))
		}

		if def.redact != systemplane.RedactNone {
			opts = append(opts, systemplane.WithRedaction(def.redact))
		}

		if err := client.Register(systemplaneNamespace, def.key, def.defaultValue, opts...); err != nil {
			return fmt.Errorf("register matcher key %q: %w", def.key, err)
		}
	}

	return nil
}

// matcherKeyDef is a local definition struct for building registration calls.
type matcherKeyDef struct {
	key          string
	defaultValue any
	description  string
	validator    func(any) error
	redact       systemplane.RedactPolicy
}
