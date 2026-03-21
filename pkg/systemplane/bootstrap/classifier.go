// Copyright 2025 Lerian Studio.

package bootstrap

import "github.com/LerianStudio/matcher/pkg/systemplane/domain"

// IsBootstrapOnly reports whether a key definition belongs to the bootstrap-only
// category and therefore cannot be mutated at runtime.
func IsBootstrapOnly(def domain.KeyDef) bool {
	return !def.MutableAtRuntime || def.ApplyBehavior == domain.ApplyBootstrapOnly
}

// IsRuntimeManaged reports whether a key definition can be managed at runtime.
func IsRuntimeManaged(def domain.KeyDef) bool {
	return !IsBootstrapOnly(def)
}
