// Copyright 2025 Lerian Studio.

package domain

import "time"

// EffectiveValue represents the resolved value of a configuration key after
// applying the override hierarchy (default -> global -> tenant).
type EffectiveValue struct {
	Key      string
	Value    any
	Default  any
	Override any
	Source   string
	Revision Revision
	Redacted bool
}

// Snapshot is an immutable point-in-time view of effective runtime state.
type Snapshot struct {
	Configs        map[string]EffectiveValue
	GlobalSettings map[string]EffectiveValue
	TenantSettings map[string]map[string]EffectiveValue
	Revision       Revision
	BuiltAt        time.Time
}

// GetConfig retrieves an effective configuration value by key.
func (s *Snapshot) GetConfig(key string) (EffectiveValue, bool) {
	if s == nil || s.Configs == nil {
		return EffectiveValue{}, false
	}

	v, ok := s.Configs[key]

	return v, ok
}

// GetGlobalSetting retrieves an effective setting value by key.
func (s *Snapshot) GetGlobalSetting(key string) (EffectiveValue, bool) {
	if s == nil || s.GlobalSettings == nil {
		return EffectiveValue{}, false
	}

	v, ok := s.GlobalSettings[key]

	return v, ok
}

// GetTenantSetting retrieves an effective tenant-scoped setting value by key.
func (s *Snapshot) GetTenantSetting(tenantID, key string) (EffectiveValue, bool) {
	if s == nil || s.TenantSettings == nil {
		return EffectiveValue{}, false
	}

	settings, ok := s.TenantSettings[tenantID]
	if !ok || settings == nil {
		return EffectiveValue{}, false
	}

	v, ok := settings[key]

	return v, ok
}

// ConfigValue returns the configuration value for the given key, or the
// fallback if the key is not present.
func (s *Snapshot) ConfigValue(key string, fallback any) any {
	if v, ok := s.GetConfig(key); ok {
		return v.Value
	}

	return fallback
}

// GlobalSettingValue returns the setting value for the given key, or the fallback
// if the key is not present.
func (s *Snapshot) GlobalSettingValue(key string, fallback any) any {
	if v, ok := s.GetGlobalSetting(key); ok {
		return v.Value
	}

	return fallback
}

// TenantSettingValue returns the tenant setting value for the given key, or the
// fallback if the key is not present.
func (s *Snapshot) TenantSettingValue(tenantID, key string, fallback any) any {
	if v, ok := s.GetTenantSetting(tenantID, key); ok {
		return v.Value
	}

	return fallback
}
