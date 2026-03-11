// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package bootstrap

import "time"

// GetConfigResponse represents the current effective configuration.
// @Description Current system configuration with metadata
type GetConfigResponse struct {
	// Config sections with redacted secrets
	Config map[string]any `json:"config"`
	// Current config version (increments on each reload/update)
	Version uint64 `json:"version" example:"3"`
	// Timestamp of last successful reload
	LastReloadAt time.Time `json:"lastReloadAt" example:"2025-07-15T10:30:00Z"`
	// List of keys currently overridden by environment variables
	EnvOverrides []string `json:"envOverrides" example:"postgres.primary_password,auth.token_secret"`
}

// ConfigFieldSchema describes a single configuration field for UI rendering.
// @Description Schema metadata for a single config field
type ConfigFieldSchema struct {
	// Dot-notation key (e.g., "rate_limit.max")
	Key string `json:"key" example:"rate_limit.max"`
	// Human-readable label
	Label string `json:"label" example:"Rate Limit Max"`
	// Data type: string, int, bool
	Type string `json:"type" example:"int" enums:"string,int,bool"`
	// Default value
	DefaultValue any `json:"defaultValue" swaggertype:"string" example:"100"`
	// Current effective value (redacted for secrets)
	CurrentValue any `json:"currentValue" swaggertype:"string" example:"200"`
	// Whether changes take effect without restart
	HotReloadable bool `json:"hotReloadable" example:"true"`
	// Whether the field is currently overridden by an env var
	EnvOverride bool `json:"envOverride" example:"false"`
	// Name of the corresponding environment variable
	EnvVar string `json:"envVar,omitempty" example:"RATE_LIMIT_MAX"`
	// Validation constraints (e.g., "min:1", "max:10000")
	Constraints []string `json:"constraints,omitempty" example:"min:1,max:10000"`
	// Human-readable description
	Description string `json:"description" example:"Maximum requests per rate limit window"`
	// Logical section for UI grouping
	Section string `json:"section" example:"rate_limit"`
}

// ConfigSchemaResponse groups field schemas by section for UI tabs.
// @Description Configuration schema grouped by section
type ConfigSchemaResponse struct {
	// Sections maps section names to their field schemas
	Sections map[string][]ConfigFieldSchema `json:"sections"`
	// Total number of managed fields
	TotalFields int `json:"totalFields" example:"56"`
}

// UpdateConfigRequest is the request body for PATCH /v1/system/config.
// @Description Request to update configuration values
type UpdateConfigRequest struct {
	// Map of dotted keys to new values
	Changes map[string]any `json:"changes" validate:"required"`
}

// UpdateConfigResponse reports the result of a config update.
// @Description Result of a configuration update operation
type UpdateConfigResponse struct {
	// Successfully applied changes (secret values are redacted)
	Applied []ConfigChangeResult `json:"applied,omitempty"`
	// Changes that were rejected (immutable keys, validation failures)
	Rejected []ConfigChangeRejection `json:"rejected,omitempty"`
	// New config version after update
	Version uint64 `json:"version" example:"4"`
}

// ReloadConfigResponse reports the result of a config reload from disk.
// @Description Result of a configuration reload operation
type ReloadConfigResponse struct {
	// New config version after reload
	Version uint64 `json:"version" example:"5"`
	// Timestamp of the reload
	ReloadedAt time.Time `json:"reloadedAt" example:"2025-07-15T10:35:00Z"`
	// Number of changed fields detected
	ChangesDetected int `json:"changesDetected" example:"2"`
	// Details of each changed field
	Changes []ConfigChange `json:"changes,omitempty"`
}

// ConfigHistoryEntry represents a single config change event.
// @Description A historical configuration change event
type ConfigHistoryEntry struct {
	// Timestamp of the change
	Timestamp time.Time `json:"timestamp" example:"2025-07-15T10:30:00Z"`
	// Actor who made the change (user ID or "system")
	Actor string `json:"actor" example:"system"`
	// Type of change: "reload", "update", "startup"
	ChangeType string `json:"changeType" example:"update"`
	// Changed fields with old/new values
	Changes []ConfigChange `json:"changes,omitempty"`
}

// ConfigHistoryResponse returns recent config change history.
// @Description Configuration change history
type ConfigHistoryResponse struct {
	// History entries ordered newest-first
	Items []ConfigHistoryEntry `json:"items"`
}
