// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package bootstrap

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetConfigResponse_JSONRoundTrip(t *testing.T) {
	t.Parallel()

	original := GetConfigResponse{
		Config:       map[string]any{"rate_limit.max": float64(100)},
		Version:      3,
		LastReloadAt: time.Date(2025, 7, 15, 10, 30, 0, 0, time.UTC),
		EnvOverrides: []string{"postgres.primary_password", "auth.token_secret"},
	}

	data, err := json.Marshal(original)
	require.NoError(t, err)

	var decoded GetConfigResponse
	require.NoError(t, json.Unmarshal(data, &decoded))

	assert.Equal(t, original.Version, decoded.Version)
	assert.Equal(t, original.LastReloadAt, decoded.LastReloadAt)
	assert.Equal(t, original.EnvOverrides, decoded.EnvOverrides)
	assert.Equal(t, original.Config["rate_limit.max"], decoded.Config["rate_limit.max"])
}

func TestGetConfigResponse_JSONTagNames(t *testing.T) {
	t.Parallel()

	resp := GetConfigResponse{
		Config:       map[string]any{"k": "v"},
		Version:      1,
		LastReloadAt: time.Now().UTC(),
		EnvOverrides: []string{"x"},
	}

	data, err := json.Marshal(resp)
	require.NoError(t, err)

	raw := make(map[string]json.RawMessage)
	require.NoError(t, json.Unmarshal(data, &raw))

	for _, key := range []string{"config", "version", "lastReloadAt", "envOverrides"} {
		_, exists := raw[key]
		assert.True(t, exists, "expected JSON key %q", key)
	}
}

func TestConfigFieldSchema_JSONRoundTrip(t *testing.T) {
	t.Parallel()

	original := ConfigFieldSchema{
		Key:           "rate_limit.max",
		Label:         "Rate Limit Max",
		Type:          "int",
		DefaultValue:  100,
		CurrentValue:  200,
		HotReloadable: true,
		EnvOverride:   false,
		EnvVar:        "RATE_LIMIT_MAX",
		Constraints:   []string{"min:1", "max:10000"},
		Description:   "Maximum requests per rate limit window",
		Section:       "rate_limit",
	}

	data, err := json.Marshal(original)
	require.NoError(t, err)

	var decoded ConfigFieldSchema
	require.NoError(t, json.Unmarshal(data, &decoded))

	assert.Equal(t, original.Key, decoded.Key)
	assert.Equal(t, original.Label, decoded.Label)
	assert.Equal(t, original.Type, decoded.Type)
	assert.Equal(t, original.HotReloadable, decoded.HotReloadable)
	assert.Equal(t, original.EnvOverride, decoded.EnvOverride)
	assert.Equal(t, original.EnvVar, decoded.EnvVar)
	assert.Equal(t, original.Constraints, decoded.Constraints)
	assert.Equal(t, original.Description, decoded.Description)
	assert.Equal(t, original.Section, decoded.Section)
}

func TestConfigFieldSchema_JSONTagNames(t *testing.T) {
	t.Parallel()

	schema := ConfigFieldSchema{
		Key:           "k",
		Label:         "l",
		Type:          "string",
		DefaultValue:  "dv",
		CurrentValue:  "cv",
		HotReloadable: true,
		EnvOverride:   true,
		EnvVar:        "EV",
		Constraints:   []string{"min:1"},
		Description:   "d",
		Section:       "s",
	}

	data, err := json.Marshal(schema)
	require.NoError(t, err)

	raw := make(map[string]json.RawMessage)
	require.NoError(t, json.Unmarshal(data, &raw))

	expectedKeys := []string{
		"key", "label", "type", "defaultValue", "currentValue",
		"hotReloadable", "envOverride", "envVar", "constraints",
		"description", "section",
	}

	for _, key := range expectedKeys {
		_, exists := raw[key]
		assert.True(t, exists, "expected JSON key %q", key)
	}
}

func TestConfigFieldSchema_OmitsEmptyOptionalFields(t *testing.T) {
	t.Parallel()

	schema := ConfigFieldSchema{
		Key:   "k",
		Label: "l",
		Type:  "string",
	}

	data, err := json.Marshal(schema)
	require.NoError(t, err)

	raw := make(map[string]json.RawMessage)
	require.NoError(t, json.Unmarshal(data, &raw))

	// envVar and constraints have omitempty — should not appear when empty.
	_, hasEnvVar := raw["envVar"]
	assert.False(t, hasEnvVar, "envVar with omitempty should be absent when empty")

	_, hasConstraints := raw["constraints"]
	assert.False(t, hasConstraints, "constraints with omitempty should be absent when empty")
}

func TestConfigSchemaResponse_JSONRoundTrip(t *testing.T) {
	t.Parallel()

	original := ConfigSchemaResponse{
		Sections: map[string][]ConfigFieldSchema{
			"rate_limit": {
				{Key: "rate_limit.max", Label: "Max", Type: "int"},
			},
		},
		TotalFields: 1,
	}

	data, err := json.Marshal(original)
	require.NoError(t, err)

	var decoded ConfigSchemaResponse
	require.NoError(t, json.Unmarshal(data, &decoded))

	assert.Equal(t, original.TotalFields, decoded.TotalFields)
	assert.Len(t, decoded.Sections["rate_limit"], 1)
	assert.Equal(t, "rate_limit.max", decoded.Sections["rate_limit"][0].Key)
}

func TestConfigSchemaResponse_JSONTagNames(t *testing.T) {
	t.Parallel()

	resp := ConfigSchemaResponse{
		Sections:    map[string][]ConfigFieldSchema{},
		TotalFields: 0,
	}

	data, err := json.Marshal(resp)
	require.NoError(t, err)

	raw := make(map[string]json.RawMessage)
	require.NoError(t, json.Unmarshal(data, &raw))

	for _, key := range []string{"sections", "totalFields"} {
		_, exists := raw[key]
		assert.True(t, exists, "expected JSON key %q", key)
	}
}

func TestUpdateConfigRequest_JSONRoundTrip(t *testing.T) {
	t.Parallel()

	original := UpdateConfigRequest{
		Changes: map[string]any{"rate_limit.max": float64(200)},
	}

	data, err := json.Marshal(original)
	require.NoError(t, err)

	var decoded UpdateConfigRequest
	require.NoError(t, json.Unmarshal(data, &decoded))

	assert.Equal(t, original.Changes["rate_limit.max"], decoded.Changes["rate_limit.max"])
}

func TestUpdateConfigRequest_JSONTagNames(t *testing.T) {
	t.Parallel()

	req := UpdateConfigRequest{
		Changes: map[string]any{"k": "v"},
	}

	data, err := json.Marshal(req)
	require.NoError(t, err)

	raw := make(map[string]json.RawMessage)
	require.NoError(t, json.Unmarshal(data, &raw))

	_, exists := raw["changes"]
	assert.True(t, exists, "expected JSON key 'changes'")
}

func TestUpdateConfigResponse_JSONRoundTrip(t *testing.T) {
	t.Parallel()

	original := UpdateConfigResponse{
		Applied: []ConfigChangeResult{
			{Key: "rate_limit.max", OldValue: 100, NewValue: 200, HotReloaded: true},
		},
		Rejected: []ConfigChangeRejection{
			{Key: "postgres.primary_host", Value: "evil", Reason: "immutable"},
		},
		Version: 4,
	}

	data, err := json.Marshal(original)
	require.NoError(t, err)

	var decoded UpdateConfigResponse
	require.NoError(t, json.Unmarshal(data, &decoded))

	assert.Equal(t, original.Version, decoded.Version)
	require.Len(t, decoded.Applied, 1)
	assert.Equal(t, "rate_limit.max", decoded.Applied[0].Key)
	require.Len(t, decoded.Rejected, 1)
	assert.Equal(t, "postgres.primary_host", decoded.Rejected[0].Key)
}

func TestUpdateConfigResponse_JSONTagNames(t *testing.T) {
	t.Parallel()

	resp := UpdateConfigResponse{
		Applied:  []ConfigChangeResult{{Key: "k"}},
		Rejected: []ConfigChangeRejection{{Key: "k"}},
		Version:  1,
	}

	data, err := json.Marshal(resp)
	require.NoError(t, err)

	raw := make(map[string]json.RawMessage)
	require.NoError(t, json.Unmarshal(data, &raw))

	for _, key := range []string{"applied", "rejected", "version"} {
		_, exists := raw[key]
		assert.True(t, exists, "expected JSON key %q", key)
	}
}

func TestUpdateConfigResponse_OmitsEmptyOptionalFields(t *testing.T) {
	t.Parallel()

	resp := UpdateConfigResponse{
		Version: 1,
	}

	data, err := json.Marshal(resp)
	require.NoError(t, err)

	raw := make(map[string]json.RawMessage)
	require.NoError(t, json.Unmarshal(data, &raw))

	// applied and rejected have omitempty — should be absent when nil.
	_, hasApplied := raw["applied"]
	assert.False(t, hasApplied, "applied with omitempty should be absent when nil")

	_, hasRejected := raw["rejected"]
	assert.False(t, hasRejected, "rejected with omitempty should be absent when nil")
}

func TestReloadConfigResponse_JSONRoundTrip(t *testing.T) {
	t.Parallel()

	original := ReloadConfigResponse{
		Version:         5,
		ReloadedAt:      time.Date(2025, 7, 15, 10, 35, 0, 0, time.UTC),
		ChangesDetected: 2,
		Changes: []ConfigChange{
			{Key: "app.log_level", OldValue: "info", NewValue: "debug"},
		},
	}

	data, err := json.Marshal(original)
	require.NoError(t, err)

	var decoded ReloadConfigResponse
	require.NoError(t, json.Unmarshal(data, &decoded))

	assert.Equal(t, original.Version, decoded.Version)
	assert.Equal(t, original.ReloadedAt, decoded.ReloadedAt)
	assert.Equal(t, original.ChangesDetected, decoded.ChangesDetected)
	require.Len(t, decoded.Changes, 1)
	assert.Equal(t, "app.log_level", decoded.Changes[0].Key)
}

func TestReloadConfigResponse_JSONTagNames(t *testing.T) {
	t.Parallel()

	resp := ReloadConfigResponse{
		Version:         1,
		ReloadedAt:      time.Now().UTC(),
		ChangesDetected: 0,
	}

	data, err := json.Marshal(resp)
	require.NoError(t, err)

	raw := make(map[string]json.RawMessage)
	require.NoError(t, json.Unmarshal(data, &raw))

	for _, key := range []string{"version", "reloadedAt", "changesDetected"} {
		_, exists := raw[key]
		assert.True(t, exists, "expected JSON key %q", key)
	}
}

func TestConfigHistoryEntry_JSONRoundTrip(t *testing.T) {
	t.Parallel()

	original := ConfigHistoryEntry{
		Timestamp:  time.Date(2025, 7, 15, 10, 30, 0, 0, time.UTC),
		Actor:      "system",
		ChangeType: "update",
		Changes: []ConfigChange{
			{Key: "rate_limit.max", OldValue: 100, NewValue: 200},
		},
	}

	data, err := json.Marshal(original)
	require.NoError(t, err)

	var decoded ConfigHistoryEntry
	require.NoError(t, json.Unmarshal(data, &decoded))

	assert.Equal(t, original.Actor, decoded.Actor)
	assert.Equal(t, original.ChangeType, decoded.ChangeType)
	assert.Equal(t, original.Timestamp, decoded.Timestamp)
	require.Len(t, decoded.Changes, 1)
}

func TestConfigHistoryEntry_JSONTagNames(t *testing.T) {
	t.Parallel()

	entry := ConfigHistoryEntry{
		Timestamp:  time.Now().UTC(),
		Actor:      "a",
		ChangeType: "c",
	}

	data, err := json.Marshal(entry)
	require.NoError(t, err)

	raw := make(map[string]json.RawMessage)
	require.NoError(t, json.Unmarshal(data, &raw))

	for _, key := range []string{"timestamp", "actor", "changeType"} {
		_, exists := raw[key]
		assert.True(t, exists, "expected JSON key %q", key)
	}
}

func TestConfigHistoryResponse_JSONRoundTrip(t *testing.T) {
	t.Parallel()

	original := ConfigHistoryResponse{
		Items: []ConfigHistoryEntry{
			{Timestamp: time.Now().UTC(), Actor: "admin", ChangeType: "update"},
		},
	}

	data, err := json.Marshal(original)
	require.NoError(t, err)

	var decoded ConfigHistoryResponse
	require.NoError(t, json.Unmarshal(data, &decoded))

	require.Len(t, decoded.Items, 1)
	assert.Equal(t, "admin", decoded.Items[0].Actor)
}

func TestConfigHistoryResponse_JSONTagNames(t *testing.T) {
	t.Parallel()

	resp := ConfigHistoryResponse{
		Items: []ConfigHistoryEntry{},
	}

	data, err := json.Marshal(resp)
	require.NoError(t, err)

	raw := make(map[string]json.RawMessage)
	require.NoError(t, json.Unmarshal(data, &raw))

	_, exists := raw["items"]
	assert.True(t, exists, "expected JSON key 'items'")
}
