//go:build unit

// Copyright 2025 Lerian Studio.

package domain

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testSnapshot() *Snapshot {
	return &Snapshot{
		Configs: map[string]EffectiveValue{
			"postgres.max_open_conns": {
				Key:      "postgres.max_open_conns",
				Value:    50,
				Default:  25,
				Override: 50,
				Source:   "global-override",
				Revision: Revision(3),
			},
		},
		GlobalSettings: map[string]EffectiveValue{
			"ui.locale": {
				Key:      "ui.locale",
				Value:    "en-US",
				Default:  "en-US",
				Override: nil,
				Source:   "default",
				Revision: Revision(2),
			},
		},
		TenantSettings: map[string]map[string]EffectiveValue{
			"tenant-1": {
				"ui.theme": {
					Key:      "ui.theme",
					Value:    "dark",
					Default:  "light",
					Override: "dark",
					Source:   "tenant-override",
					Revision: Revision(7),
				},
			},
		},
		Revision: Revision(7),
		BuiltAt:  time.Now().UTC(),
	}
}

func TestSnapshot_GetConfig_Found(t *testing.T) {
	t.Parallel()

	snap := testSnapshot()

	v, ok := snap.GetConfig("postgres.max_open_conns")

	require.True(t, ok)
	assert.Equal(t, 50, v.Value)
	assert.Equal(t, 25, v.Default)
	assert.Equal(t, 50, v.Override)
	assert.Equal(t, "global-override", v.Source)
}

func TestSnapshot_GetConfig_NotFound(t *testing.T) {
	t.Parallel()

	snap := testSnapshot()

	_, ok := snap.GetConfig("nonexistent.key")

	assert.False(t, ok)
}

func TestSnapshot_GetGlobalSetting_Found(t *testing.T) {
	t.Parallel()

	snap := testSnapshot()

	v, ok := snap.GetGlobalSetting("ui.locale")

	require.True(t, ok)
	assert.Equal(t, "en-US", v.Value)
	assert.Equal(t, "en-US", v.Default)
	assert.Nil(t, v.Override)
	assert.Equal(t, "default", v.Source)
}

func TestSnapshot_GetTenantSetting_Found(t *testing.T) {
	t.Parallel()

	snap := testSnapshot()

	v, ok := snap.GetTenantSetting("tenant-1", "ui.theme")

	require.True(t, ok)
	assert.Equal(t, "dark", v.Value)
	assert.Equal(t, "light", v.Default)
	assert.Equal(t, "dark", v.Override)
	assert.Equal(t, "tenant-override", v.Source)
}

func TestSnapshot_ConfigValue_Override(t *testing.T) {
	t.Parallel()

	snap := testSnapshot()

	val := snap.ConfigValue("postgres.max_open_conns", 10)

	assert.Equal(t, 50, val)
}

func TestSnapshot_ConfigValue_Fallback(t *testing.T) {
	t.Parallel()

	snap := testSnapshot()

	val := snap.ConfigValue("missing.key", 99)

	assert.Equal(t, 99, val)
}

func TestSnapshot_GetTenantSetting_NotFound(t *testing.T) {
	t.Parallel()

	snap := testSnapshot()

	_, ok := snap.GetTenantSetting("tenant-1", "missing.setting")

	assert.False(t, ok)
}

func TestSnapshot_GlobalSettingValue_Override(t *testing.T) {
	t.Parallel()

	snap := testSnapshot()

	val := snap.GlobalSettingValue("ui.locale", "fallback")

	assert.Equal(t, "en-US", val)
}

func TestSnapshot_NilReceiver_GetConfig(t *testing.T) {
	t.Parallel()

	var snap *Snapshot

	v, ok := snap.GetConfig("any.key")

	assert.False(t, ok)
	assert.Equal(t, EffectiveValue{}, v)
}

func TestSnapshot_TenantSettingValue_Override(t *testing.T) {
	t.Parallel()

	snap := testSnapshot()

	val := snap.TenantSettingValue("tenant-1", "ui.theme", "default")

	assert.Equal(t, "dark", val)
}

func TestSnapshot_TenantSettingValue_Fallback(t *testing.T) {
	t.Parallel()

	snap := testSnapshot()

	val := snap.TenantSettingValue("tenant-1", "missing.setting", "fallback")

	assert.Equal(t, "fallback", val)
}

func TestSnapshot_NilReceiver_GetGlobalSetting(t *testing.T) {
	t.Parallel()

	var snap *Snapshot

	v, ok := snap.GetGlobalSetting("any.key")

	assert.False(t, ok)
	assert.Equal(t, EffectiveValue{}, v)
}

func TestSnapshot_NilReceiver_ConfigValue(t *testing.T) {
	t.Parallel()

	var snap *Snapshot

	val := snap.ConfigValue("any.key", "safe")

	assert.Equal(t, "safe", val)
}

func TestSnapshot_NilReceiver_GetTenantSetting(t *testing.T) {
	t.Parallel()

	var snap *Snapshot

	v, ok := snap.GetTenantSetting("tenant-1", "any.key")

	assert.False(t, ok)
	assert.Equal(t, EffectiveValue{}, v)
}

func TestSnapshot_NilReceiver_GlobalSettingValue(t *testing.T) {
	t.Parallel()

	var snap *Snapshot

	val := snap.GlobalSettingValue("any.key", "safe")

	assert.Equal(t, "safe", val)
}

func TestSnapshot_NilReceiver_TenantSettingValue(t *testing.T) {
	t.Parallel()

	var snap *Snapshot

	val := snap.TenantSettingValue("tenant-1", "any.key", "safe")

	assert.Equal(t, "safe", val)
}

func TestSnapshot_NilMaps(t *testing.T) {
	t.Parallel()

	snap := &Snapshot{}

	_, configOK := snap.GetConfig("key")
	_, settingOK := snap.GetGlobalSetting("key")
	_, tenantSettingOK := snap.GetTenantSetting("tenant-1", "key")

	assert.False(t, configOK)
	assert.False(t, settingOK)
	assert.False(t, tenantSettingOK)
	assert.Equal(t, "fb", snap.ConfigValue("key", "fb"))
	assert.Equal(t, "fb", snap.GlobalSettingValue("key", "fb"))
	assert.Equal(t, "fb", snap.TenantSettingValue("tenant-1", "key", "fb"))
}
