// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package bootstrap

import (
	"os"
	"reflect"
	"strings"

	"github.com/LerianStudio/lib-commons/v4/commons/systemplane/domain"
)

const (
	maxRestoreDepth     = 10
	envTagSplitPartsMax = 2
)

var configKeyAliases = map[string]string{
	"cors.allowed_origins":    "server.cors_allowed_origins",
	"cors.allowed_methods":    "server.cors_allowed_methods",
	"cors.allowed_headers":    "server.cors_allowed_headers",
	"postgres.max_open_conns": "postgres.max_open_connections",
	"postgres.max_idle_conns": "postgres.max_idle_connections",
	"redis.min_idle_conns":    "redis.min_idle_conn",
	"rabbitmq.url":            "rabbitmq.uri",
}

// reverseConfigKeyAliases maps legacy keys to canonical keys (built from configKeyAliases at init).
var reverseConfigKeyAliases = func() map[string]string {
	m := make(map[string]string, len(configKeyAliases))
	for canonical, legacy := range configKeyAliases {
		m[legacy] = canonical
	}

	return m
}()

func legacyConfigKey(key string) (string, bool) {
	legacyKey, ok := configKeyAliases[key]

	return legacyKey, ok
}

func canonicalConfigKey(key string) (string, bool) {
	canonicalKey, ok := reverseConfigKeyAliases[key]

	return canonicalKey, ok
}

func resolveSnapshotConfigValue(snap domain.Snapshot, key string) (any, bool) {
	if value, ok := resolveSnapshotValueByPreference(snap, key, settingKeyPrefersSettings(key)); ok {
		return value, true
	}

	legacyKey, ok := legacyConfigKey(key)
	if !ok {
		return nil, false
	}

	return resolveSnapshotValueByPreference(snap, legacyKey, settingKeyPrefersSettings(legacyKey))
}

func resolveSnapshotValueByPreference(snap domain.Snapshot, key string, preferSettings bool) (any, bool) {
	if preferSettings {
		if value, ok := snapshotGlobalSettingValueByKey(snap, key); ok {
			return value, true
		}

		// Keep upgrade compatibility for keys that were historically persisted as
		// configs before being reclassified as settings.
		return snapshotConfigValueByKey(snap, key)
	}

	if value, ok := snapshotConfigValueByKey(snap, key); ok {
		return value, true
	}

	return snapshotGlobalSettingValueByKey(snap, key)
}

func settingKeyPrefersSettings(key string) bool {
	kind, ok := matcherSystemplaneKeyKind(key)
	if !ok {
		return false
	}

	return kind == domain.KindSetting
}

func snapshotConfigValueByKey(snap domain.Snapshot, key string) (any, bool) {
	if strings.TrimSpace(key) == "" || snap.Configs == nil {
		return nil, false
	}

	effectiveValue, ok := snap.Configs[key]
	if !ok {
		return nil, false
	}

	return effectiveValue.Value, true
}

func snapshotGlobalSettingValueByKey(snap domain.Snapshot, key string) (any, bool) {
	if strings.TrimSpace(key) == "" || snap.GlobalSettings == nil {
		return nil, false
	}

	effectiveValue, ok := snap.GlobalSettings[key]
	if !ok {
		return nil, false
	}

	return effectiveValue.Value, true
}

func restoreZeroedFields(dst, snapshot *Config) {
	if dst == nil || snapshot == nil {
		return
	}

	restoreZeroedFieldsRecursive(reflect.ValueOf(dst).Elem(), reflect.ValueOf(snapshot).Elem(), 0)
}

func overlayExplicitEnvOverrides(dst, source *Config) {
	if dst == nil || source == nil {
		return
	}

	overlayExplicitEnvOverridesRecursive(reflect.ValueOf(dst).Elem(), reflect.ValueOf(source).Elem(), 0)
}

func restoreZeroedFieldsRecursive(dst, snapshot reflect.Value, depth int) {
	if depth > maxRestoreDepth {
		return
	}

	dstType := dst.Type()
	for i := range dstType.NumField() {
		field := dstType.Field(i)
		if !field.IsExported() {
			continue
		}

		if field.Tag.Get("mapstructure") == "-" {
			continue
		}

		dstField := dst.Field(i)

		snapField := snapshot.Field(i)
		if field.Type.Kind() == reflect.Struct {
			restoreZeroedFieldsRecursive(dstField, snapField, depth+1)
			continue
		}

		if dstField.IsZero() && !snapField.IsZero() && !hasExplicitEnvOverride(field) {
			dstField.Set(snapField)
		}
	}
}

func overlayExplicitEnvOverridesRecursive(dst, source reflect.Value, depth int) {
	if depth > maxRestoreDepth {
		return
	}

	dstType := dst.Type()
	for i := range dstType.NumField() {
		field := dstType.Field(i)
		if !field.IsExported() {
			continue
		}

		if field.Tag.Get("mapstructure") == "-" {
			continue
		}

		dstField := dst.Field(i)

		sourceField := source.Field(i)
		if field.Type.Kind() == reflect.Struct {
			overlayExplicitEnvOverridesRecursive(dstField, sourceField, depth+1)
			continue
		}

		if hasExplicitEnvOverride(field) {
			dstField.Set(sourceField)
		}
	}
}

func hasExplicitEnvOverride(field reflect.StructField) bool {
	envTag := strings.TrimSpace(field.Tag.Get("env"))
	if envTag == "" {
		return false
	}

	envName := strings.TrimSpace(strings.SplitN(envTag, ",", envTagSplitPartsMax)[0])
	if envName == "" {
		return false
	}

	_, exists := os.LookupEnv(envName)

	return exists
}

func hasExplicitEnvOverrideForKey(key string) bool {
	envVar, ok := resolveConfigEnvVar(key)
	if !ok {
		return false
	}

	_, exists := os.LookupEnv(envVar)

	return exists
}

func resolveConfigValue(cfg *Config, key string) (any, bool) {
	if cfg == nil || strings.TrimSpace(key) == "" {
		return nil, false
	}

	if alias, ok := legacyConfigKey(key); ok {
		key = alias
	}

	parts := strings.Split(key, ".")

	current, ok := derefPointerValue(reflect.ValueOf(cfg))
	if !ok {
		return nil, false
	}

	for idx, part := range parts {
		if current.Kind() != reflect.Struct {
			return nil, false
		}

		next, found := findMapstructureField(current, part)
		if !found {
			return nil, false
		}

		if idx == len(parts)-1 {
			return next.Interface(), true
		}

		current, ok = derefPointerValue(next)
		if !ok {
			return nil, false
		}
	}

	return nil, false
}

func resolveConfigEnvVar(key string) (string, bool) {
	if strings.TrimSpace(key) == "" {
		return "", false
	}

	if alias, ok := legacyConfigKey(key); ok {
		key = alias
	}

	parts := strings.Split(key, ".")
	current := reflect.TypeOf(Config{})

	for idx, part := range parts {
		if current.Kind() == reflect.Pointer {
			current = current.Elem()
		}

		if current.Kind() != reflect.Struct {
			return "", false
		}

		field, found := findMapstructureStructField(current, part)
		if !found {
			return "", false
		}

		if idx == len(parts)-1 {
			envTag := strings.TrimSpace(field.Tag.Get("env"))
			if envTag == "" {
				return "", false
			}

			envName := strings.TrimSpace(strings.SplitN(envTag, ",", envTagSplitPartsMax)[0])
			if envName == "" {
				return "", false
			}

			return envName, true
		}

		current = field.Type
	}

	return "", false
}

func derefPointerValue(value reflect.Value) (reflect.Value, bool) {
	if value.Kind() != reflect.Pointer {
		return value, true
	}

	if value.IsNil() {
		return reflect.Value{}, false
	}

	return value.Elem(), true
}

func findMapstructureField(current reflect.Value, part string) (reflect.Value, bool) {
	currentType := current.Type()
	for i := range currentType.NumField() {
		field := currentType.Field(i)
		if !field.IsExported() {
			continue
		}

		if field.Tag.Get("mapstructure") != part {
			continue
		}

		return current.Field(i), true
	}

	return reflect.Value{}, false
}

func findMapstructureStructField(currentType reflect.Type, part string) (reflect.StructField, bool) {
	for i := range currentType.NumField() {
		field := currentType.Field(i)
		if !field.IsExported() {
			continue
		}

		if field.Tag.Get("mapstructure") != part {
			continue
		}

		return field, true
	}

	return reflect.StructField{}, false
}
