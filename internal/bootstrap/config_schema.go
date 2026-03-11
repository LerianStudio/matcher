// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package bootstrap

import (
	"fmt"
	"os"
	"reflect"
	"strconv"
	"strings"
)

// configFieldDef is the static definition of a config field.
// CurrentValue is populated at runtime from the live config.
type configFieldDef struct {
	Key           string
	Label         string
	Type          string // "string", "int", "bool"
	DefaultValue  any
	HotReloadable bool
	EnvVar        string
	Constraints   []string
	Description   string
	Section       string
	Secret        bool // if true, value is redacted in API responses
}

// redactedValue is the placeholder shown for secret fields.
const redactedValue = "********"

// buildConfigSchema returns the schema definitions for all config fields,
// derived via reflection from the Config struct's tags. Descriptions,
// labels, and constraints come from the companion maps above.
func buildConfigSchema() []configFieldDef {
	var defs []configFieldDef

	t := reflect.TypeOf(Config{})

	for i := range t.NumField() {
		sectionField := t.Field(i)
		if !sectionField.IsExported() {
			continue
		}

		sectionTag := sectionField.Tag.Get("mapstructure")
		if sectionTag == "-" || sectionTag == "" {
			continue
		}

		// Only recurse into struct types (sub-configs).
		ft := sectionField.Type
		if ft.Kind() != reflect.Struct {
			continue
		}

		collectSchemaFields(&defs, ft, sectionTag)
	}

	return defs
}

// collectSchemaFields appends a configFieldDef for each leaf field in the given
// struct type, using the section prefix for dotted key construction.
func collectSchemaFields(defs *[]configFieldDef, t reflect.Type, section string) {
	for i := range t.NumField() {
		field := t.Field(i)
		if !field.IsExported() {
			continue
		}

		tag := field.Tag.Get("mapstructure")
		if tag == "-" || tag == "" {
			continue
		}

		key := section + "." + tag

		def := configFieldDef{
			Key:           key,
			Type:          goKindToSchemaType(field.Type.Kind()),
			DefaultValue:  parseDefaultValue(field),
			EnvVar:        extractEnvVar(field),
			Section:       section,
			Secret:        isSensitiveKey(key),
			HotReloadable: mutableConfigKeys[key],
			Description:   fieldDescriptions[key],
			Label:         fieldLabels[key],
			Constraints:   fieldConstraints[key],
		}

		// Fallback: generate label from the mapstructure tag if none is registered.
		if def.Label == "" {
			def.Label = labelFromTag(tag)
		}

		// Fallback: generate description from the label if none is registered.
		if def.Description == "" {
			def.Description = def.Label
		}

		*defs = append(*defs, def)
	}
}

// goKindToSchemaType maps a reflect.Kind to the schema type string.
func goKindToSchemaType(k reflect.Kind) string {
	switch k {
	case reflect.Bool:
		return "bool"
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return "int"
	default:
		return "string"
	}
}

// extractEnvVar reads the first token from the `env` struct tag.
func extractEnvVar(field reflect.StructField) string {
	env := field.Tag.Get("env")
	if env == "" {
		return ""
	}

	if idx := strings.IndexByte(env, ','); idx >= 0 {
		return env[:idx]
	}

	return env
}

// parseDefaultValue extracts the envDefault tag value and coerces it to the
// correct Go type (int/bool/string) to match the existing schema contract.
func parseDefaultValue(field reflect.StructField) any {
	raw := field.Tag.Get("envDefault")

	switch field.Type.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		if raw == "" {
			return 0
		}

		v, err := strconv.Atoi(raw)
		if err != nil {
			return 0
		}

		return v
	case reflect.Bool:
		return raw == "true"
	default:
		return raw
	}
}

// labelFromTag converts a snake_case mapstructure tag to a Title Case label.
// e.g. "primary_ssl_mode" → "Primary Ssl Mode".
func labelFromTag(tag string) string {
	parts := strings.Split(tag, "_")
	for i, p := range parts {
		if p == "" {
			continue
		}

		parts[i] = strings.ToUpper(p[:1]) + p[1:]
	}

	return strings.Join(parts, " ")
}

// isEnvOverridden returns true if the given env var is set in the process environment.
// It checks both legacy keys (e.g. LOG_LEVEL) and MATCHER-prefixed keys
// (e.g. MATCHER_APP_LOG_LEVEL) used by Viper.
func isEnvOverridden(envVar, key string) bool {
	if envVar == "" && key == "" {
		return false
	}

	if envVar != "" {
		if _, exists := os.LookupEnv(envVar); exists {
			return true
		}
	}

	if key == "" {
		return false
	}

	prefixedKey := "MATCHER_" + strings.ToUpper(strings.ReplaceAll(key, ".", "_"))
	_, exists := os.LookupEnv(prefixedKey)

	return exists
}

// buildSchemaResponse builds a ConfigSchemaResponse from the static schema
// and the current effective config snapshot (via viper for value resolution).
func buildSchemaResponse(cm *ConfigManager) ConfigSchemaResponse {
	defs := buildConfigSchema()
	sections := make(map[string][]ConfigFieldSchema, len(defs))
	visibleFields := 0

	for _, def := range defs {
		if def.Secret {
			continue
		}

		currentVal := resolveCurrentValue(cm, def)

		field := ConfigFieldSchema{
			Key:           def.Key,
			Label:         def.Label,
			Type:          def.Type,
			DefaultValue:  schemaValueString(def.DefaultValue),
			CurrentValue:  schemaValueString(currentVal),
			HotReloadable: def.HotReloadable,
			EnvOverride:   isEnvOverridden(def.EnvVar, def.Key),
			EnvVar:        def.EnvVar,
			Constraints:   def.Constraints,
			Description:   def.Description,
			Section:       def.Section,
		}

		sections[def.Section] = append(sections[def.Section], field)
		visibleFields++
	}

	return ConfigSchemaResponse{
		Sections:    sections,
		TotalFields: visibleFields,
	}
}

func schemaValueString(value any) string {
	if value == nil {
		return ""
	}

	return fmt.Sprint(value)
}

// resolveCurrentValue reads the current value from viper (which merges YAML + env).
// Secret fields are redacted.
func resolveCurrentValue(cm *ConfigManager, def configFieldDef) any {
	if def.Secret {
		return redactedValue
	}

	if val, ok := resolveCurrentConfigValue(cm, def.Key); ok {
		return redactIfSensitive(def.Key, val)
	}

	return redactIfSensitive(def.Key, def.DefaultValue)
}

// buildRedactedConfig builds a map representation of the current config
// with secrets replaced by redactedValue.
func buildRedactedConfig(cm *ConfigManager) map[string]any {
	defs := buildConfigSchema()
	result := make(map[string]any, len(defs))

	for _, def := range defs {
		if def.Secret {
			result[def.Key] = redactedValue

			continue
		}

		if val, ok := resolveCurrentConfigValue(cm, def.Key); ok {
			result[def.Key] = redactIfSensitive(def.Key, val)

			continue
		}

		result[def.Key] = redactIfSensitive(def.Key, def.DefaultValue)
	}

	return result
}

// buildEnvOverridesList returns the list of config keys currently overridden by env vars.
func buildEnvOverridesList() []string {
	defs := buildConfigSchema()
	overrides := make([]string, 0)

	for _, def := range defs {
		if isEnvOverridden(def.EnvVar, def.Key) {
			overrides = append(overrides, def.Key)
		}
	}

	return overrides
}

func resolveConfigValue(cfg *Config, key string) (any, bool) {
	if cfg == nil || strings.TrimSpace(key) == "" {
		return nil, false
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

func resolveCurrentConfigValue(cm *ConfigManager, key string) (any, bool) {
	if cm == nil {
		return nil, false
	}

	if cfg := cm.Get(); cfg != nil {
		if val, ok := resolveConfigValue(cfg, key); ok {
			return val, true
		}
	}

	if cm.viper == nil {
		return nil, false
	}

	val := cm.viper.Get(key)
	if val == nil {
		return nil, false
	}

	return val, true
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
