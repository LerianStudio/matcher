// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package bootstrap

import (
	"reflect"
	"strconv"
	"strings"
)

// configFieldDef is the static definition of a config field.
// CurrentValue is populated at runtime from the live config.
type configFieldDef struct { //nolint:unused // Used by buildConfigSchema (reachable only from unit tests with build tag: unit).
	Key          string
	Label        string
	Type         string // "string", "int", "bool"
	DefaultValue any
	EnvVar       string
	Constraints  []string
	Description  string
	Section      string
	Secret       bool // if true, value is redacted in API responses
}

// buildConfigSchema returns the schema definitions for all config fields,
// derived via reflection from the Config struct's tags. Descriptions,
// labels, and constraints come from the companion maps above.
func buildConfigSchema() []configFieldDef { //nolint:unused // Called from unit tests (build tag: unit).
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
func collectSchemaFields(defs *[]configFieldDef, t reflect.Type, section string) { //nolint:unused // Called from buildConfigSchema (reachable only from unit tests with build tag: unit).
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
			Key:          key,
			Type:         goKindToSchemaType(field.Type.Kind()),
			DefaultValue: parseDefaultValue(field),
			EnvVar:       extractEnvVar(field),
			Section:      section,
			Secret:       isSensitiveKey(key),
			Description:  fieldDescriptions[key],
			Label:        fieldLabels[key],
			Constraints:  fieldConstraints[key],
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
func goKindToSchemaType(k reflect.Kind) string { //nolint:unused // Called from collectSchemaFields (reachable only from unit tests with build tag: unit).
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
func extractEnvVar(field reflect.StructField) string { //nolint:unused // Called from collectSchemaFields (reachable only from unit tests with build tag: unit).
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
func parseDefaultValue(field reflect.StructField) any { //nolint:unused // Called from collectSchemaFields (reachable only from unit tests with build tag: unit).
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
func labelFromTag(tag string) string { //nolint:unused // Called from collectSchemaFields (reachable only from unit tests with build tag: unit).
	parts := strings.Split(tag, "_")
	for i, p := range parts {
		if p == "" {
			continue
		}

		parts[i] = strings.ToUpper(p[:1]) + p[1:]
	}

	return strings.Join(parts, " ")
}

// resolveConfigValue resolves a dotted config key (e.g. "app.log_level")
// to its current value by walking the Config struct using mapstructure tags.
// This is used by the systemplane seed process to extract current values.
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
