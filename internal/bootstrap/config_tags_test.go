// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package bootstrap

import (
	"reflect"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// snakeCaseRegexp validates that a mapstructure tag value is strict snake_case
// (lowercase letters, digits, underscores) or the special "-" skip value.
var snakeCaseRegexp = regexp.MustCompile(`^[a-z][a-z0-9]*(_[a-z0-9]+)*$`)

// TestConfigMapstructureTags_AllExportedFieldsTagged verifies every exported
// field across Config and all its sub-structs carries a `mapstructure` tag.
// This prevents silent regressions where new fields are added without the
// tag required for viper YAML unmarshalling.
func TestConfigMapstructureTags_AllExportedFieldsTagged(t *testing.T) {
	t.Parallel()

	var missing []string

	walkStructFieldsForTags(reflect.TypeOf(Config{}), "Config", func(path string, field reflect.StructField) {
		_, ok := field.Tag.Lookup("mapstructure")
		if !ok {
			missing = append(missing, path+"."+field.Name)
		}
	})

	assert.Empty(t, missing,
		"exported fields missing mapstructure tag — every field must be tagged for viper YAML unmarshalling:\n  %s",
		strings.Join(missing, "\n  "))
}

// TestConfigMapstructureTags_ValuesAreSnakeCase verifies every mapstructure
// tag value is either "-" (skip) or strict snake_case.
func TestConfigMapstructureTags_ValuesAreSnakeCase(t *testing.T) {
	t.Parallel()

	var violations []string

	walkStructFieldsForTags(reflect.TypeOf(Config{}), "Config", func(path string, field reflect.StructField) {
		tag, ok := field.Tag.Lookup("mapstructure")
		if !ok {
			return // missing tags are caught by the other test
		}

		if tag == "-" {
			return // skip marker is valid
		}

		if !snakeCaseRegexp.MatchString(tag) {
			violations = append(violations, path+"."+field.Name+" = "+tag)
		}
	})

	assert.Empty(t, violations,
		"mapstructure tag values must be snake_case:\n  %s",
		strings.Join(violations, "\n  "))
}

// TestConfigMapstructureTags_NoDuplicatesPerStruct verifies no two fields
// within the same struct level share the same mapstructure tag value.
// Duplicates would cause viper to silently overwrite one field during
// unmarshalling.
func TestConfigMapstructureTags_NoDuplicatesPerStruct(t *testing.T) {
	t.Parallel()

	var duplicates []string

	checkStructForDuplicates(reflect.TypeOf(Config{}), "Config", &duplicates)

	assert.Empty(t, duplicates,
		"duplicate mapstructure tag values within the same struct:\n  %s",
		strings.Join(duplicates, "\n  "))
}

// TestConfigMapstructureTags_FieldCount is a smoke test that ensures the
// total number of mapstructure-tagged fields matches expectations. Bump
// expectedCount when adding new config fields.
func TestConfigMapstructureTags_FieldCount(t *testing.T) {
	t.Parallel()

	// 23 fields on Config itself (21 sub-structs + ShutdownGracePeriod + Logger with "-")
	// + all leaf fields across sub-structs.
	// This is a lower-bound sanity check, not an exact count.
	const minimumExpected = 100

	var count int

	walkStructFieldsForTags(reflect.TypeOf(Config{}), "Config", func(_ string, field reflect.StructField) {
		if _, ok := field.Tag.Lookup("mapstructure"); ok {
			count++
		}
	})

	require.GreaterOrEqual(t, count, minimumExpected,
		"expected at least %d mapstructure-tagged fields, got %d — did you add new config fields without tags?",
		minimumExpected, count)

	t.Logf("total mapstructure-tagged fields: %d", count)
}

// walkStructFieldsForTags recursively visits every exported field in a struct type,
// including fields inside nested structs. The callback receives the parent
// path (e.g., "Config.Postgres") and the field descriptor.
func walkStructFieldsForTags(t reflect.Type, path string, fn func(path string, field reflect.StructField)) {
	for i := range t.NumField() {
		field := t.Field(i)
		if !field.IsExported() {
			continue
		}

		fn(path, field)

		// Recurse into nested struct fields (but not pointer-to-struct or
		// interface fields like Logger).
		ft := field.Type
		if ft.Kind() == reflect.Struct && ft != reflect.TypeOf(struct{}{}) {
			// Skip well-known non-config struct types (time.Duration, etc.)
			if ft.PkgPath() != "" && !strings.HasPrefix(ft.PkgPath(), "github.com/LerianStudio/matcher") {
				continue
			}

			walkStructFieldsForTags(ft, path+"."+field.Name, fn)
		}
	}
}

// checkStructForDuplicates checks a single struct level for duplicate
// mapstructure tag values and recurses into nested struct fields.
func checkStructForDuplicates(t reflect.Type, path string, duplicates *[]string) {
	seen := make(map[string]string) // tag value → field name

	for i := range t.NumField() {
		field := t.Field(i)
		if !field.IsExported() {
			continue
		}

		tag, ok := field.Tag.Lookup("mapstructure")
		if !ok || tag == "-" {
			continue
		}

		if prev, exists := seen[tag]; exists {
			*duplicates = append(*duplicates,
				path+": tag \""+tag+"\" used by both "+prev+" and "+field.Name)
		}

		seen[tag] = field.Name

		// Recurse into nested struct fields.
		ft := field.Type
		if ft.Kind() == reflect.Struct && ft.PkgPath() != "" &&
			strings.HasPrefix(ft.PkgPath(), "github.com/LerianStudio/matcher") {
			checkStructForDuplicates(ft, path+"."+field.Name, duplicates)
		}
	}
}
