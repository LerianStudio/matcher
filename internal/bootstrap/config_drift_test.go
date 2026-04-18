// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package bootstrap

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"testing"
)

// TestDefaults_NoDriftBetweenSources ensures the 3 default-value sources stay aligned:
//  1. defaultConfig() — zero-value-overridden Config struct
//  2. matcherKeyDefs() — systemplane key registry with default values
//  3. envDefault struct tag strings on Config
//
// Drift causes silent config mismatches where operators see one value in
// systemplane/UI but the service uses a different fallback. This test is a
// hard gate: any mismatch between Config's mapstructure-path value and the
// corresponding systemplane default is reported.
//
// Keys only present in one source (e.g., bootstrap-only keys absent from
// matcherKeyDefs, or aggregated systemplane keys like "cors.allowed_origins"
// that do not share a dotted mapstructure path with the struct) are skipped
// — drift is only meaningful when both sources describe the same key.
func TestDefaults_NoDriftBetweenSources(t *testing.T) {
	t.Parallel()

	cfg := defaultConfig()
	if cfg == nil {
		t.Fatal("defaultConfig() returned nil")
	}

	keyDefs := matcherKeyDefs(cfg)
	bySpKey := make(map[string]any, len(keyDefs))

	for _, d := range keyDefs {
		bySpKey[d.key] = d.defaultValue
	}

	drifts := walkAndCompareDefaults(reflect.ValueOf(cfg).Elem(), reflect.TypeOf(*cfg), "", bySpKey)
	for _, d := range drifts {
		t.Errorf("drift key=%q: defaultConfig()=%v (%T), matcherKeyDefs()=%v (%T), envDefault=%q",
			d.Key, d.FromStruct, d.FromStruct, d.FromSpDef, d.FromSpDef, d.FromEnvTag)
	}
}

type driftRecord struct {
	Key        string
	FromStruct any
	FromSpDef  any
	FromEnvTag string
}

// walkAndCompareDefaults recursively walks the Config struct, joining mapstructure
// tags to form the full dotted key path, and compares each leaf field against
// the systemplane key registry.
func walkAndCompareDefaults(v reflect.Value, t reflect.Type, prefix string, sp map[string]any) []driftRecord {
	var drifts []driftRecord

	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		mapstructureTag := f.Tag.Get("mapstructure")
		envDefaultTag := f.Tag.Get("envDefault")

		// Skip fields explicitly excluded from mapstructure (e.g., Logger, ShutdownGracePeriod).
		if mapstructureTag == "-" {
			continue
		}

		fullKey := prefix
		if mapstructureTag != "" {
			if fullKey != "" {
				fullKey += "."
			}

			fullKey += mapstructureTag
		}

		if f.Type.Kind() == reflect.Struct {
			drifts = append(drifts, walkAndCompareDefaults(v.Field(i), f.Type, fullKey, sp)...)

			continue
		}

		if mapstructureTag == "" {
			continue
		}

		spVal, hasSp := sp[fullKey]
		if !hasSp {
			continue // bootstrap-only key, not in systemplane registry
		}

		structVal := v.Field(i).Interface()
		if !valuesEqual(structVal, spVal) {
			drifts = append(drifts, driftRecord{
				Key:        fullKey,
				FromStruct: structVal,
				FromSpDef:  spVal,
				FromEnvTag: envDefaultTag,
			})
		}
	}

	return drifts
}

// valuesEqual normalizes numeric types across int/int64/float64 so that
// matcherKeyDef's defaultValue (often int) can be compared to a Config field
// (which may be int, int64, or a typed numeric under the hood).
func valuesEqual(a, b any) bool {
	if reflect.DeepEqual(a, b) {
		return true
	}

	aInt, aOk := toInt64(a)
	bInt, bOk := toInt64(b)

	if aOk && bOk {
		return aInt == bInt
	}

	return false
}

func toInt64(v any) (int64, bool) {
	switch x := v.(type) {
	case int:
		return int64(x), true
	case int32:
		return int64(x), true
	case int64:
		return x, true
	case float64:
		return int64(x), true
	}

	return 0, false
}

// TestDefaults_EnvDefaultTagsMatchConstants is an optional stronger check:
// asserts each `envDefault:"100"` string actually matches the Go constant
// assigned in defaultConfig(). Catches cases like defaultRateLimitMax=100 but
// envDefault:"99".
//
// Fields without an envDefault tag are skipped (the tag is the authoritative
// source when it exists; fields that rely on the Go zero value don't need to
// appear in envDefault form).
func TestDefaults_EnvDefaultTagsMatchConstants(t *testing.T) {
	t.Parallel()

	cfg := defaultConfig()
	if cfg == nil {
		t.Fatal("defaultConfig() returned nil")
	}

	v := reflect.ValueOf(cfg).Elem()
	tType := reflect.TypeOf(*cfg)

	mismatches := walkEnvDefaults(v, tType, "")
	for _, m := range mismatches {
		t.Errorf("envDefault tag mismatch: field=%q envDefault=%q structValue=%v",
			m.Field, m.EnvTag, m.StructValue)
	}
}

type envDefaultMismatch struct {
	Field       string
	EnvTag      string
	StructValue any
}

func walkEnvDefaults(v reflect.Value, t reflect.Type, prefix string) []envDefaultMismatch {
	var mismatches []envDefaultMismatch

	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		envTag := f.Tag.Get("envDefault")
		mapstructureTag := f.Tag.Get("mapstructure")

		// Skip fields explicitly excluded from mapstructure (e.g., Logger, ShutdownGracePeriod).
		if mapstructureTag == "-" {
			continue
		}

		fieldPath := prefix
		if fieldPath != "" {
			fieldPath += "."
		}

		fieldPath += f.Name

		if f.Type.Kind() == reflect.Struct {
			mismatches = append(mismatches, walkEnvDefaults(v.Field(i), f.Type, fieldPath)...)

			continue
		}

		if envTag == "" {
			continue
		}

		structVal := v.Field(i).Interface()

		if envDefaultMatches(envTag, structVal) {
			continue
		}

		mismatches = append(mismatches, envDefaultMismatch{
			Field:       fieldPath,
			EnvTag:      envTag,
			StructValue: structVal,
		})
	}

	return mismatches
}

// envDefaultMatches returns true when the envDefault tag string faithfully
// represents the Go value populated in defaultConfig(). Handles numeric, bool
// and string coercions (including whitespace-trimmed string comparison).
func envDefaultMatches(envTag string, structVal any) bool {
	structStr := fmt.Sprintf("%v", structVal)
	if envTag == structStr {
		return true
	}

	if i64, err := strconv.ParseInt(envTag, 10, 64); err == nil {
		if sInt, ok := toInt64(structVal); ok && i64 == sInt {
			return true
		}
	}

	if b, err := strconv.ParseBool(envTag); err == nil {
		if sBool, ok := structVal.(bool); ok && b == sBool {
			return true
		}
	}

	return strings.TrimSpace(envTag) == strings.TrimSpace(structStr)
}
