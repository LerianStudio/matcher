// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package bootstrap

import (
	"os"
	"reflect"
	"strings"
)

const (
	maxRestoreDepth     = 10
	envTagSplitPartsMax = 2
)

func restoreZeroedFields(dst, snapshot *Config) {
	if dst == nil || snapshot == nil {
		return
	}

	restoreZeroedFieldsRecursive(reflect.ValueOf(dst).Elem(), reflect.ValueOf(snapshot).Elem(), 0)
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
