//go:build unit

// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package bootstrap

import (
	"bufio"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"

	spBootstrap "github.com/LerianStudio/lib-commons/v4/commons/systemplane/bootstrap"
	"github.com/LerianStudio/lib-commons/v4/commons/systemplane/domain"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConfigMapExample_SyncWithNonHotReloadEnvVars(t *testing.T) {
	t.Parallel()

	actualVars, err := readConfigMapExampleEnvVars(filepath.Join("..", "..", "config", ".config-map.example"))
	require.NoError(t, err)

	expectedVars := expectedNonHotReloadEnvVars(t)

	var missing []string
	for key := range expectedVars {
		if !actualVars[key] {
			missing = append(missing, key)
		}
	}

	var stale []string
	for key := range actualVars {
		if !expectedVars[key] {
			stale = append(stale, key)
		}
	}

	sort.Strings(missing)
	sort.Strings(stale)

	assert.Empty(t, missing,
		"config/.config-map.example is missing non-hot-reload env vars: %v", missing)
	assert.Empty(t, stale,
		"config/.config-map.example contains env vars managed by systemplane hot reload or unknown vars: %v", stale)
}

func expectedNonHotReloadEnvVars(t *testing.T) map[string]bool {
	t.Helper()

	keyToEnv := collectConfigKeyToEnvMap(reflect.TypeOf(Config{}), "")
	expected := make(map[string]bool)

	for _, def := range matcherKeyDefs() {
		if def.ApplyBehavior != domain.ApplyBootstrapOnly {
			continue
		}

		envName, ok := keyToEnv[def.Key]
		require.Truef(t, ok, "missing env mapping for bootstrap-only config key %q", def.Key)
		expected[envName] = true
	}

	for _, key := range []string{
		"MULTI_TENANT_INFRA_ENABLED",
		spBootstrap.EnvBackend,
		spBootstrap.EnvPostgresDSN,
		spBootstrap.EnvPostgresSchema,
		spBootstrap.EnvPostgresEntriesTable,
		spBootstrap.EnvPostgresHistoryTable,
		spBootstrap.EnvPostgresRevisionTable,
		spBootstrap.EnvPostgresNotifyChannel,
		spBootstrap.EnvMongoURI,
		spBootstrap.EnvMongoDatabase,
		spBootstrap.EnvMongoEntriesCollection,
		spBootstrap.EnvMongoHistoryCollection,
		spBootstrap.EnvMongoWatchMode,
		spBootstrap.EnvMongoPollIntervalSec,
		"SYSTEMPLANE_SECRET_MASTER_KEY",
	} {
		expected[key] = true
	}

	return expected
}

func collectConfigKeyToEnvMap(structType reflect.Type, prefix string) map[string]string {
	for structType.Kind() == reflect.Pointer {
		structType = structType.Elem()
	}

	if structType.Kind() != reflect.Struct {
		return nil
	}

	result := make(map[string]string)

	for i := range structType.NumField() {
		field := structType.Field(i)
		if !field.IsExported() {
			continue
		}

		mapKey := strings.TrimSpace(field.Tag.Get("mapstructure"))
		if mapKey == "-" {
			continue
		}

		envTag := strings.TrimSpace(field.Tag.Get("env"))
		if envTag != "" {
			envName := strings.TrimSpace(strings.SplitN(envTag, ",", 2)[0])
			if envName == "" || mapKey == "" {
				continue
			}

			fullKey := mapKey
			if prefix != "" {
				fullKey = prefix + "." + mapKey
			}

			result[fullKey] = envName
			continue
		}

		if field.Type.Kind() != reflect.Struct || mapKey == "" {
			continue
		}

		nextPrefix := mapKey
		if prefix != "" {
			nextPrefix = prefix + "." + mapKey
		}

		for key, envName := range collectConfigKeyToEnvMap(field.Type, nextPrefix) {
			result[key] = envName
		}
	}

	return result
}

func readConfigMapExampleEnvVars(path string) (map[string]bool, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = file.Close()
	}()

	vars := make(map[string]bool)
	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		if strings.HasPrefix(line, "#") {
			line = strings.TrimSpace(strings.TrimPrefix(line, "#"))
		}

		name, _, found := strings.Cut(line, "=")
		if !found {
			continue
		}

		name = strings.TrimSpace(name)
		if name == "" || !isAllCapsEnvVar(name) {
			continue
		}

		vars[name] = true
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	return vars, nil
}

func isAllCapsEnvVar(value string) bool {
	for _, r := range value {
		if (r < 'A' || r > 'Z') && (r < '0' || r > '9') && r != '_' {
			return false
		}
	}

	return value != ""
}
