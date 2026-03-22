// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package bootstrap

import (
	"strings"

	libZap "github.com/LerianStudio/lib-commons/v4/commons/zap"
)

const defaultLoggerLevel = "info"

// ResolveLoggerEnvironment maps app environment names to lib-commons zap environments.
func ResolveLoggerEnvironment(envName string) libZap.Environment {
	switch strings.ToLower(strings.TrimSpace(envName)) {
	case envProduction:
		return libZap.EnvironmentProduction
	case "staging":
		return libZap.EnvironmentStaging
	default:
		return libZap.EnvironmentDevelopment
	}
}

// IsProductionEnvironment reports whether envName should be treated as production.
func IsProductionEnvironment(envName string) bool {
	return strings.EqualFold(strings.TrimSpace(envName), envProduction)
}

// ResolveLoggerLevel validates and normalizes logger level values.
// Invalid or empty values fall back to "info".
func ResolveLoggerLevel(level string) string {
	switch strings.ToLower(strings.TrimSpace(level)) {
	case "debug", "info", "warn", "error", "fatal":
		return strings.ToLower(strings.TrimSpace(level))
	default:
		return defaultLoggerLevel
	}
}
