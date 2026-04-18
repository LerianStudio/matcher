// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package bootstrap

import (
	"strings"

	libZap "github.com/LerianStudio/lib-commons/v5/commons/zap"
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

// isLocalDevelopmentEnvironment reports whether envName is a local development
// environment where insecure HTTP may be acceptable (e.g., for tenant-manager
// communication over localhost). Staging, pre-production, and other real
// deployment environments must use HTTPS.
func isLocalDevelopmentEnvironment(envName string) bool {
	normalized := strings.ToLower(strings.TrimSpace(envName))
	return normalized == "development" || normalized == "dev" || normalized == ""
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
