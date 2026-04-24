// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package bootstrap

import "strings"

const (
	envDevShortName = "dev"
	envLocalName    = "local"
	envTestName     = "test"
)

func allowInsecureObjectStorageEndpoint(cfg *Config) bool {
	if cfg == nil || !cfg.ObjectStorage.AllowInsecure {
		return false
	}

	return isAllowedInsecureObjectStorageEnvironment(cfg.App.EnvName)
}

func isAllowedInsecureObjectStorageEnvironment(envName string) bool {
	switch strings.ToLower(strings.TrimSpace(envName)) {
	case defaultEnvName, envDevShortName, envLocalName, envTestName:
		return true
	default:
		return false
	}
}
