// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package bootstrap

import (
	"fmt"

	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"
	libZap "github.com/LerianStudio/lib-commons/v5/commons/zap"

	"github.com/LerianStudio/matcher/internal/shared/constants"
)

// LoggerBundle wraps a rebuilt Logger produced by buildLoggerBundle.
type LoggerBundle struct {
	Logger libLog.Logger
}

func buildLoggerBundle(envName, level string) (*LoggerBundle, error) {
	resolvedLevel := ResolveLoggerLevel(level)
	env := ResolveLoggerEnvironment(envName)

	logger, err := libZap.New(libZap.Config{
		Environment:     env,
		Level:           resolvedLevel,
		OTelLibraryName: constants.ApplicationName,
	})
	if err != nil {
		return nil, fmt.Errorf("create logger: %w", err)
	}

	return &LoggerBundle{
		Logger: logger,
	}, nil
}

func buildLoggerFromConfig(cfg *Config) (libLog.Logger, error) {
	if cfg == nil {
		return nil, ErrConfigNil
	}

	bundle, err := buildLoggerBundle(cfg.App.EnvName, cfg.App.LogLevel)
	if err != nil {
		return nil, err
	}

	return bundle.Logger, nil
}
