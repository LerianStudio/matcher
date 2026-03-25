// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package bootstrap

import (
	"context"
	"fmt"

	libLog "github.com/LerianStudio/lib-commons/v4/commons/log"
	libZap "github.com/LerianStudio/lib-commons/v4/commons/zap"

	"github.com/LerianStudio/matcher/internal/shared/constants"
)

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
		Level:  resolvedLevel,
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

func syncRuntimeLogger(ctx context.Context, logger libLog.Logger, cfg *Config, bundle *MatcherBundle) error {
	swappable, ok := logger.(*SwappableLogger)
	if !ok {
		return nil
	}

	if cfg == nil {
		swapBundleLogger(swappable, bundle)

		return nil
	}

	resolvedLevel := ResolveLoggerLevel(cfg.App.LogLevel)
	if hasBundleLoggerLevel(bundle, resolvedLevel) {
		swappable.Swap(bundle.Logger.Logger)

		return nil
	}

	runtimeBundle, err := buildLoggerBundle(cfg.App.EnvName, cfg.App.LogLevel)
	if err != nil {
		return err
	}

	replaceBundleLogger(ctx, bundle, runtimeBundle)

	swappable.Swap(runtimeBundle.Logger)

	return nil
}

func swapBundleLogger(swappable *SwappableLogger, bundle *MatcherBundle) {
	if bundle != nil && bundle.Logger != nil {
		swappable.Swap(bundle.Logger.Logger)
	}
}

func hasBundleLoggerLevel(bundle *MatcherBundle, expectedLevel string) bool {
	return bundle != nil && bundle.Logger != nil && bundle.Logger.Level == expectedLevel
}

func replaceBundleLogger(ctx context.Context, bundle *MatcherBundle, runtimeBundle *LoggerBundle) {
	if bundle == nil {
		return
	}

	previous := bundle.Logger
	bundle.Logger = runtimeBundle
	bundle.ownsLogger = true

	if previous == nil || previous.Logger == nil || previous.Logger == runtimeBundle.Logger {
		return
	}

	if ctx != nil {
		_ = previous.Logger.Sync(ctx)
	}
}
