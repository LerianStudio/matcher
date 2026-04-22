// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

// Package extractionpoller provides a hot-reloadable wrapper around
// the discovery worker's extraction poller. The concrete Poller stores
// the active Runner behind an atomic.Pointer so bootstrap can swap
// the runner in response to config changes (poll interval, timeout)
// without restarting the service or mutating an in-flight poll
// goroutine.
//
// Design note: prior to T-015 this was modelled as an interface
// (ExtractionJobPoller port) plus a dynamic wrapper that rebuilt a
// fresh worker.ExtractionPoller on every PollUntilComplete call. The
// port had exactly one production implementation — hot-reload was
// expressed as "substitutable implementation" when the reality is
// "swap the internal state". This package collapses the port + wrapper
// into a concrete type whose backend is atomically swappable.
//
// The Runner interface keeps the production worker.ExtractionPoller
// out of this package's import graph so integration tests in
// services/worker that transitively pull in this type via
// services/command do not form a cycle.
package extractionpoller

import (
	"context"
	"errors"
	"sync/atomic"
	"time"

	"github.com/google/uuid"

	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"

	discoveryRepos "github.com/LerianStudio/matcher/internal/discovery/domain/repositories"
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

// ErrPollerUnavailable indicates the Poller has no configured runner
// and no resolver wired. PollUntilComplete surfaces this via onFailed
// when present.
var ErrPollerUnavailable = errors.New("extraction poller: no runner configured")

// Runner is the inner extraction-polling backend that Poller wraps.
// discoveryWorker.ExtractionPoller satisfies this interface by method
// match; keeping the dependency as an interface here avoids an import
// cycle between extractionpoller and discovery/services/worker.
type Runner interface {
	PollUntilComplete(
		ctx context.Context,
		extractionID uuid.UUID,
		onComplete func(ctx context.Context, resultPath string) error,
		onFailed func(ctx context.Context, errMsg string),
	)
}

// RunnerConfig captures the knobs that control a Runner. Mirrors
// discoveryWorker.ExtractionPollerConfig so bootstrap can map directly
// from runtime configuration; kept as a separate value type so this
// package does not import the worker package.
type RunnerConfig struct {
	PollInterval time.Duration
	Timeout      time.Duration
}

// RunnerFactory builds a Runner from fetcherClient + extractionRepo +
// config + logger. Bootstrap supplies a closure that calls
// discoveryWorker.NewExtractionPoller, keeping the production dep out
// of this package's import graph.
type RunnerFactory func(
	fetcherClient sharedPorts.FetcherClient,
	extractionRepo discoveryRepos.ExtractionRepository,
	cfg RunnerConfig,
	logger libLog.Logger,
) (Runner, error)

// ConfigResolver returns the current extraction poller config.
// Bootstrap wires this via the configGetter so runtime config changes
// (poll interval, timeout) apply on the next PollUntilComplete call
// without needing an explicit Reload.
type ConfigResolver func() RunnerConfig

// state is what Poller.state swaps atomically. runner is the active
// Runner; lastConfig is the config value that built it, used to skip
// rebuilds when the resolver reports the same config.
type state struct {
	runner     Runner
	lastConfig RunnerConfig
	hasConfig  bool
}

// Poller is the concrete, hot-reloadable extraction poller. It wraps
// a Runner behind an atomic.Pointer so reload and PollUntilComplete
// never race.
type Poller struct {
	state          atomic.Pointer[state]
	fetcherClient  sharedPorts.FetcherClient
	extractionRepo discoveryRepos.ExtractionRepository
	resolver       ConfigResolver
	factory        RunnerFactory
	logger         libLog.Logger
}

// NewPoller constructs a hot-reloadable extraction poller. The
// fetcherClient and extractionRepo are long-lived; the resolver
// returns the current config on demand so the underlying Runner
// can be rebuilt when config changes. factory is the bootstrap-provided
// closure that builds a Runner (typically wrapping
// discoveryWorker.NewExtractionPoller).
//
// Pass nil for resolver to disable lazy rebuild — callers must then
// drive reloads explicitly via Reload. Pass nil for factory to
// disable resolver-driven rebuild entirely (Reload still works).
func NewPoller(
	fetcherClient sharedPorts.FetcherClient,
	extractionRepo discoveryRepos.ExtractionRepository,
	resolver ConfigResolver,
	factory RunnerFactory,
	logger libLog.Logger,
) *Poller {
	poller := &Poller{
		fetcherClient:  fetcherClient,
		extractionRepo: extractionRepo,
		resolver:       resolver,
		factory:        factory,
		logger:         logger,
	}
	poller.state.Store(&state{})

	return poller
}

// Reload atomically installs runner as the active Runner, tagging it
// with cfg as the cache key. Subsequent resolver-driven rebuilds skip
// reconstruction when cfg matches. Safe to call from multiple
// goroutines.
func (poller *Poller) Reload(runner Runner, cfg RunnerConfig) {
	if poller == nil {
		return
	}

	for {
		current := poller.state.Load()

		next := &state{
			runner:     runner,
			lastConfig: cfg,
			hasConfig:  true,
		}

		if poller.state.CompareAndSwap(current, next) {
			return
		}
	}
}

// PollUntilComplete delegates to the current Runner. If the resolver
// is wired and reports a new config, a fresh Runner is constructed
// via factory and atomically installed before the call.
func (poller *Poller) PollUntilComplete(
	ctx context.Context,
	extractionID uuid.UUID,
	onComplete func(ctx context.Context, resultPath string) error,
	onFailed func(ctx context.Context, errMsg string),
) {
	if poller == nil {
		if onFailed != nil {
			onFailed(ctx, ErrPollerUnavailable.Error())
		}

		return
	}

	runner, err := poller.current()
	if err != nil {
		if onFailed != nil {
			onFailed(ctx, err.Error())
		}

		return
	}

	runner.PollUntilComplete(ctx, extractionID, onComplete, onFailed)
}

// current resolves the Runner, rebuilding via resolver+factory when
// config changed. Returns the installed Runner or an error when no
// Runner is available and the resolver/factory cannot build one.
func (poller *Poller) current() (Runner, error) {
	if poller == nil {
		return nil, ErrPollerUnavailable
	}

	if poller.resolver != nil && poller.factory != nil {
		poller.resolveAndMaybeSwap()
	}

	snapshot := poller.state.Load()
	if snapshot == nil || snapshot.runner == nil {
		return nil, ErrPollerUnavailable
	}

	return snapshot.runner, nil
}

// resolveAndMaybeSwap consults the resolver; if it reports a new
// config, a fresh Runner is built via factory and CAS'd in. Resolver
// or factory errors leave the last good Runner in place.
func (poller *Poller) resolveAndMaybeSwap() {
	cfg := poller.resolver()
	current := poller.state.Load()

	if current != nil && current.hasConfig && current.runner != nil && current.lastConfig == cfg {
		return
	}

	runner, err := poller.factory(poller.fetcherClient, poller.extractionRepo, cfg, poller.logger)
	if err != nil {
		return
	}

	for {
		next := &state{
			runner:     runner,
			lastConfig: cfg,
			hasConfig:  true,
		}

		if poller.state.CompareAndSwap(current, next) {
			return
		}

		current = poller.state.Load()

		// Re-check: if another goroutine already installed the same cfg,
		// abandon our build.
		if current != nil && current.hasConfig && current.runner != nil && current.lastConfig == cfg {
			return
		}
	}
}
