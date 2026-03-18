// Copyright 2025 Lerian Studio.

// Package bootstrap wires systemplane backends from bootstrap configuration.
package bootstrap

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/LerianStudio/matcher/pkg/systemplane/domain"
	"github.com/LerianStudio/matcher/pkg/systemplane/ports"
)

// BackendResources holds the backend family created by the backend factory.
type BackendResources struct {
	Store      ports.Store
	History    ports.HistoryStore
	ChangeFeed ports.ChangeFeed
	Closer     io.Closer
}

// BackendFactory constructs the runtime backend family from the bootstrap
// configuration. The function is injected to avoid import cycles between the
// bootstrap and adapter packages.
type BackendFactory func(ctx context.Context, cfg *BootstrapConfig) (*BackendResources, error)

// backendFactories maps each supported BackendKind to its constructor.
// Entries are registered via RegisterBackendFactory at init time by the
// adapter packages (or by the wiring code that imports them).
var backendFactories = map[domain.BackendKind]BackendFactory{}

var (
	errNilBackendConfig         = errors.New("bootstrap backend: config is nil")
	errUnsupportedBackend       = errors.New("bootstrap backend: unsupported backend")
	errNilBackendResources      = errors.New("bootstrap backend: factory returned nil resources")
	errNilBackendStore          = errors.New("bootstrap backend: factory returned nil store")
	errNilBackendHistoryStore   = errors.New("bootstrap backend: factory returned nil history store")
	errNilBackendChangeFeed     = errors.New("bootstrap backend: factory returned nil change feed")
	errNilBackendCloser         = errors.New("bootstrap backend: factory returned nil closer")
	errInvalidBackendKind       = errors.New("bootstrap backend: invalid backend kind")
	errNilBackendFactory        = errors.New("bootstrap backend: factory is nil")
	errBackendAlreadyRegistered = errors.New("bootstrap backend: factory already registered")
)

// ResetBackendFactories clears all registered backend factories and init
// errors. This function exists only for test isolation and must not be called
// in production code.
func ResetBackendFactories() {
	backendFactories = map[domain.BackendKind]BackendFactory{}
	initErrors = nil
}

// initErrors collects errors from backend factory registrations that occur
// during init(). These are checked lazily in NewBackendFromConfig so that
// registration failures surface as actionable errors instead of panics.
var initErrors []error

// RecordInitError appends an error to the package-level init error list.
// It is intended to be called from init() functions in adapter packages when
// RegisterBackendFactory fails.
func RecordInitError(err error) {
	initErrors = append(initErrors, err)
}

// RegisterBackendFactory registers a BackendFactory for the given backend kind.
// It is intended to be called from adapter package init() functions or from
// the application wiring code that imports the adapter packages.
//
// Registration is single-write per backend kind. Duplicate or nil
// registrations are rejected to preserve bootstrap integrity.
func RegisterBackendFactory(kind domain.BackendKind, factory BackendFactory) error {
	if !kind.IsValid() {
		return fmt.Errorf("%w %q", errInvalidBackendKind, kind)
	}

	if factory == nil {
		return errNilBackendFactory
	}

	if _, exists := backendFactories[kind]; exists {
		return fmt.Errorf("%w %q", errBackendAlreadyRegistered, kind)
	}

	backendFactories[kind] = factory

	return nil
}

// NewBackendFromConfig creates the backend family based on the configured
// backend kind. It validates the config, applies defaults, and delegates to the
// registered BackendFactory for the configured backend.
//
// The caller is responsible for calling Closer.Close when the resources are no
// longer needed. Callers typically defer res.Closer.Close().
func NewBackendFromConfig(ctx context.Context, cfg *BootstrapConfig) (*BackendResources, error) {
	if len(initErrors) > 0 {
		return nil, fmt.Errorf("bootstrap backend: init registration errors: %w", errors.Join(initErrors...))
	}

	if cfg == nil {
		return nil, errNilBackendConfig
	}

	cfg.ApplyDefaults()

	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("bootstrap backend: %w", err)
	}

	factory, ok := backendFactories[cfg.Backend]
	if !ok {
		return nil, fmt.Errorf("%w %q (no factory registered)", errUnsupportedBackend, cfg.Backend)
	}

	resources, err := factory(ctx, cfg)
	if err != nil {
		return nil, err
	}

	if resources == nil {
		return nil, errNilBackendResources
	}

	if resources.Store == nil {
		return nil, errNilBackendStore
	}

	if resources.History == nil {
		return nil, errNilBackendHistoryStore
	}

	if resources.ChangeFeed == nil {
		return nil, errNilBackendChangeFeed
	}

	if resources.Closer == nil {
		return nil, errNilBackendCloser
	}

	return resources, nil
}
