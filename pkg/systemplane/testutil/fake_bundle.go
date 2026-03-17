// Copyright 2025 Lerian Studio.

package testutil

import (
	"context"
	"sync"

	"github.com/LerianStudio/matcher/pkg/systemplane/domain"
	"github.com/LerianStudio/matcher/pkg/systemplane/ports"
)

// Compile-time interface checks.
var (
	_ domain.RuntimeBundle = (*FakeBundle)(nil)
	_ ports.BundleFactory  = (*FakeBundleFactory)(nil)
)

// FakeBundle is a stub RuntimeBundle that tracks whether Close was called.
type FakeBundle struct {
	Closed   bool
	CloseErr error // configurable error returned by Close
}

// Close marks the bundle as closed and returns the configured CloseErr.
func (b *FakeBundle) Close(_ context.Context) error {
	b.Closed = true
	return b.CloseErr
}

// FakeBundleFactory is a configurable BundleFactory for testing. By default
// it returns a new FakeBundle on every Build call. Override BuildFn to
// customise behavior.
type FakeBundleFactory struct {
	mu      sync.Mutex
	BuildFn func(ctx context.Context, snap domain.Snapshot) (domain.RuntimeBundle, error)
	Calls   int // number of Build invocations
}

// NewFakeBundleFactory creates a factory that returns a fresh FakeBundle for
// every Build call.
func NewFakeBundleFactory() *FakeBundleFactory {
	return &FakeBundleFactory{
		BuildFn: func(_ context.Context, _ domain.Snapshot) (domain.RuntimeBundle, error) {
			return &FakeBundle{}, nil
		},
	}
}

// Build delegates to BuildFn and increments the call counter.
func (f *FakeBundleFactory) Build(ctx context.Context, snap domain.Snapshot) (domain.RuntimeBundle, error) {
	f.mu.Lock()
	f.Calls++
	fn := f.BuildFn
	f.mu.Unlock()

	return fn(ctx, snap)
}

// SetError configures the factory to return the given error (and a nil bundle)
// on every subsequent Build call.
func (f *FakeBundleFactory) SetError(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.BuildFn = func(_ context.Context, _ domain.Snapshot) (domain.RuntimeBundle, error) {
		return nil, err
	}
}

// CallCount returns the number of Build invocations observed so far.
func (f *FakeBundleFactory) CallCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.Calls
}
