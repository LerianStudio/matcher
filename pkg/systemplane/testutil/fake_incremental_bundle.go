// Copyright 2025 Lerian Studio.

package testutil

import (
	"context"
	"errors"
	"sync"

	"github.com/LerianStudio/matcher/pkg/systemplane/domain"
	"github.com/LerianStudio/matcher/pkg/systemplane/ports"
)

// Compile-time interface checks.
var (
	_                                ports.BundleFactory            = (*FakeIncrementalBundleFactory)(nil)
	_                                ports.IncrementalBundleFactory = (*FakeIncrementalBundleFactory)(nil)
	errIncrementalBuildNotConfigured                                = errors.New("incremental build not configured")
)

// FakeIncrementalBundleFactory implements both BundleFactory and
// IncrementalBundleFactory. It embeds FakeBundleFactory for full-build
// behavior and adds configurable IncrementalBuildFunc for incremental builds.
type FakeIncrementalBundleFactory struct {
	FakeBundleFactory // embed existing full-build factory

	mu                   sync.Mutex
	IncrementalBuildFunc func(ctx context.Context, snap domain.Snapshot,
		previous domain.RuntimeBundle, prevSnap domain.Snapshot) (domain.RuntimeBundle, error)
	IncrementalCalls int // number of BuildIncremental invocations
}

// NewFakeIncrementalBundleFactory creates a factory that returns a fresh
// FakeBundle for both Build and BuildIncremental calls.
func NewFakeIncrementalBundleFactory() *FakeIncrementalBundleFactory {
	return &FakeIncrementalBundleFactory{
		FakeBundleFactory: *NewFakeBundleFactory(),
		IncrementalBuildFunc: func(_ context.Context, _ domain.Snapshot,
			_ domain.RuntimeBundle, _ domain.Snapshot,
		) (domain.RuntimeBundle, error) {
			return &FakeBundle{}, nil
		},
	}
}

// BuildIncremental delegates to IncrementalBuildFunc if set, otherwise returns
// an error.
func (factory *FakeIncrementalBundleFactory) BuildIncremental(
	ctx context.Context,
	snap domain.Snapshot,
	previous domain.RuntimeBundle,
	prevSnap domain.Snapshot,
) (domain.RuntimeBundle, error) {
	factory.mu.Lock()
	factory.IncrementalCalls++
	incrementalBuildFunc := factory.IncrementalBuildFunc
	factory.mu.Unlock()

	if incrementalBuildFunc != nil {
		return incrementalBuildFunc(ctx, snap, previous, prevSnap)
	}

	return nil, errIncrementalBuildNotConfigured
}

// IncrementalCallCount returns the number of BuildIncremental invocations.
func (factory *FakeIncrementalBundleFactory) IncrementalCallCount() int {
	factory.mu.Lock()
	defer factory.mu.Unlock()

	return factory.IncrementalCalls
}
