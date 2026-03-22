//go:build unit

// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package bootstrap

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewActiveMatcherBundleState_ReturnsNonNil(t *testing.T) {
	t.Parallel()

	state := newActiveMatcherBundleState()

	require.NotNil(t, state)
}

func TestActiveMatcherBundleState_Current_InitiallyNil(t *testing.T) {
	t.Parallel()

	state := newActiveMatcherBundleState()

	assert.Nil(t, state.Current(), "initial bundle must be nil")
}

func TestActiveMatcherBundleState_Current_NilReceiver(t *testing.T) {
	t.Parallel()

	var state *activeMatcherBundleState

	assert.Nil(t, state.Current(), "nil receiver must return nil")
}

func TestActiveMatcherBundleState_Update_SetsBundle(t *testing.T) {
	t.Parallel()

	state := newActiveMatcherBundleState()
	bundle := &MatcherBundle{}

	state.Update(bundle)

	assert.Same(t, bundle, state.Current())
}

func TestActiveMatcherBundleState_Update_OverwritesPrevious(t *testing.T) {
	t.Parallel()

	state := newActiveMatcherBundleState()
	bundleA := &MatcherBundle{}
	bundleB := &MatcherBundle{}

	state.Update(bundleA)
	assert.Same(t, bundleA, state.Current())

	state.Update(bundleB)
	assert.Same(t, bundleB, state.Current())
}

func TestActiveMatcherBundleState_Update_NilBundle(t *testing.T) {
	t.Parallel()

	state := newActiveMatcherBundleState()
	bundle := &MatcherBundle{}

	state.Update(bundle)
	require.NotNil(t, state.Current())

	state.Update(nil)
	assert.Nil(t, state.Current(), "updating with nil must clear the bundle")
}

func TestActiveMatcherBundleState_Update_NilReceiver(t *testing.T) {
	t.Parallel()

	var state *activeMatcherBundleState

	// Must not panic when receiver is nil.
	assert.NotPanics(t, func() {
		state.Update(&MatcherBundle{})
	})
}

func TestActiveMatcherBundleState_ConcurrentAccess(t *testing.T) {
	t.Parallel()

	state := newActiveMatcherBundleState()
	bundle := &MatcherBundle{}
	const goroutines = 50

	var wg sync.WaitGroup

	wg.Add(goroutines * 2)

	for range goroutines {
		go func() {
			defer wg.Done()
			state.Update(bundle)
		}()

		go func() {
			defer wg.Done()
			_ = state.Current()
		}()
	}

	wg.Wait()

	// After all writes, the bundle should be the last one written (or nil in
	// a race-free world, the bundle pointer). The point is no data race.
	result := state.Current()
	assert.True(t, result == bundle || result == nil,
		"after concurrent writes, bundle should be either the written value or nil")
}
