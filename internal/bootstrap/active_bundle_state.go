// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package bootstrap

import "sync"

type activeMatcherBundleState struct {
	mu     sync.RWMutex
	bundle *MatcherBundle
}

func newActiveMatcherBundleState() *activeMatcherBundleState {
	return &activeMatcherBundleState{}
}

func (state *activeMatcherBundleState) Current() *MatcherBundle {
	if state == nil {
		return nil
	}

	state.mu.RLock()
	defer state.mu.RUnlock()

	return state.bundle
}

func (state *activeMatcherBundleState) Update(bundle *MatcherBundle) {
	if state == nil {
		return
	}

	state.mu.Lock()
	defer state.mu.Unlock()

	state.bundle = bundle
}
