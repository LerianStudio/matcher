// Copyright 2025 Lerian Studio.

//go:build unit

package storetest

import (
	"testing"

	"github.com/LerianStudio/matcher/pkg/systemplane/ports"
)

// TestFakeStoreContracts runs the full contract suite against CombinedFakeStore.
// This validates that the in-memory reference implementation itself conforms to
// the contract, and serves as the baseline for all other adapter tests.
func TestFakeStoreContracts(t *testing.T) {
	factory := func(t *testing.T) (ports.Store, ports.HistoryStore, func()) {
		t.Helper()

		combined := NewCombinedFakeStore()

		return combined, combined.History, func() {}
	}

	RunAll(t, factory)
}
