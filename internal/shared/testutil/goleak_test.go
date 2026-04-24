// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package testutil

import "testing"

func TestLeakOptions_NonEmpty(t *testing.T) {
	t.Parallel()

	opts := LeakOptions()
	if len(opts) == 0 {
		t.Fatal("LeakOptions returned empty slice — expected standard ignore entries")
	}
}

func TestLeakOptionsWithSystemplane_ExtendsBase(t *testing.T) {
	t.Parallel()

	base := LeakOptions()
	extended := LeakOptionsWithSystemplane()

	if len(extended) <= len(base) {
		t.Fatalf("expected systemplane variant to extend base (base=%d, extended=%d)", len(base), len(extended))
	}
}

func TestLeakOptionsBootstrap_ExtendsSystemplane(t *testing.T) {
	t.Parallel()

	systemplane := LeakOptionsWithSystemplane()
	bootstrap := LeakOptionsBootstrap()

	if len(bootstrap) <= len(systemplane) {
		t.Fatalf("expected bootstrap variant to extend systemplane (systemplane=%d, bootstrap=%d)", len(systemplane), len(bootstrap))
	}
}
