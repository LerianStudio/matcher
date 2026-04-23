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
