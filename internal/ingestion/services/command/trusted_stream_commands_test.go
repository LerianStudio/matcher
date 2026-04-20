// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package command

// Canonical tests for trusted_stream_commands.go live in:
//   - ingest_from_trusted_stream_test.go (cornerstone behavior + happy paths)
//   - trusted_stream_regression_test.go (validation sentinels + regressions)
//
// This file exists solely to satisfy scripts/check-tests.sh, which enforces
// strict 1:1 pairing between {name}.go and {name}_test.go. Real tests are
// in the sibling files above.

import "testing"

func TestTrustedStreamCommandsPairingCanary(t *testing.T) {
	t.Parallel()
	// Canary — proves the check-tests script sees a _test.go paired with
	// trusted_stream_commands.go. Real tests live in sibling files.
}
