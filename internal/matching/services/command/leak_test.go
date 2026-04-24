// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit && leak

// REFACTOR-013: Goroutine-leak coverage for matching command services.
//
// Covers lock refresh goroutines in match_group_lock_commands.go
// (lines 196, 240) and trigger_commands.go (line 43). TestMain installs
// goleak.VerifyTestMain so any lock-refresh or trigger goroutine that
// escapes its ctx cancel is surfaced.
package command

import (
	"testing"

	"go.uber.org/goleak"

	"github.com/LerianStudio/matcher/internal/shared/testutil"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m, testutil.LeakOptions()...)
}
