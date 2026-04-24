// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit && leak

// REFACTOR-012: Goroutine-leak coverage for reporting workers.
//
// Covers ExportWorker (export_worker.go:215) and CleanupWorker
// (cleanup_worker.go:142, :265). TestMain installs
// goleak.VerifyTestMain so any goroutine spawned by Start() that
// survives Stop() is surfaced. This also validates REFACTOR-015's
// defer-order fix — if RecoverAndLogWithContext ran before Done(),
// panics would mask worker-goroutine leaks.
package worker

import (
	"testing"

	"go.uber.org/goleak"

	"github.com/LerianStudio/matcher/internal/shared/testutil"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m, testutil.LeakOptions()...)
}
