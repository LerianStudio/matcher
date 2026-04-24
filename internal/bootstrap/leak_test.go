// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit && leak

// REFACTOR-008: Goroutine-leak coverage for internal/bootstrap.
//
// The bootstrap package owns 8 goroutine spawn sites that implement
// long-lived composition-root concerns (OTel init, migrations, self-probe,
// worker lifecycle, DB metrics, health check evaluator). TestMain installs
// goleak.VerifyTestMain with the shared ignore list plus systemplane
// listeners — the bootstrap wires the lib-commons v5 runtime-config client
// whose Subscribe goroutine is intentionally long-lived.
package bootstrap

import (
	"testing"

	"go.uber.org/goleak"

	"github.com/LerianStudio/matcher/internal/shared/testutil"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m, testutil.LeakOptionsBootstrap()...)
}
