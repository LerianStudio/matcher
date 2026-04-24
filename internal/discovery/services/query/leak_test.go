// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit && leak

// REFACTOR-014: Goroutine-leak coverage for the discovery query service.
//
// connection_queries.go detaches the parent context via
// context.WithoutCancel before spawning cacheSchemas in a SafeGo
// goroutine — the detached ctx cannot be cancelled from outside, so an
// internal cacheSchemaDeadline inside cacheSchemas is the only
// termination guarantee. TestMain installs goleak.VerifyTestMain and
// connection_queries_leak_test.go adds a targeted VerifyNone that
// drives the deadline path with a cache that never returns.
package query

import (
	"testing"

	"go.uber.org/goleak"

	"github.com/LerianStudio/matcher/internal/shared/testutil"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m, testutil.LeakOptions()...)
}
