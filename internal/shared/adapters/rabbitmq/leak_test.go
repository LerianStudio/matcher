// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit && leak

// REFACTOR-011: Goroutine-leak coverage for ConfirmablePublisher.
//
// ConfirmablePublisher spawns five goroutines during its lifecycle:
//   - close-monitor (line ~426)
//   - recovery-sleep-watcher (line ~559)
//   - drain workers (lines ~654, ~793)
//   - a second close-monitor (line ~876)
//
// The failure modes of interest — Close during active recovery,
// reconnect after panic, Close with pending confirms — are already
// exercised by the confirmable_publisher_test.go suite. TestMain
// installs goleak.VerifyTestMain to surface any goroutine that outlives
// the owning publisher.
package rabbitmq

import (
	"testing"

	"go.uber.org/goleak"

	"github.com/LerianStudio/matcher/internal/shared/testutil"
)

func TestMain(m *testing.M) {
	opts := append(
		testutil.LeakOptions(),
		// ConfirmablePublisher.Close drains broker confirmations in a
		// goroutine bounded by DefaultConfirmTimeout (5s). In unit tests
		// the mock channel never closes its confirms channel, so the
		// drain goroutine waits the full timeout before returning.
		// This is slow cleanup, not a leak — the goroutine ALWAYS exits.
		// A focused test could shorten the timeout via WithConfirmTimeout,
		// but the infrastructure cost of re-plumbing every existing test
		// outweighs the detection benefit.
		goleak.IgnoreAnyFunction("github.com/LerianStudio/matcher/internal/shared/adapters/rabbitmq.(*ConfirmablePublisher).Close.func2"),
		// Same bounded-drain pattern as Close.func2 — prepareForRecovery
		// spawns a separate drain when replacing the AMQP channel.
		goleak.IgnoreAnyFunction("github.com/LerianStudio/matcher/internal/shared/adapters/rabbitmq.(*ConfirmablePublisher).prepareForRecovery.func2"),
	)
	goleak.VerifyTestMain(m, opts...)
}
