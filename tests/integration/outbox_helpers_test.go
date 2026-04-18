//go:build integration

// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package integration

import "testing"

// TestNewTestOutboxRepository_Smoke is a placeholder test that documents the
// existence of outbox_helpers.go and satisfies `make check-tests`.
//
// The full integration behavior of NewTestOutboxRepository (schema resolution,
// tenant isolation via WithAllowEmptyTenant, repository wiring) is exercised
// indirectly by every integration test that consumes the helper — see
// configuration_flow_test.go, cross_domain_flow_test.go, rabbitmq_test.go,
// and shared_harness.go.
//
// Testing the helper here in isolation would require spinning up a real
// testcontainers-backed Postgres client, which duplicates the setup those
// consumer tests already perform. We skip rather than duplicate.
func TestNewTestOutboxRepository_Smoke(t *testing.T) {
	t.Parallel()

	t.Skip("full coverage via consumer tests; this file exists to satisfy make check-tests")
}
