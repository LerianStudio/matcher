// Copyright 2026 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package catalog_test

import (
	"strings"
	"testing"

	streaming "github.com/LerianStudio/lib-streaming"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	streamingcatalog "github.com/LerianStudio/matcher/internal/streaming/catalog"
)

// TestNewRoutes_OneRoutePerDefinition guarantees the route generator emits
// exactly one route per catalog definition. Drift here means an event has
// either lost its publish path (missing route) or gained a duplicate route
// — both are silent correctness bugs the catalog_exactness_test tile of the
// suite cannot catch on its own.
func TestNewRoutes_OneRoutePerDefinition(t *testing.T) {
	t.Parallel()

	routes := streamingcatalog.NewRoutes()
	require.Len(t, routes, streamingcatalog.EventCount,
		"expected one route per catalog definition")

	defKeys := make(map[string]int, len(routes))

	for _, r := range routes {
		defKeys[r.DefinitionKey]++
	}

	for key, count := range defKeys {
		assert.Equal(t, 1, count, "definition %q must have exactly one route", key)
	}
}

// TestNewRoutes_AllRequired locks the policy that every catalog event is a
// RouteRequired publish — matcher's all-or-error semantics depend on this.
// Promoting a route to RouteOptional must be a deliberate, reviewed change
// and updating this test is the forcing function.
func TestNewRoutes_AllRequired(t *testing.T) {
	t.Parallel()

	for _, r := range streamingcatalog.NewRoutes() {
		assert.Equal(t, streaming.RouteRequired, r.Requirement,
			"route %q must be RouteRequired (promotion to RouteOptional needs explicit review)", r.Key)
	}
}

// TestNewRoutes_TargetIsPrimaryKafka asserts every route points at the
// single canonical Kafka transport. Multi-target wiring (per-region replicas,
// SQS shadows) must be added as separate route slices, not by mutating the
// primary route table.
func TestNewRoutes_TargetIsPrimaryKafka(t *testing.T) {
	t.Parallel()

	for _, r := range streamingcatalog.NewRoutes() {
		assert.Equal(t, streamingcatalog.PrimaryKafkaTarget, r.Target,
			"route %q target=%q; want %q", r.Key, r.Target, streamingcatalog.PrimaryKafkaTarget)
		assert.Equal(t, streaming.TransportKafkaLike, r.Destination.Kind,
			"route %q destination kind=%q; want kafka", r.Key, r.Destination.Kind)
		assert.True(t, strings.HasPrefix(r.Destination.Name, "lerian.streaming."),
			"route %q topic=%q; want lerian.streaming.* prefix", r.Key, r.Destination.Name)
	}
}

// TestNewRoutes_KeyShape confirms every Route.Key matches lib-streaming's
// canonical key regex: lower-case, dot-delimited, no underscores. Regression
// guard against an accidental future change to routeKeyFor that would feed
// invalid keys into NewRouteTable and surface as runtime build failures
// instead of compile-time test failures.
func TestNewRoutes_KeyShape(t *testing.T) {
	t.Parallel()

	for _, r := range streamingcatalog.NewRoutes() {
		assert.NotContains(t, r.Key, "_",
			"route %q must not contain underscores (lib-streaming canonical key shape)", r.Key)
		assert.Equal(t, strings.ToLower(r.Key), r.Key,
			"route %q must be lower-case", r.Key)
		assert.True(t, strings.HasSuffix(r.Key, ".kafka.primary"),
			"route %q must end with .kafka.primary suffix", r.Key)
	}
}

// TestNewRouteTable_BuildsCleanly asserts that NewRouteTable() succeeds for
// the catalog-derived routes. lib-streaming validates each RouteDefinition on
// table construction (canonical key shape, destination shape, header bounds,
// SSRF posture) — a passing test here means every catalog event is wired with
// a publishable route at boot, not at the first failed Emit.
func TestNewRouteTable_BuildsCleanly(t *testing.T) {
	t.Parallel()

	table, err := streamingcatalog.NewRouteTable()
	require.NoError(t, err, "route table must validate against lib-streaming v1 contract")

	require.NotNil(t, table.Definitions(), "route table must surface defensive copy of routes")
	assert.Len(t, table.Definitions(), streamingcatalog.EventCount)
}

// TestNewRoutes_RouteKeyMapsUnderscoreToHyphen pins the route-key normalization
// rule. Definition keys (e.g. "reconciliation_context.created") use snake_case
// for resource types because they double as Kafka topic names; route keys must
// use hyphens because lib-streaming's canonical regex forbids underscores.
func TestNewRoutes_RouteKeyMapsUnderscoreToHyphen(t *testing.T) {
	t.Parallel()

	routes := streamingcatalog.NewRoutes()
	found := false

	for _, r := range routes {
		if r.DefinitionKey == "reconciliation_context.created" {
			assert.Equal(t, "reconciliation-context.created.kafka.primary", r.Key)

			found = true

			break
		}
	}

	require.True(t, found, "test fixture key reconciliation_context.created missing from catalog")
}
