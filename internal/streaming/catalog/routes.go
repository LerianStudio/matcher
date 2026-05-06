// Copyright 2026 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package catalog

import (
	"fmt"
	"strings"

	streaming "github.com/LerianStudio/lib-streaming"
)

// PrimaryKafkaTarget is the canonical name of Matcher's Kafka producer target
// referenced by every RouteDefinition this package emits. Bootstrap wires a
// single transport runtime under this name. Multi-target wiring (per-region
// replicas, SQS shadows) would extend this package with additional targets
// and route generators rather than mutating the primary route table.
const PrimaryKafkaTarget = "primary"

// routeKindSuffix is the lower-case dot-delimited token appended to every
// catalog DefinitionKey when forming a Route.Key. Keeping the suffix constant
// for the single-target wiring matches lib-streaming's canonical route-key
// pattern (`<defKey-normalized>.kafka.<target>`).
const routeKindSuffix = "kafka.primary"

// NewRoutes returns Matcher's canonical RouteDefinition slice — one route per
// catalog EventDefinition, all required, all targeting the single Kafka
// transport named PrimaryKafkaTarget. Topic names follow the lib-streaming
// canonical convention `lerian.streaming.<resource>.<event>` with a `.v<major>`
// suffix appended for SchemaMajorVersion >= 2 (see streaming.EventDefinition.Topic).
//
// Auto-derivation guarantees: adding a new EventDefinition to Definitions()
// automatically adds its route here. Removing one removes its route. The
// catalog_exactness_test.go locks count and topic shape so accidental drift
// surfaces as a test failure rather than a silent missing route.
func NewRoutes() []streaming.RouteDefinition {
	defs := Definitions()
	routes := make([]streaming.RouteDefinition, 0, len(defs))

	for _, def := range defs {
		routes = append(routes, streaming.RouteDefinition{
			Key:           routeKeyFor(def.Key),
			DefinitionKey: def.Key,
			Target:        PrimaryKafkaTarget,
			Destination:   streaming.KafkaTopic(def.Topic()),
			Requirement:   streaming.RouteRequired,
			Description:   def.Description,
		})
	}

	return routes
}

// NewRouteTable builds a validated RouteTable from NewRoutes. Returns an error
// if any route fails lib-streaming's structural validation (canonical key shape,
// destination shape, etc.). This is the value handed to streaming.NewBuilder.Routes(...).
func NewRouteTable() (streaming.RouteTable, error) {
	table, err := streaming.NewRouteTable(NewRoutes()...)
	if err != nil {
		return streaming.RouteTable{}, fmt.Errorf("build matcher streaming route table: %w", err)
	}

	return table, nil
}

// routeKeyFor maps a catalog DefinitionKey (which may contain underscores —
// e.g. "reconciliation_context.created") to a lib-streaming route key shape.
// lib-streaming's canonical route-key regex is
// `^[a-z0-9][a-z0-9-]*(\.[a-z0-9][a-z0-9-]*)+$`, which forbids underscores —
// so we translate `_` to `-` here. The DefinitionKey itself stays unchanged
// in the route's DefinitionKey field; only Route.Key is normalized.
func routeKeyFor(definitionKey string) string {
	normalized := strings.ReplaceAll(definitionKey, "_", "-")

	return normalized + "." + routeKindSuffix
}
