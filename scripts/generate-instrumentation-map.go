// Copyright 2026 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build ignore
// +build ignore

// Generator for docs/streaming/instrumentation-map.json.
//
// Reads the canonical streaming catalog from
// internal/streaming/catalog.Definitions() and emits the PM-validated
// instrumentation map consumed by:
//
//   - internal/streaming/catalog/catalog_exactness_test.go (contract test)
//   - ring:dev-streaming-instrumentation skill (downstream wiring)
//
// CRITICAL events are emitted "in-transaction-before-commit" via the outbox.
// IMPORTANT events are emitted "post-commit" (best-effort, broker-outage
// surviving). The CRITICAL set is derived empirically by greping the
// codebase for emission.RequireOutboxTx() / WithOutboxTx() call sites and
// confirming each event key is reached only through tx-bound emission.
//
// Run with:
//
//	go run scripts/generate-instrumentation-map.go
package main

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"

	streamingcatalog "github.com/LerianStudio/matcher/internal/streaming/catalog"
)

// criticalEventKeys is the canonical set of CRITICAL-posture events.
// Every key listed here MUST be reached only via emission.WithOutboxTx +
// emission.RequireOutboxTx call sites in the matcher source tree.
//
// To verify: rg 'WithOutboxTx|RequireOutboxTx' internal/ -l, then trace
// each call site's definitionKey argument.
var criticalEventKeys = map[string]struct{}{
	"actor.pseudonymized":             {},
	"archive_metadata.created":        {},
	"archive.completed":               {},
	"archive.uploaded":                {},
	"audit_log.created":               {},
	"dispute.lost":                    {},
	"dispute.opened":                  {},
	"dispute.won":                     {},
	"evidence.submitted":              {},
	"exception.adjust_entry_resolved": {},
	"exception.callback_processed":    {},
	"exception.dispatched":            {},
	"exception.force_match_resolved":  {},
	"exception.resolved":              {},
}

type instrumentationMap struct {
	ServiceName   string               `json:"service_name"`
	Skill         string               `json:"skill"`
	SchemaVersion string               `json:"schema_version"`
	Events        []instrumentationEvt `json:"events"`
}

type instrumentationEvt struct {
	DefinitionKey        string                `json:"definition_key"`
	ResourceType         string                `json:"resource_type"`
	EventType            string                `json:"event_type"`
	SchemaVersion        string                `json:"schema_version"`
	DataContentType      string                `json:"data_content_type"`
	DataSchema           string                `json:"data_schema"`
	IsSystemEvent        bool                  `json:"is_system_event"`
	Description          string                `json:"description"`
	InstrumentationSites []instrumentationSite `json:"instrumentation_sites"`
	Posture              string                `json:"posture"`
	DeliveryPolicy       deliveryPolicy        `json:"delivery_policy"`
}

type instrumentationSite struct {
	EmissionTiming string `json:"emission_timing"`
}

type deliveryPolicy struct {
	Enabled bool   `json:"enabled"`
	Direct  string `json:"direct"`
	Outbox  string `json:"outbox"`
	DLQ     string `json:"dlq"`
}

func main() {
	defs := streamingcatalog.Definitions()

	if len(defs) != streamingcatalog.EventCount {
		fmt.Fprintf(os.Stderr, "catalog returned %d definitions, expected %d\n", len(defs), streamingcatalog.EventCount)
		os.Exit(1)
	}

	criticalSeen := map[string]bool{}
	for key := range criticalEventKeys {
		criticalSeen[key] = false
	}

	events := make([]instrumentationEvt, 0, len(defs))
	for _, d := range defs {
		_, isCritical := criticalEventKeys[d.Key]
		if isCritical {
			criticalSeen[d.Key] = true
		}

		posture := "IMPORTANT"
		emissionTiming := "post-commit"
		if isCritical {
			posture = "CRITICAL"
			emissionTiming = "in-transaction-before-commit"
		}

		events = append(events, instrumentationEvt{
			DefinitionKey:        d.Key,
			ResourceType:         d.ResourceType,
			EventType:            d.EventType,
			SchemaVersion:        d.SchemaVersion,
			DataContentType:      d.DataContentType,
			DataSchema:           d.DataSchema,
			IsSystemEvent:        d.SystemEvent,
			Description:          d.Description,
			InstrumentationSites: []instrumentationSite{{EmissionTiming: emissionTiming}},
			Posture:              posture,
			DeliveryPolicy: deliveryPolicy{
				Enabled: d.DefaultPolicy.Enabled,
				Direct:  string(d.DefaultPolicy.Direct),
				Outbox:  string(d.DefaultPolicy.Outbox),
				DLQ:     string(d.DefaultPolicy.DLQ),
			},
		})
	}

	// Verify every declared CRITICAL key was actually present in the catalog.
	missing := []string{}
	for key, seen := range criticalSeen {
		if !seen {
			missing = append(missing, key)
		}
	}
	if len(missing) > 0 {
		sort.Strings(missing)
		fmt.Fprintf(os.Stderr, "CRITICAL keys declared in generator but not in catalog: %v\n", missing)
		os.Exit(1)
	}

	// Sort events by definition_key for stable, reviewable output.
	sort.Slice(events, func(i, j int) bool {
		return events[i].DefinitionKey < events[j].DefinitionKey
	})

	// Sanity checks before writing.
	criticalCount := 0
	importantCount := 0
	for _, e := range events {
		switch e.Posture {
		case "CRITICAL":
			criticalCount++
		case "IMPORTANT":
			importantCount++
		}
	}
	if criticalCount != 14 || importantCount != 32 {
		fmt.Fprintf(os.Stderr, "posture distribution wrong: CRITICAL=%d IMPORTANT=%d (expected 14/32)\n", criticalCount, importantCount)
		os.Exit(1)
	}

	out := instrumentationMap{
		ServiceName:   "matcher",
		Skill:         "ring:streaming-event-mapping",
		SchemaVersion: "1.0.0",
		Events:        events,
	}

	body, err := json.MarshalIndent(out, "", "  ")
	if err != nil {
		fmt.Fprintf(os.Stderr, "marshal: %v\n", err)
		os.Exit(1)
	}

	body = append(body, '\n')

	const target = "docs/streaming/instrumentation-map.json"
	if err := os.WriteFile(target, body, 0o644); err != nil {
		fmt.Fprintf(os.Stderr, "write %s: %v\n", target, err)
		os.Exit(1)
	}

	fmt.Printf("[ok] wrote %s (%d events, %d CRITICAL, %d IMPORTANT)\n", target, len(events), criticalCount, importantCount)
}
