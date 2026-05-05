// Copyright 2026 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package catalog_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	streaming "github.com/LerianStudio/lib-streaming/v2"
	"github.com/stretchr/testify/require"

	streamingcatalog "github.com/LerianStudio/matcher/internal/streaming/catalog"
)

type instrumentationMap struct {
	Events []instrumentationEvent `json:"events"`
}

type instrumentationEvent struct {
	DefinitionKey        string                `json:"definition_key"`
	ResourceType         string                `json:"resource_type"`
	EventType            string                `json:"event_type"`
	SchemaVersion        string                `json:"schema_version"`
	DataContentType      string                `json:"data_content_type"`
	DataSchema           string                `json:"data_schema"`
	SystemEvent          bool                  `json:"is_system_event"`
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

func TestCriticalEventsUseTransactionalEmissionTiming(t *testing.T) {
	contract := loadInstrumentationMap(t)

	criticalEvents := 0
	for _, event := range contract.Events {
		if event.Posture != "CRITICAL" {
			continue
		}

		criticalEvents++
		for _, site := range event.InstrumentationSites {
			require.Equal(t, "in-transaction-before-commit", site.EmissionTiming, event.DefinitionKey)
		}
	}

	require.Equal(t, 14, criticalEvents)
}

func TestDefinitionsMatchInstrumentationMapExactly(t *testing.T) {
	contract := loadInstrumentationMap(t)
	definitions := streamingcatalog.Definitions()

	require.Len(t, definitions, len(contract.Events))
	require.Equal(t, 46, len(definitions))

	byKey := make(map[string]streaming.EventDefinition, len(definitions))
	for _, definition := range definitions {
		byKey[definition.Key] = definition
	}

	postureCounts := map[string]int{}
	for _, event := range contract.Events {
		postureCounts[event.Posture]++

		definition, ok := byKey[event.DefinitionKey]
		require.True(t, ok, "missing catalog definition %q", event.DefinitionKey)
		require.Equal(t, event.ResourceType, definition.ResourceType, event.DefinitionKey)
		require.Equal(t, event.EventType, definition.EventType, event.DefinitionKey)
		require.Equal(t, event.SchemaVersion, definition.SchemaVersion, event.DefinitionKey)
		require.Equal(t, event.DataContentType, definition.DataContentType, event.DefinitionKey)
		require.Equal(t, event.DataSchema, definition.DataSchema, event.DefinitionKey)
		require.Equal(t, event.SystemEvent, definition.SystemEvent, event.DefinitionKey)
		require.Equal(t, event.Description, definition.Description, event.DefinitionKey)
		require.Equal(t, event.DeliveryPolicy.Enabled, definition.DefaultPolicy.Enabled, event.DefinitionKey)
		require.Equal(t, streaming.DirectMode(event.DeliveryPolicy.Direct), definition.DefaultPolicy.Direct, event.DefinitionKey)
		require.Equal(t, streaming.OutboxMode(event.DeliveryPolicy.Outbox), definition.DefaultPolicy.Outbox, event.DefinitionKey)
		require.Equal(t, streaming.DLQMode(event.DeliveryPolicy.DLQ), definition.DefaultPolicy.DLQ, event.DefinitionKey)
	}

	require.Equal(t, map[string]int{"CRITICAL": 14, "IMPORTANT": 32}, postureCounts)
}

func TestCatalogBuildsFromAllDefinitions(t *testing.T) {
	catalog, err := streamingcatalog.NewCatalog()
	require.NoError(t, err)
	require.Len(t, catalog.Definitions(), 46)
}

func loadInstrumentationMap(t *testing.T) instrumentationMap {
	t.Helper()

	_, file, _, ok := runtime.Caller(0)
	require.True(t, ok)

	contractPath := filepath.Join(filepath.Dir(file), "..", "..", "..", "docs", "streaming", "instrumentation-map.json")
	raw, err := os.ReadFile(contractPath)
	require.NoError(t, err)

	var contract instrumentationMap
	require.NoError(t, json.Unmarshal(raw, &contract))

	return contract
}
