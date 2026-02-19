//go:build e2e

package factories

import (
	"testing"

	"github.com/LerianStudio/matcher/tests/e2e"
	"github.com/LerianStudio/matcher/tests/e2e/client"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewSourceFactory(t *testing.T) {
	t.Parallel()

	factory := NewSourceFactory(nil, nil)

	require.NotNil(t, factory)
	assert.Nil(t, factory.tc)
	assert.Nil(t, factory.client)
}

func TestSourceFactory_NewSource_DefaultValues(t *testing.T) {
	t.Parallel()

	cfg := &e2e.E2EConfig{DefaultTenantID: "test-tenant"}
	tc := e2e.NewTestContext(t, cfg)
	factory := NewSourceFactory(tc, nil)

	builder := factory.NewSource("context-id-123")

	require.NotNil(t, builder)
	assert.Equal(t, "context-id-123", builder.contextID)
	assert.Contains(t, builder.req.Name, "e2e-")
	assert.Contains(t, builder.req.Name, "source")
	assert.Equal(t, "LEDGER", builder.req.Type)
	assert.NotNil(t, builder.req.Config)
}

func TestSourceBuilder_WithName(t *testing.T) {
	t.Parallel()

	cfg := &e2e.E2EConfig{DefaultTenantID: "test-tenant"}
	tc := e2e.NewTestContext(t, cfg)
	factory := NewSourceFactory(tc, nil)

	builder := factory.NewSource("ctx").WithName("my-source")

	assert.Contains(t, builder.req.Name, "e2e-")
	assert.Contains(t, builder.req.Name, "my-source")
}

func TestSourceBuilder_WithRawName(t *testing.T) {
	t.Parallel()

	cfg := &e2e.E2EConfig{DefaultTenantID: "test-tenant"}
	tc := e2e.NewTestContext(t, cfg)
	factory := NewSourceFactory(tc, nil)

	builder := factory.NewSource("ctx").WithRawName("exact-source-name")

	assert.Equal(t, "exact-source-name", builder.req.Name)
}

func TestSourceBuilder_WithType(t *testing.T) {
	t.Parallel()

	cfg := &e2e.E2EConfig{DefaultTenantID: "test-tenant"}
	tc := e2e.NewTestContext(t, cfg)
	factory := NewSourceFactory(tc, nil)

	builder := factory.NewSource("ctx").WithType("custom_type")

	assert.Equal(t, "custom_type", builder.req.Type)
}

func TestSourceBuilder_AsLedger(t *testing.T) {
	t.Parallel()

	cfg := &e2e.E2EConfig{DefaultTenantID: "test-tenant"}
	tc := e2e.NewTestContext(t, cfg)
	factory := NewSourceFactory(tc, nil)

	builder := factory.NewSource("ctx").AsLedger()

	assert.Equal(t, "LEDGER", builder.req.Type)
}

func TestSourceBuilder_AsBank(t *testing.T) {
	t.Parallel()

	cfg := &e2e.E2EConfig{DefaultTenantID: "test-tenant"}
	tc := e2e.NewTestContext(t, cfg)
	factory := NewSourceFactory(tc, nil)

	builder := factory.NewSource("ctx").AsBank()

	assert.Equal(t, "BANK", builder.req.Type)
}

func TestSourceBuilder_AsGateway(t *testing.T) {
	t.Parallel()

	cfg := &e2e.E2EConfig{DefaultTenantID: "test-tenant"}
	tc := e2e.NewTestContext(t, cfg)
	factory := NewSourceFactory(tc, nil)

	builder := factory.NewSource("ctx").AsGateway()

	assert.Equal(t, "GATEWAY", builder.req.Type)
}

func TestSourceBuilder_WithConfig(t *testing.T) {
	t.Parallel()

	cfg := &e2e.E2EConfig{DefaultTenantID: "test-tenant"}
	tc := e2e.NewTestContext(t, cfg)
	factory := NewSourceFactory(tc, nil)

	config := map[string]any{
		"endpoint": "https://api.example.com",
		"timeout":  30,
		"enabled":  true,
	}

	builder := factory.NewSource("ctx").WithConfig(config)

	assert.Equal(t, config, builder.req.Config)
	assert.Equal(t, "https://api.example.com", builder.req.Config["endpoint"])
	assert.Equal(t, 30, builder.req.Config["timeout"])
	assert.Equal(t, true, builder.req.Config["enabled"])
}

func TestSourceBuilder_Chaining(t *testing.T) {
	t.Parallel()

	cfg := &e2e.E2EConfig{DefaultTenantID: "test-tenant"}
	tc := e2e.NewTestContext(t, cfg)
	factory := NewSourceFactory(tc, nil)

	builder := factory.NewSource("ctx-123").
		WithRawName("chained-source").
		AsBank().
		WithConfig(map[string]any{"key": "value"})

	assert.Equal(t, "chained-source", builder.req.Name)
	assert.Equal(t, "BANK", builder.req.Type)
	assert.Equal(t, "value", builder.req.Config["key"])
}

func TestSourceBuilder_TypeOverwrite(t *testing.T) {
	t.Parallel()

	cfg := &e2e.E2EConfig{DefaultTenantID: "test-tenant"}
	tc := e2e.NewTestContext(t, cfg)
	factory := NewSourceFactory(tc, nil)

	builder := factory.NewSource("ctx").
		AsLedger().
		AsBank().
		AsGateway()

	assert.Equal(t, "GATEWAY", builder.req.Type)
}

func TestCreateSourceRequest_Structure(t *testing.T) {
	t.Parallel()

	req := client.CreateSourceRequest{
		Name:   "test-source",
		Type:   "LEDGER",
		Config: map[string]any{"setting": "value"},
	}

	assert.Equal(t, "test-source", req.Name)
	assert.Equal(t, "LEDGER", req.Type)
	assert.Equal(t, "value", req.Config["setting"])
}

func TestSourceFactory_NewFieldMap_DefaultValues(t *testing.T) {
	t.Parallel()

	cfg := &e2e.E2EConfig{DefaultTenantID: "test-tenant"}
	tc := e2e.NewTestContext(t, cfg)
	factory := NewSourceFactory(tc, nil)

	builder := factory.NewFieldMap("context-id", "source-id")

	require.NotNil(t, builder)
	assert.Equal(t, "context-id", builder.contextID)
	assert.Equal(t, "source-id", builder.sourceID)
	assert.NotNil(t, builder.req.Mapping)

	assert.Equal(t, "external_id", builder.req.Mapping["id"])
	assert.Equal(t, "amount", builder.req.Mapping["amount"])
	assert.Equal(t, "currency", builder.req.Mapping["currency"])
	assert.Equal(t, "date", builder.req.Mapping["date"])
	assert.Equal(t, "description", builder.req.Mapping["description"])
}

func TestFieldMapBuilder_WithMapping(t *testing.T) {
	t.Parallel()

	cfg := &e2e.E2EConfig{DefaultTenantID: "test-tenant"}
	tc := e2e.NewTestContext(t, cfg)
	factory := NewSourceFactory(tc, nil)

	customMapping := map[string]any{
		"id":          "transaction_ref",
		"amount":      "value",
		"currency":    "currency_code",
		"date":        "transaction_date",
		"description": "memo",
	}

	builder := factory.NewFieldMap("ctx", "src").WithMapping(customMapping)

	assert.Equal(t, customMapping, builder.req.Mapping)
	assert.Equal(t, "transaction_ref", builder.req.Mapping["id"])
	assert.Equal(t, "value", builder.req.Mapping["amount"])
}

func TestFieldMapBuilder_WithStandardMapping(t *testing.T) {
	t.Parallel()

	cfg := &e2e.E2EConfig{DefaultTenantID: "test-tenant"}
	tc := e2e.NewTestContext(t, cfg)
	factory := NewSourceFactory(tc, nil)

	builder := factory.NewFieldMap("ctx", "src").
		WithMapping(map[string]any{"custom": "mapping"}).
		WithStandardMapping()

	// Mapping format: internal_field → source_column
	assert.Equal(t, "id", builder.req.Mapping["external_id"])
	assert.Equal(t, "amount", builder.req.Mapping["amount"])
	assert.Equal(t, "currency", builder.req.Mapping["currency"])
	assert.Equal(t, "date", builder.req.Mapping["date"])
	assert.Equal(t, "description", builder.req.Mapping["description"])
	assert.Nil(t, builder.req.Mapping["custom"])
}

func TestCreateFieldMapRequest_Structure(t *testing.T) {
	t.Parallel()

	req := client.CreateFieldMapRequest{
		Mapping: map[string]any{
			"id":     "ref",
			"amount": "amt",
		},
	}

	assert.Equal(t, "ref", req.Mapping["id"])
	assert.Equal(t, "amt", req.Mapping["amount"])
}

func TestFieldMapBuilder_Chaining(t *testing.T) {
	t.Parallel()

	cfg := &e2e.E2EConfig{DefaultTenantID: "test-tenant"}
	tc := e2e.NewTestContext(t, cfg)
	factory := NewSourceFactory(tc, nil)

	builder := factory.NewFieldMap("ctx-1", "src-1").
		WithMapping(map[string]any{"initial": "value"}).
		WithStandardMapping()

	assert.Equal(t, "ctx-1", builder.contextID)
	assert.Equal(t, "src-1", builder.sourceID)
	// Mapping format: internal_field → source_column
	assert.Equal(t, "id", builder.req.Mapping["external_id"])
	assert.Nil(t, builder.req.Mapping["initial"])
}
