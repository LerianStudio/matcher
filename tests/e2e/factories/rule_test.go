//go:build e2e

package factories

import (
	"testing"

	"github.com/LerianStudio/matcher/tests/e2e"
	"github.com/LerianStudio/matcher/tests/client"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewRuleFactory(t *testing.T) {
	factory := NewRuleFactory(nil, nil)

	require.NotNil(t, factory)
	assert.Nil(t, factory.tc)
	assert.Nil(t, factory.client)
}

func TestRuleFactory_NewRule_DefaultValues(t *testing.T) {
	cfg := &e2e.E2EConfig{DefaultTenantID: "test-tenant"}
	tc := e2e.NewTestContext(t, cfg)
	factory := NewRuleFactory(tc, nil)

	builder := factory.NewRule("context-id-456")

	require.NotNil(t, builder)
	assert.Equal(t, "context-id-456", builder.contextID)
	assert.Equal(t, 1, builder.req.Priority)
	assert.Equal(t, "EXACT", builder.req.Type)
	assert.NotNil(t, builder.req.Config)
}

func TestRuleBuilder_WithPriority(t *testing.T) {
	cfg := &e2e.E2EConfig{DefaultTenantID: "test-tenant"}
	tc := e2e.NewTestContext(t, cfg)
	factory := NewRuleFactory(tc, nil)

	builder := factory.NewRule("ctx").WithPriority(5)

	assert.Equal(t, 5, builder.req.Priority)
}

func TestRuleBuilder_WithType(t *testing.T) {
	cfg := &e2e.E2EConfig{DefaultTenantID: "test-tenant"}
	tc := e2e.NewTestContext(t, cfg)
	factory := NewRuleFactory(tc, nil)

	builder := factory.NewRule("ctx").WithType("custom_type")

	assert.Equal(t, "custom_type", builder.req.Type)
}

func TestRuleBuilder_Exact(t *testing.T) {
	cfg := &e2e.E2EConfig{DefaultTenantID: "test-tenant"}
	tc := e2e.NewTestContext(t, cfg)
	factory := NewRuleFactory(tc, nil)

	builder := factory.NewRule("ctx").Exact()

	assert.Equal(t, "EXACT", builder.req.Type)
}

func TestRuleBuilder_Tolerance(t *testing.T) {
	cfg := &e2e.E2EConfig{DefaultTenantID: "test-tenant"}
	tc := e2e.NewTestContext(t, cfg)
	factory := NewRuleFactory(tc, nil)

	builder := factory.NewRule("ctx").Tolerance()

	assert.Equal(t, "TOLERANCE", builder.req.Type)
}

func TestRuleBuilder_DateLag(t *testing.T) {
	cfg := &e2e.E2EConfig{DefaultTenantID: "test-tenant"}
	tc := e2e.NewTestContext(t, cfg)
	factory := NewRuleFactory(tc, nil)

	builder := factory.NewRule("ctx").DateLag()

	assert.Equal(t, "DATE_LAG", builder.req.Type)
}

func TestRuleBuilder_WithConfig(t *testing.T) {
	cfg := &e2e.E2EConfig{DefaultTenantID: "test-tenant"}
	tc := e2e.NewTestContext(t, cfg)
	factory := NewRuleFactory(tc, nil)

	config := map[string]any{
		"threshold":  0.95,
		"strict":     true,
		"maxResults": 10,
	}

	builder := factory.NewRule("ctx").WithConfig(config)

	assert.Equal(t, config, builder.req.Config)
	assert.Equal(t, 0.95, builder.req.Config["threshold"])
	assert.Equal(t, true, builder.req.Config["strict"])
	assert.Equal(t, 10, builder.req.Config["maxResults"])
}

func TestRuleBuilder_WithExactConfig(t *testing.T) {
	cfg := &e2e.E2EConfig{DefaultTenantID: "test-tenant"}
	tc := e2e.NewTestContext(t, cfg)
	factory := NewRuleFactory(tc, nil)

	builder := factory.NewRule("ctx").WithExactConfig(true, true)

	assert.Equal(t, true, builder.req.Config["matchCurrency"])
	assert.Equal(t, true, builder.req.Config["matchAmount"])
}

func TestRuleBuilder_WithExactConfig_Mixed(t *testing.T) {
	cfg := &e2e.E2EConfig{DefaultTenantID: "test-tenant"}
	tc := e2e.NewTestContext(t, cfg)
	factory := NewRuleFactory(tc, nil)

	builder := factory.NewRule("ctx").WithExactConfig(true, false)

	assert.Equal(t, true, builder.req.Config["matchCurrency"])
	assert.Equal(t, false, builder.req.Config["matchAmount"])
}

func TestRuleBuilder_WithToleranceConfig(t *testing.T) {
	cfg := &e2e.E2EConfig{DefaultTenantID: "test-tenant"}
	tc := e2e.NewTestContext(t, cfg)
	factory := NewRuleFactory(tc, nil)

	builder := factory.NewRule("ctx").WithToleranceConfig("0.01")

	assert.Equal(t, "0.01", builder.req.Config["absTolerance"])
}

func TestRuleBuilder_WithPercentToleranceConfig(t *testing.T) {
	cfg := &e2e.E2EConfig{DefaultTenantID: "test-tenant"}
	tc := e2e.NewTestContext(t, cfg)
	factory := NewRuleFactory(tc, nil)

	builder := factory.NewRule("ctx").WithPercentToleranceConfig(5.5)

	assert.Equal(t, 5.5, builder.req.Config["percentTolerance"])
}

func TestRuleBuilder_Chaining(t *testing.T) {
	cfg := &e2e.E2EConfig{DefaultTenantID: "test-tenant"}
	tc := e2e.NewTestContext(t, cfg)
	factory := NewRuleFactory(tc, nil)

	builder := factory.NewRule("ctx-789").
		WithPriority(3).
		Tolerance().
		WithToleranceConfig("0.05")

	assert.Equal(t, "ctx-789", builder.contextID)
	assert.Equal(t, 3, builder.req.Priority)
	assert.Equal(t, "TOLERANCE", builder.req.Type)
	assert.Equal(t, "0.05", builder.req.Config["absTolerance"])
}

func TestRuleBuilder_TypeOverwrite(t *testing.T) {
	cfg := &e2e.E2EConfig{DefaultTenantID: "test-tenant"}
	tc := e2e.NewTestContext(t, cfg)
	factory := NewRuleFactory(tc, nil)

	builder := factory.NewRule("ctx").
		Exact().
		Tolerance().
		DateLag()

	assert.Equal(t, "DATE_LAG", builder.req.Type)
}

func TestRuleBuilder_ConfigOverwrite(t *testing.T) {
	cfg := &e2e.E2EConfig{DefaultTenantID: "test-tenant"}
	tc := e2e.NewTestContext(t, cfg)
	factory := NewRuleFactory(tc, nil)

	builder := factory.NewRule("ctx").
		WithExactConfig(true, true).
		WithToleranceConfig("0.01")

	assert.Nil(t, builder.req.Config["matchCurrency"])
	assert.Nil(t, builder.req.Config["matchAmount"])
	assert.Equal(t, "0.01", builder.req.Config["absTolerance"])
}

func TestCreateMatchRuleRequest_Structure(t *testing.T) {
	req := client.CreateMatchRuleRequest{
		Priority: 2,
		Type:     "TOLERANCE",
		Config:   map[string]any{"threshold": 0.99},
	}

	assert.Equal(t, 2, req.Priority)
	assert.Equal(t, "TOLERANCE", req.Type)
	assert.Equal(t, 0.99, req.Config["threshold"])
}

func TestRuleBuilder_PriorityValues(t *testing.T) {
	cfg := &e2e.E2EConfig{DefaultTenantID: "test-tenant"}
	tc := e2e.NewTestContext(t, cfg)
	factory := NewRuleFactory(tc, nil)

	testCases := []struct {
		name     string
		priority int
	}{
		{"zero priority", 0},
		{"low priority", 1},
		{"medium priority", 50},
		{"high priority", 100},
		{"very high priority", 1000},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			builder := factory.NewRule("ctx").WithPriority(testCase.priority)
			assert.Equal(t, testCase.priority, builder.req.Priority)
		})
	}
}

func TestRuleBuilder_FullConfiguration(t *testing.T) {
	cfg := &e2e.E2EConfig{DefaultTenantID: "test-tenant"}
	tc := e2e.NewTestContext(t, cfg)
	factory := NewRuleFactory(tc, nil)

	builder := factory.NewRule("full-context").
		WithPriority(10).
		Exact().
		WithExactConfig(true, true)

	assert.Equal(t, "full-context", builder.contextID)
	assert.Equal(t, 10, builder.req.Priority)
	assert.Equal(t, "EXACT", builder.req.Type)
	assert.Equal(t, true, builder.req.Config["matchCurrency"])
	assert.Equal(t, true, builder.req.Config["matchAmount"])
}
