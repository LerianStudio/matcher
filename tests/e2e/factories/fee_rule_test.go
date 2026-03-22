//go:build e2e

package factories

import (
	"testing"

	"github.com/LerianStudio/matcher/tests/e2e"
	"github.com/LerianStudio/matcher/tests/e2e/client"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewFeeRuleFactory(t *testing.T) {
	t.Parallel()

	factory := NewFeeRuleFactory(nil, nil)

	require.NotNil(t, factory)
	assert.Nil(t, factory.tc)
	assert.Nil(t, factory.client)
}

func TestFeeRuleFactory_NewFeeRule_DefaultValues(t *testing.T) {
	t.Parallel()

	cfg := &e2e.E2EConfig{DefaultTenantID: "test-tenant"}
	tc := e2e.NewTestContext(t, cfg)
	factory := NewFeeRuleFactory(tc, nil)

	builder := factory.NewFeeRule("context-id-123")

	require.NotNil(t, builder)
	assert.Equal(t, "context-id-123", builder.contextID)
	assert.Equal(t, "ANY", builder.req.Side)
	assert.Contains(t, builder.req.Name, "e2e-")
	assert.Contains(t, builder.req.Name, "fee-rule")
	assert.Equal(t, 0, builder.req.Priority)
	assert.NotNil(t, builder.req.Predicates)
	assert.Empty(t, builder.req.Predicates)
}

func TestFeeRuleBuilder_WithName(t *testing.T) {
	t.Parallel()

	cfg := &e2e.E2EConfig{DefaultTenantID: "test-tenant"}
	tc := e2e.NewTestContext(t, cfg)
	factory := NewFeeRuleFactory(tc, nil)

	builder := factory.NewFeeRule("ctx").WithName("my-fee-rule")

	assert.Contains(t, builder.req.Name, "e2e-")
	assert.Contains(t, builder.req.Name, "my-fee-rule")
}

func TestFeeRuleBuilder_WithSide(t *testing.T) {
	t.Parallel()

	cfg := &e2e.E2EConfig{DefaultTenantID: "test-tenant"}
	tc := e2e.NewTestContext(t, cfg)
	factory := NewFeeRuleFactory(tc, nil)

	builder := factory.NewFeeRule("ctx").WithSide("LEFT")

	assert.Equal(t, "LEFT", builder.req.Side)
}

func TestFeeRuleBuilder_Any(t *testing.T) {
	t.Parallel()

	cfg := &e2e.E2EConfig{DefaultTenantID: "test-tenant"}
	tc := e2e.NewTestContext(t, cfg)
	factory := NewFeeRuleFactory(tc, nil)

	builder := factory.NewFeeRule("ctx").WithSide("LEFT").Any()

	assert.Equal(t, "ANY", builder.req.Side)
}

func TestFeeRuleBuilder_Left(t *testing.T) {
	t.Parallel()

	cfg := &e2e.E2EConfig{DefaultTenantID: "test-tenant"}
	tc := e2e.NewTestContext(t, cfg)
	factory := NewFeeRuleFactory(tc, nil)

	builder := factory.NewFeeRule("ctx").Left()

	assert.Equal(t, "LEFT", builder.req.Side)
}

func TestFeeRuleBuilder_Right(t *testing.T) {
	t.Parallel()

	cfg := &e2e.E2EConfig{DefaultTenantID: "test-tenant"}
	tc := e2e.NewTestContext(t, cfg)
	factory := NewFeeRuleFactory(tc, nil)

	builder := factory.NewFeeRule("ctx").Right()

	assert.Equal(t, "RIGHT", builder.req.Side)
}

func TestFeeRuleBuilder_WithFeeScheduleID(t *testing.T) {
	t.Parallel()

	cfg := &e2e.E2EConfig{DefaultTenantID: "test-tenant"}
	tc := e2e.NewTestContext(t, cfg)
	factory := NewFeeRuleFactory(tc, nil)

	builder := factory.NewFeeRule("ctx").WithFeeScheduleID("schedule-abc-123")

	assert.Equal(t, "schedule-abc-123", builder.req.FeeScheduleID)
}

func TestFeeRuleBuilder_WithPriority(t *testing.T) {
	t.Parallel()

	cfg := &e2e.E2EConfig{DefaultTenantID: "test-tenant"}
	tc := e2e.NewTestContext(t, cfg)
	factory := NewFeeRuleFactory(tc, nil)

	builder := factory.NewFeeRule("ctx").WithPriority(5)

	assert.Equal(t, 5, builder.req.Priority)
}

func TestFeeRuleBuilder_WithEqualsPredicate(t *testing.T) {
	t.Parallel()

	cfg := &e2e.E2EConfig{DefaultTenantID: "test-tenant"}
	tc := e2e.NewTestContext(t, cfg)
	factory := NewFeeRuleFactory(tc, nil)

	builder := factory.NewFeeRule("ctx").WithEqualsPredicate("currency", "USD")

	require.Len(t, builder.req.Predicates, 1)
	assert.Equal(t, "currency", builder.req.Predicates[0].Field)
	assert.Equal(t, "EQUALS", builder.req.Predicates[0].Operator)
	assert.Equal(t, "USD", builder.req.Predicates[0].Value)
}

func TestFeeRuleBuilder_WithInPredicate(t *testing.T) {
	t.Parallel()

	cfg := &e2e.E2EConfig{DefaultTenantID: "test-tenant"}
	tc := e2e.NewTestContext(t, cfg)
	factory := NewFeeRuleFactory(tc, nil)

	builder := factory.NewFeeRule("ctx").WithInPredicate("currency", "USD", "EUR", "GBP")

	require.Len(t, builder.req.Predicates, 1)
	assert.Equal(t, "currency", builder.req.Predicates[0].Field)
	assert.Equal(t, "IN", builder.req.Predicates[0].Operator)
	assert.Equal(t, []string{"USD", "EUR", "GBP"}, builder.req.Predicates[0].Values)
}

func TestFeeRuleBuilder_MultiplePredicates(t *testing.T) {
	t.Parallel()

	cfg := &e2e.E2EConfig{DefaultTenantID: "test-tenant"}
	tc := e2e.NewTestContext(t, cfg)
	factory := NewFeeRuleFactory(tc, nil)

	builder := factory.NewFeeRule("ctx").
		WithEqualsPredicate("currency", "USD").
		WithInPredicate("type", "DEBIT", "CREDIT").
		WithEqualsPredicate("region", "US")

	require.Len(t, builder.req.Predicates, 3)
	assert.Equal(t, "EQUALS", builder.req.Predicates[0].Operator)
	assert.Equal(t, "IN", builder.req.Predicates[1].Operator)
	assert.Equal(t, "EQUALS", builder.req.Predicates[2].Operator)
	assert.Equal(t, "region", builder.req.Predicates[2].Field)
}

func TestFeeRuleBuilder_Chaining(t *testing.T) {
	t.Parallel()

	cfg := &e2e.E2EConfig{DefaultTenantID: "test-tenant"}
	tc := e2e.NewTestContext(t, cfg)
	factory := NewFeeRuleFactory(tc, nil)

	builder := factory.NewFeeRule("ctx-789").
		WithName("chained-rule").
		Left().
		WithFeeScheduleID("sched-001").
		WithPriority(10).
		WithEqualsPredicate("currency", "BRL")

	assert.Equal(t, "ctx-789", builder.contextID)
	assert.Contains(t, builder.req.Name, "chained-rule")
	assert.Equal(t, "LEFT", builder.req.Side)
	assert.Equal(t, "sched-001", builder.req.FeeScheduleID)
	assert.Equal(t, 10, builder.req.Priority)
	require.Len(t, builder.req.Predicates, 1)
	assert.Equal(t, "BRL", builder.req.Predicates[0].Value)
}

func TestFeeRuleBuilder_SideOverwrite(t *testing.T) {
	t.Parallel()

	cfg := &e2e.E2EConfig{DefaultTenantID: "test-tenant"}
	tc := e2e.NewTestContext(t, cfg)
	factory := NewFeeRuleFactory(tc, nil)

	builder := factory.NewFeeRule("ctx").
		Left().
		Right().
		Any()

	assert.Equal(t, "ANY", builder.req.Side)
}

func TestFeeRuleBuilder_PriorityValues(t *testing.T) {
	t.Parallel()

	cfg := &e2e.E2EConfig{DefaultTenantID: "test-tenant"}
	tc := e2e.NewTestContext(t, cfg)
	factory := NewFeeRuleFactory(tc, nil)

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

			builder := factory.NewFeeRule("ctx").WithPriority(testCase.priority)
			assert.Equal(t, testCase.priority, builder.req.Priority)
		})
	}
}

func TestCreateFeeRuleRequest_Structure(t *testing.T) {
	t.Parallel()

	req := client.CreateFeeRuleRequest{
		Side:          "LEFT",
		FeeScheduleID: "sched-xyz",
		Name:          "test-fee-rule",
		Priority:      3,
		Predicates: []client.CreateFeeRulePredicateRequest{
			{Field: "currency", Operator: "EQUALS", Value: "USD"},
		},
	}

	assert.Equal(t, "LEFT", req.Side)
	assert.Equal(t, "sched-xyz", req.FeeScheduleID)
	assert.Equal(t, "test-fee-rule", req.Name)
	assert.Equal(t, 3, req.Priority)
	require.Len(t, req.Predicates, 1)
	assert.Equal(t, "currency", req.Predicates[0].Field)
	assert.Equal(t, "EQUALS", req.Predicates[0].Operator)
	assert.Equal(t, "USD", req.Predicates[0].Value)
}

func TestCreateFeeRulePredicateRequest_Structure(t *testing.T) {
	t.Parallel()

	equalsPred := client.CreateFeeRulePredicateRequest{
		Field:    "currency",
		Operator: "EQUALS",
		Value:    "EUR",
	}

	assert.Equal(t, "currency", equalsPred.Field)
	assert.Equal(t, "EQUALS", equalsPred.Operator)
	assert.Equal(t, "EUR", equalsPred.Value)
	assert.Nil(t, equalsPred.Values)

	inPred := client.CreateFeeRulePredicateRequest{
		Field:    "type",
		Operator: "IN",
		Values:   []string{"DEBIT", "CREDIT"},
	}

	assert.Equal(t, "type", inPred.Field)
	assert.Equal(t, "IN", inPred.Operator)
	assert.Empty(t, inPred.Value)
	assert.Equal(t, []string{"DEBIT", "CREDIT"}, inPred.Values)
}

func TestFeeRuleBuilder_FullConfiguration(t *testing.T) {
	t.Parallel()

	cfg := &e2e.E2EConfig{DefaultTenantID: "test-tenant"}
	tc := e2e.NewTestContext(t, cfg)
	factory := NewFeeRuleFactory(tc, nil)

	builder := factory.NewFeeRule("full-context").
		WithName("full-rule").
		Right().
		WithFeeScheduleID("full-schedule-id").
		WithPriority(99).
		WithEqualsPredicate("currency", "USD").
		WithInPredicate("region", "US", "CA", "MX")

	assert.Equal(t, "full-context", builder.contextID)
	assert.Contains(t, builder.req.Name, "full-rule")
	assert.Equal(t, "RIGHT", builder.req.Side)
	assert.Equal(t, "full-schedule-id", builder.req.FeeScheduleID)
	assert.Equal(t, 99, builder.req.Priority)
	require.Len(t, builder.req.Predicates, 2)
	assert.Equal(t, "EQUALS", builder.req.Predicates[0].Operator)
	assert.Equal(t, "USD", builder.req.Predicates[0].Value)
	assert.Equal(t, "IN", builder.req.Predicates[1].Operator)
	assert.Equal(t, []string{"US", "CA", "MX"}, builder.req.Predicates[1].Values)
}

func TestFeeRuleBuilder_PredicatesAccumulate(t *testing.T) {
	t.Parallel()

	cfg := &e2e.E2EConfig{DefaultTenantID: "test-tenant"}
	tc := e2e.NewTestContext(t, cfg)
	factory := NewFeeRuleFactory(tc, nil)

	builder := factory.NewFeeRule("ctx")
	assert.Empty(t, builder.req.Predicates)

	builder.WithEqualsPredicate("field1", "val1")
	assert.Len(t, builder.req.Predicates, 1)

	builder.WithEqualsPredicate("field2", "val2")
	assert.Len(t, builder.req.Predicates, 2)

	builder.WithInPredicate("field3", "a", "b")
	assert.Len(t, builder.req.Predicates, 3)
}

func TestFeeRuleBuilder_WithInPredicate_SingleValue(t *testing.T) {
	t.Parallel()

	cfg := &e2e.E2EConfig{DefaultTenantID: "test-tenant"}
	tc := e2e.NewTestContext(t, cfg)
	factory := NewFeeRuleFactory(tc, nil)

	builder := factory.NewFeeRule("ctx").WithInPredicate("status", "ACTIVE")

	require.Len(t, builder.req.Predicates, 1)
	assert.Equal(t, "IN", builder.req.Predicates[0].Operator)
	assert.Equal(t, []string{"ACTIVE"}, builder.req.Predicates[0].Values)
}

func TestFeeRuleBuilder_DefaultPredicatesEmpty(t *testing.T) {
	t.Parallel()

	cfg := &e2e.E2EConfig{DefaultTenantID: "test-tenant"}
	tc := e2e.NewTestContext(t, cfg)
	factory := NewFeeRuleFactory(tc, nil)

	builder := factory.NewFeeRule("ctx")

	assert.NotNil(t, builder.req.Predicates)
	assert.Empty(t, builder.req.Predicates)
	assert.Len(t, builder.req.Predicates, 0)
}
