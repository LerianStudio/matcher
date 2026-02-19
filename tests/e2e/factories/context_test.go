//go:build e2e

package factories

import (
	"testing"

	"github.com/LerianStudio/matcher/tests/e2e"
	"github.com/LerianStudio/matcher/tests/e2e/client"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewContextFactory(t *testing.T) {
	t.Parallel()

	factory := NewContextFactory(nil, nil)

	require.NotNil(t, factory)
	assert.Nil(t, factory.tc)
	assert.Nil(t, factory.client)
}

func TestContextFactory_NewContext_DefaultValues(t *testing.T) {
	cfg := &e2e.E2EConfig{DefaultTenantID: "test-tenant"}
	tc := e2e.NewTestContext(t, cfg)
	factory := NewContextFactory(tc, nil)

	builder := factory.NewContext()

	require.NotNil(t, builder)
	assert.Contains(t, builder.req.Name, "e2e-")
	assert.Contains(t, builder.req.Name, "context")
	assert.Equal(t, "1:1", builder.req.Type)
	assert.Equal(t, "0 0 * * *", builder.req.Interval)
}

func TestContextBuilder_WithName(t *testing.T) {
	cfg := &e2e.E2EConfig{DefaultTenantID: "test-tenant"}
	tc := e2e.NewTestContext(t, cfg)
	factory := NewContextFactory(tc, nil)

	builder := factory.NewContext().WithName("my-context")

	assert.Contains(t, builder.req.Name, "e2e-")
	assert.Contains(t, builder.req.Name, "my-context")
}

func TestContextBuilder_WithRawName(t *testing.T) {
	cfg := &e2e.E2EConfig{DefaultTenantID: "test-tenant"}
	tc := e2e.NewTestContext(t, cfg)
	factory := NewContextFactory(tc, nil)

	builder := factory.NewContext().WithRawName("exact-name")

	assert.Equal(t, "exact-name", builder.req.Name)
}

func TestContextBuilder_WithType(t *testing.T) {
	cfg := &e2e.E2EConfig{DefaultTenantID: "test-tenant"}
	tc := e2e.NewTestContext(t, cfg)
	factory := NewContextFactory(tc, nil)

	builder := factory.NewContext().WithType("custom_type")

	assert.Equal(t, "custom_type", builder.req.Type)
}

func TestContextBuilder_OneToOne(t *testing.T) {
	cfg := &e2e.E2EConfig{DefaultTenantID: "test-tenant"}
	tc := e2e.NewTestContext(t, cfg)
	factory := NewContextFactory(tc, nil)

	builder := factory.NewContext().OneToOne()

	assert.Equal(t, "1:1", builder.req.Type)
}

func TestContextBuilder_OneToMany(t *testing.T) {
	cfg := &e2e.E2EConfig{DefaultTenantID: "test-tenant"}
	tc := e2e.NewTestContext(t, cfg)
	factory := NewContextFactory(tc, nil)

	builder := factory.NewContext().OneToMany()

	assert.Equal(t, "1:N", builder.req.Type)
}

func TestContextBuilder_ManyToMany(t *testing.T) {
	cfg := &e2e.E2EConfig{DefaultTenantID: "test-tenant"}
	tc := e2e.NewTestContext(t, cfg)
	factory := NewContextFactory(tc, nil)

	builder := factory.NewContext().ManyToMany()

	assert.Equal(t, "N:M", builder.req.Type)
}

func TestContextBuilder_WithInterval(t *testing.T) {
	cfg := &e2e.E2EConfig{DefaultTenantID: "test-tenant"}
	tc := e2e.NewTestContext(t, cfg)
	factory := NewContextFactory(tc, nil)

	builder := factory.NewContext().WithInterval("*/15 * * * *")

	assert.Equal(t, "*/15 * * * *", builder.req.Interval)
}

func TestContextBuilder_WithDescription(t *testing.T) {
	cfg := &e2e.E2EConfig{DefaultTenantID: "test-tenant"}
	tc := e2e.NewTestContext(t, cfg)
	factory := NewContextFactory(tc, nil)

	builder := factory.NewContext().WithDescription("Test context description")

	assert.Equal(t, "Test context description", builder.req.Description)
}

func TestContextBuilder_Chaining(t *testing.T) {
	cfg := &e2e.E2EConfig{DefaultTenantID: "test-tenant"}
	tc := e2e.NewTestContext(t, cfg)
	factory := NewContextFactory(tc, nil)

	builder := factory.NewContext().
		WithRawName("chained-context").
		ManyToMany().
		WithInterval("0 */6 * * *").
		WithDescription("Chained configuration")

	assert.Equal(t, "chained-context", builder.req.Name)
	assert.Equal(t, "N:M", builder.req.Type)
	assert.Equal(t, "0 */6 * * *", builder.req.Interval)
	assert.Equal(t, "Chained configuration", builder.req.Description)
}

func TestCreateContextRequest_Structure(t *testing.T) {
	req := client.CreateContextRequest{
		Name:        "test-context",
		Type:        "1:1",
		Interval:    "0 0 * * *",
		Description: "Test description",
	}

	assert.Equal(t, "test-context", req.Name)
	assert.Equal(t, "1:1", req.Type)
	assert.Equal(t, "0 0 * * *", req.Interval)
	assert.Equal(t, "Test description", req.Description)
}

func TestContextBuilder_TypeOverwrite(t *testing.T) {
	cfg := &e2e.E2EConfig{DefaultTenantID: "test-tenant"}
	tc := e2e.NewTestContext(t, cfg)
	factory := NewContextFactory(tc, nil)

	builder := factory.NewContext().
		OneToOne().
		OneToMany().
		ManyToMany()

	assert.Equal(t, "N:M", builder.req.Type)
}
