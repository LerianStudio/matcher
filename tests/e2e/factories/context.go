//go:build e2e

package factories

import (
	"context"
	"fmt"

	"github.com/LerianStudio/matcher/tests/e2e"
	"github.com/LerianStudio/matcher/tests/client"
)

// ContextFactory creates reconciliation contexts for tests.
type ContextFactory struct {
	tc     *e2e.TestContext
	client *e2e.Client
}

// NewContextFactory creates a new context factory.
func NewContextFactory(tc *e2e.TestContext, c *e2e.Client) *ContextFactory {
	return &ContextFactory{tc: tc, client: c}
}

// Client returns the underlying API client.
func (f *ContextFactory) Client() *e2e.Client {
	return f.client
}

// ContextBuilder builds context creation requests.
type ContextBuilder struct {
	factory        *ContextFactory
	req            client.CreateContextRequest
	skipActivation bool
}

// NewContext starts building a new context.
func (f *ContextFactory) NewContext() *ContextBuilder {
	return &ContextBuilder{
		factory: f,
		req: client.CreateContextRequest{
			Name:     f.tc.UniqueName("context"),
			Type:     "1:1",
			Interval: "0 0 * * *",
		},
	}
}

// WithName sets the context name.
func (b *ContextBuilder) WithName(name string) *ContextBuilder {
	b.req.Name = b.factory.tc.UniqueName(name)
	return b
}

// WithRawName sets the context name without prefix.
func (b *ContextBuilder) WithRawName(name string) *ContextBuilder {
	b.req.Name = name
	return b
}

// GetRequest returns the underlying request for inspection.
func (b *ContextBuilder) GetRequest() client.CreateContextRequest {
	return b.req
}

// WithType sets the context type.
func (b *ContextBuilder) WithType(contextType string) *ContextBuilder {
	b.req.Type = contextType
	return b
}

// OneToOne sets the context type to 1:1.
func (b *ContextBuilder) OneToOne() *ContextBuilder {
	return b.WithType("1:1")
}

// OneToMany sets the context type to 1:N.
func (b *ContextBuilder) OneToMany() *ContextBuilder {
	return b.WithType("1:N")
}

// ManyToMany sets the context type to N:M.
func (b *ContextBuilder) ManyToMany() *ContextBuilder {
	return b.WithType("N:M")
}

// WithInterval sets the reconciliation interval.
func (b *ContextBuilder) WithInterval(interval string) *ContextBuilder {
	b.req.Interval = interval
	return b
}

// WithDescription sets the context description.
func (b *ContextBuilder) WithDescription(desc string) *ContextBuilder {
	b.req.Description = desc
	return b
}

// WithFeeNormalization sets the fee normalization mode for the context.
func (b *ContextBuilder) WithFeeNormalization(mode string) *ContextBuilder {
	b.req.FeeNormalization = mode
	return b
}

// WithTenantID sets the tenant ID.
func (b *ContextBuilder) WithTenantID(tenantID string) *ContextBuilder {
	return b
}

// WithoutActivation prevents the factory from auto-activating the context after creation.
// Use this when testing DRAFT-status behavior (e.g., verifying that operations reject DRAFT contexts).
func (b *ContextBuilder) WithoutActivation() *ContextBuilder {
	b.skipActivation = true
	return b
}

// Create creates the context, activates it (unless WithoutActivation was called), and registers cleanup.
func (b *ContextBuilder) Create(ctx context.Context) (*client.Context, error) {
	created, err := b.factory.client.Configuration.CreateContext(ctx, b.req)
	if err != nil {
		return nil, err
	}

	result := created

	if !b.skipActivation {
		// Activate the context so ingestion/matching/reporting verifiers accept it.
		// New contexts are created in DRAFT status; most E2E tests need ACTIVE.
		activeStatus := "ACTIVE"

		activated, activateErr := b.factory.client.Configuration.UpdateContext(ctx, created.ID, client.UpdateContextRequest{
			Status: &activeStatus,
		})
		if activateErr != nil {
			return nil, fmt.Errorf("activate context %s: %w", created.ID, activateErr)
		}

		result = activated
	}

	b.factory.tc.RegisterCleanup(func() error {
		return b.factory.client.Configuration.DeleteContext(context.Background(), result.ID)
	})

	b.factory.tc.Logf("Created context: %s (%s)", result.Name, result.ID)
	return result, nil
}

// MustCreate creates the context and panics on error.
func (b *ContextBuilder) MustCreate(ctx context.Context) *client.Context {
	created, err := b.Create(ctx)
	if err != nil {
		panic(err)
	}
	return created
}
