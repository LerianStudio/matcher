//go:build e2e

package factories

import (
	"context"

	"github.com/LerianStudio/matcher/tests/e2e"
	"github.com/LerianStudio/matcher/tests/e2e/client"
)

// SourceFactory creates reconciliation sources for tests.
type SourceFactory struct {
	tc     *e2e.TestContext
	client *e2e.Client
}

// NewSourceFactory creates a new source factory.
func NewSourceFactory(tc *e2e.TestContext, c *e2e.Client) *SourceFactory {
	return &SourceFactory{tc: tc, client: c}
}

// Client returns the underlying API client.
func (f *SourceFactory) Client() *e2e.Client {
	return f.client
}

// SourceBuilder builds source creation requests.
type SourceBuilder struct {
	factory   *SourceFactory
	contextID string
	req       client.CreateSourceRequest
}

// NewSource starts building a new source.
func (f *SourceFactory) NewSource(contextID string) *SourceBuilder {
	return &SourceBuilder{
		factory:   f,
		contextID: contextID,
		req: client.CreateSourceRequest{
			Name:   f.tc.UniqueName("source"),
			Type:   "LEDGER",
			Config: map[string]any{},
		},
	}
}

// WithName sets the source name.
func (b *SourceBuilder) WithName(name string) *SourceBuilder {
	b.req.Name = b.factory.tc.UniqueName(name)
	return b
}

// WithRawName sets the source name without prefix.
func (b *SourceBuilder) WithRawName(name string) *SourceBuilder {
	b.req.Name = name
	return b
}

// GetRequest returns the underlying request for inspection.
func (b *SourceBuilder) GetRequest() client.CreateSourceRequest {
	return b.req
}

// WithType sets the source type.
func (b *SourceBuilder) WithType(sourceType string) *SourceBuilder {
	b.req.Type = sourceType
	return b
}

// AsLedger sets the source type to LEDGER.
func (b *SourceBuilder) AsLedger() *SourceBuilder {
	return b.WithType("LEDGER")
}

// AsBank sets the source type to BANK.
func (b *SourceBuilder) AsBank() *SourceBuilder {
	return b.WithType("BANK")
}

// AsGateway sets the source type to GATEWAY.
func (b *SourceBuilder) AsGateway() *SourceBuilder {
	return b.WithType("GATEWAY")
}

// WithConfig sets the source configuration.
func (b *SourceBuilder) WithConfig(config map[string]any) *SourceBuilder {
	b.req.Config = config
	return b
}

// Create creates the source and registers cleanup.
func (b *SourceBuilder) Create(ctx context.Context) (*client.Source, error) {
	created, err := b.factory.client.Configuration.CreateSource(ctx, b.contextID, b.req)
	if err != nil {
		return nil, err
	}

	b.factory.tc.RegisterCleanup(func() error {
		return b.factory.client.Configuration.DeleteSource(
			context.Background(),
			b.contextID,
			created.ID,
		)
	})

	b.factory.tc.Logf("Created source: %s (%s)", created.Name, created.ID)
	return created, nil
}

// MustCreate creates the source and panics on error.
func (b *SourceBuilder) MustCreate(ctx context.Context) *client.Source {
	created, err := b.Create(ctx)
	if err != nil {
		panic(err)
	}
	return created
}

// FieldMapBuilder builds field map creation requests.
type FieldMapBuilder struct {
	factory   *SourceFactory
	contextID string
	sourceID  string
	req       client.CreateFieldMapRequest
}

// NewFieldMap starts building a new field map.
func (f *SourceFactory) NewFieldMap(contextID, sourceID string) *FieldMapBuilder {
	return &FieldMapBuilder{
		factory:   f,
		contextID: contextID,
		sourceID:  sourceID,
		req: client.CreateFieldMapRequest{
			Mapping: map[string]any{
				"id":          "external_id",
				"amount":      "amount",
				"currency":    "currency",
				"date":        "date",
				"description": "description",
			},
		},
	}
}

// WithMapping sets the field mapping.
func (b *FieldMapBuilder) WithMapping(mapping map[string]any) *FieldMapBuilder {
	b.req.Mapping = mapping
	return b
}

// WithStandardMapping sets a standard CSV/JSON field mapping.
// The mapping format is: internal_field → source_column
// e.g., "external_id": "id" means the internal field "external_id" maps to CSV column "id".
func (b *FieldMapBuilder) WithStandardMapping() *FieldMapBuilder {
	b.req.Mapping = map[string]any{
		"external_id": "id",
		"amount":      "amount",
		"currency":    "currency",
		"date":        "date",
		"description": "description",
	}
	return b
}

// Create creates the field map and registers cleanup.
func (b *FieldMapBuilder) Create(ctx context.Context) (*client.FieldMap, error) {
	created, err := b.factory.client.Configuration.CreateFieldMap(
		ctx,
		b.contextID,
		b.sourceID,
		b.req,
	)
	if err != nil {
		return nil, err
	}

	b.factory.tc.RegisterCleanup(func() error {
		return b.factory.client.Configuration.DeleteFieldMap(context.Background(), created.ID)
	})

	b.factory.tc.Logf("Created field map: %s", created.ID)
	return created, nil
}

// MustCreate creates the field map and panics on error.
func (b *FieldMapBuilder) MustCreate(ctx context.Context) *client.FieldMap {
	created, err := b.Create(ctx)
	if err != nil {
		panic(err)
	}
	return created
}
