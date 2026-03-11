//go:build unit

package ports_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/LerianStudio/matcher/internal/discovery/ports"
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

// mockSchemaCache is a test double for the SchemaCache interface.
type mockSchemaCache struct {
	getSchemaFunc func(ctx context.Context, connectionID string) (*sharedPorts.FetcherSchema, error)
	setSchemaFunc func(ctx context.Context, connectionID string, schema *sharedPorts.FetcherSchema, ttl time.Duration) error
}

func (m *mockSchemaCache) GetSchema(ctx context.Context, connectionID string) (*sharedPorts.FetcherSchema, error) {
	if m.getSchemaFunc != nil {
		return m.getSchemaFunc(ctx, connectionID)
	}

	return nil, nil
}

func (m *mockSchemaCache) SetSchema(ctx context.Context, connectionID string, schema *sharedPorts.FetcherSchema, ttl time.Duration) error {
	if m.setSchemaFunc != nil {
		return m.setSchemaFunc(ctx, connectionID, schema, ttl)
	}

	return nil
}

// Compile-time interface compliance check.
var _ ports.SchemaCache = (*mockSchemaCache)(nil)

func TestSchemaCache_InterfaceCompliance(t *testing.T) {
	t.Parallel()

	// Verify that the mock implements the interface correctly.
	var cache ports.SchemaCache = &mockSchemaCache{}
	assert.NotNil(t, cache)
}

func TestSchemaCache_GetSchema(t *testing.T) {
	t.Parallel()

	expectedSchema := &sharedPorts.FetcherSchema{
		ConnectionID: "test-conn",
		Tables: []sharedPorts.FetcherTableSchema{
			{
				TableName: "users",
				Columns: []sharedPorts.FetcherColumnInfo{
					{Name: "id", Type: "uuid", Nullable: false},
				},
			},
		},
	}

	cache := &mockSchemaCache{
		getSchemaFunc: func(_ context.Context, connectionID string) (*sharedPorts.FetcherSchema, error) {
			if connectionID == "test-conn" {
				return expectedSchema, nil
			}

			return nil, nil
		},
	}

	result, err := cache.GetSchema(context.Background(), "test-conn")
	assert.NoError(t, err)
	assert.Equal(t, expectedSchema, result)
}

func TestSchemaCache_SetSchema(t *testing.T) {
	t.Parallel()

	var capturedConnectionID string

	var capturedTTL time.Duration

	cache := &mockSchemaCache{
		setSchemaFunc: func(_ context.Context, connectionID string, _ *sharedPorts.FetcherSchema, ttl time.Duration) error {
			capturedConnectionID = connectionID
			capturedTTL = ttl

			return nil
		},
	}

	schema := &sharedPorts.FetcherSchema{ConnectionID: "test-conn"}
	err := cache.SetSchema(context.Background(), "test-conn", schema, 5*time.Minute)

	assert.NoError(t, err)
	assert.Equal(t, "test-conn", capturedConnectionID)
	assert.Equal(t, 5*time.Minute, capturedTTL)
}
