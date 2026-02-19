//go:build unit

package ports_test

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"

	"github.com/LerianStudio/matcher/internal/ingestion/ports"
)

// Compile-time interface satisfaction checks.
var (
	_ ports.MatchTrigger    = (*mockMatchTrigger)(nil)
	_ ports.ContextProvider = (*mockContextProvider)(nil)
)

type mockMatchTrigger struct {
	called    bool
	tenantID  uuid.UUID
	contextID uuid.UUID
}

func (m *mockMatchTrigger) TriggerMatchForContext(_ context.Context, tenantID, contextID uuid.UUID) {
	m.called = true
	m.tenantID = tenantID
	m.contextID = contextID
}

type mockContextProvider struct {
	enabled bool
	err     error
}

func (m *mockContextProvider) IsAutoMatchEnabled(_ context.Context, _ uuid.UUID) (bool, error) {
	return m.enabled, m.err
}

func TestMatchTriggerAlias_InterfaceSatisfaction(t *testing.T) {
	t.Parallel()

	trigger := &mockMatchTrigger{}
	tenantID := uuid.New()
	contextID := uuid.New()

	var mt ports.MatchTrigger = trigger
	mt.TriggerMatchForContext(context.Background(), tenantID, contextID)

	assert.True(t, trigger.called)
	assert.Equal(t, tenantID, trigger.tenantID)
	assert.Equal(t, contextID, trigger.contextID)
}

func TestContextProviderAlias_InterfaceSatisfaction(t *testing.T) {
	t.Parallel()

	provider := &mockContextProvider{enabled: true}

	var cp ports.ContextProvider = provider
	enabled, err := cp.IsAutoMatchEnabled(context.Background(), uuid.New())

	assert.NoError(t, err)
	assert.True(t, enabled)
}

func TestContextProviderAlias_ReturnsFalse(t *testing.T) {
	t.Parallel()

	provider := &mockContextProvider{enabled: false}

	var cp ports.ContextProvider = provider
	enabled, err := cp.IsAutoMatchEnabled(context.Background(), uuid.New())

	assert.NoError(t, err)
	assert.False(t, enabled)
}

func TestContextProviderAlias_ReturnsError(t *testing.T) {
	t.Parallel()

	expectedErr := assert.AnError
	provider := &mockContextProvider{err: expectedErr}

	var cp ports.ContextProvider = provider
	_, err := cp.IsAutoMatchEnabled(context.Background(), uuid.New())

	assert.ErrorIs(t, err, expectedErr)
}
