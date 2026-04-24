// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package ports_test

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"

	"github.com/LerianStudio/matcher/internal/shared/infrastructure/testutil"
	"github.com/LerianStudio/matcher/internal/shared/ports"
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

func TestMatchTriggerInterfaceSatisfaction(t *testing.T) {
	t.Parallel()

	trigger := &mockMatchTrigger{}
	tenantID := testutil.DeterministicUUID("trigger-tenant")
	contextID := testutil.DeterministicUUID("trigger-context")

	var mt ports.MatchTrigger = trigger
	mt.TriggerMatchForContext(context.Background(), tenantID, contextID)

	assert.True(t, trigger.called)
	assert.Equal(t, tenantID, trigger.tenantID)
	assert.Equal(t, contextID, trigger.contextID)
}

func TestContextProviderInterfaceSatisfaction(t *testing.T) {
	t.Parallel()

	provider := &mockContextProvider{enabled: true}

	var cp ports.ContextProvider = provider
	enabled, err := cp.IsAutoMatchEnabled(context.Background(), testutil.DeterministicUUID("provider-context"))

	assert.NoError(t, err)
	assert.True(t, enabled)
}

func TestContextProviderInterfaceSatisfaction_ErrorPath(t *testing.T) {
	t.Parallel()

	expectedErr := errors.New("test error")
	provider := &mockContextProvider{enabled: false, err: expectedErr}

	var cp ports.ContextProvider = provider
	enabled, err := cp.IsAutoMatchEnabled(context.Background(), testutil.DeterministicUUID("error-path-context"))

	assert.ErrorIs(t, err, expectedErr)
	assert.False(t, enabled)
}
