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
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/matching/ports"
	"github.com/LerianStudio/matcher/internal/shared/domain/fee"
)

// mockFeeRuleProvider is a minimal test double for FeeRuleProvider.
type mockFeeRuleProvider struct {
	rules []*fee.FeeRule
	err   error
}

// Compile-time interface compliance check.
var _ ports.FeeRuleProvider = (*mockFeeRuleProvider)(nil)

func (m *mockFeeRuleProvider) FindByContextID(
	_ context.Context,
	_ uuid.UUID,
) ([]*fee.FeeRule, error) {
	return m.rules, m.err
}

func TestFeeRuleProvider_InterfaceCompliance(t *testing.T) {
	t.Parallel()

	var provider ports.FeeRuleProvider = &mockFeeRuleProvider{}
	require.NotNil(t, provider)
}

func TestFeeRuleProvider_FindByContextID_Success(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	contextID := uuid.New()

	expectedRules := []*fee.FeeRule{
		{ID: uuid.New(), ContextID: contextID, Name: "rule-1"},
		{ID: uuid.New(), ContextID: contextID, Name: "rule-2"},
	}

	mock := &mockFeeRuleProvider{rules: expectedRules}

	rules, err := mock.FindByContextID(ctx, contextID)
	require.NoError(t, err)
	require.Len(t, rules, 2)
	require.Equal(t, expectedRules[0].ID, rules[0].ID)
	require.Equal(t, expectedRules[1].ID, rules[1].ID)
}

func TestFeeRuleProvider_FindByContextID_EmptyResult(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	contextID := uuid.New()

	mock := &mockFeeRuleProvider{rules: nil}

	rules, err := mock.FindByContextID(ctx, contextID)
	require.NoError(t, err)
	require.Nil(t, rules)
}

func TestFeeRuleProvider_FindByContextID_Error(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	contextID := uuid.New()
	expectedErr := errors.New("database connection failed")

	mock := &mockFeeRuleProvider{err: expectedErr}

	rules, err := mock.FindByContextID(ctx, contextID)
	require.ErrorIs(t, err, expectedErr)
	require.Nil(t, rules)
}
