// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package ports

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockTenantLister is a test double for TenantLister.
type mockTenantLister struct {
	tenants []string
	err     error
}

func (m *mockTenantLister) ListTenants(_ context.Context) ([]string, error) {
	return m.tenants, m.err
}

func TestTenantLister_InterfaceCompliance(t *testing.T) {
	t.Parallel()

	var lister TenantLister = &mockTenantLister{
		tenants: []string{"tenant-1", "tenant-2"},
	}

	tenants, err := lister.ListTenants(context.Background())

	require.NoError(t, err)
	assert.Len(t, tenants, 2)
	assert.Equal(t, "tenant-1", tenants[0])
	assert.Equal(t, "tenant-2", tenants[1])
}

func TestTenantLister_EmptyTenants(t *testing.T) {
	t.Parallel()

	var lister TenantLister = &mockTenantLister{
		tenants: []string{},
	}

	tenants, err := lister.ListTenants(context.Background())

	require.NoError(t, err)
	assert.Empty(t, tenants)
}

func TestTenantLister_Error(t *testing.T) {
	t.Parallel()

	expectedErr := errors.New("database unreachable")

	var lister TenantLister = &mockTenantLister{
		err: expectedErr,
	}

	tenants, err := lister.ListTenants(context.Background())

	assert.ErrorIs(t, err, expectedErr)
	assert.Nil(t, tenants)
}
