// Copyright 2026 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package command

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"

	tmcore "github.com/LerianStudio/lib-commons/v5/commons/tenant-manager/core"

	"github.com/LerianStudio/matcher/internal/auth"
)

func TestContextWithTriggerTenantSetsLegacyAndTenantManagerContexts(t *testing.T) {
	tenantID := uuid.MustParse("550e8400-e29b-41d4-a716-446655440000")

	ctx := contextWithTriggerTenant(context.Background(), tenantID)

	assert.Equal(t, tenantID.String(), auth.GetTenantID(ctx))
	assert.Equal(t, tenantID.String(), tmcore.GetTenantIDContext(ctx))
}
