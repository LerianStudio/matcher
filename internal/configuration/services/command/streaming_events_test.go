// Copyright 2026 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package command

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	tmcore "github.com/LerianStudio/lib-commons/v5/commons/tenant-manager/core"
	"github.com/LerianStudio/lib-streaming/v2/streamingtest"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/configuration/domain/entities"
	configurationVO "github.com/LerianStudio/matcher/internal/configuration/domain/value_objects"
	sharedDomain "github.com/LerianStudio/matcher/internal/shared/domain"
)

func TestFormatConfigurationTimeUsesUTCAndRFC3339Nano(t *testing.T) {
	input := time.Date(2026, time.May, 4, 10, 11, 12, 13, time.FixedZone("BRT", -3*60*60))

	formatted := formatConfigurationTime(input)

	parsed, err := time.Parse(time.RFC3339Nano, formatted)
	require.NoError(t, err)
	assert.Equal(t, input.UTC(), parsed)
	assert.Equal(t, "Z", formatted[len(formatted)-1:])
}

func TestHashConfigurationPayloadIsDeterministic(t *testing.T) {
	payload := map[string]any{"priority": 10, "rule_type": "EXACT"}

	first := hashConfigurationPayload(payload)
	second := hashConfigurationPayload(payload)

	assert.Len(t, first, 64)
	assert.Equal(t, first, second)
}

func TestEmitConfigurationEventIncludesTenantIDFromTenantManagerContext(t *testing.T) {
	tenantID := "018f4f95-0000-7000-8000-000000000001"
	emitter := streamingtest.NewMockEmitter()
	uc := &UseCase{streamEmitter: emitter}
	ctx := tmcore.ContextWithTenantID(context.Background(), tenantID)

	uc.emitReconciliationContextCreated(ctx, nil, &entities.ReconciliationContext{
		ID:                uuid.New(),
		Name:              "daily reconciliation",
		Type:              sharedDomain.ContextTypeOneToOne,
		Interval:          "daily",
		Status:            configurationVO.ContextStatusActive,
		AutoMatchOnUpload: true,
		CreatedAt:         time.Date(2026, time.May, 4, 12, 0, 0, 0, time.UTC),
	})

	requests := emitter.Requests()
	require.Len(t, requests, 1)

	var payload map[string]any
	require.NoError(t, json.Unmarshal(requests[0].Payload, &payload))
	require.Equal(t, tenantID, payload["tenant_id"])
}
