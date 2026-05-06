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
	"github.com/LerianStudio/lib-streaming/streamingtest"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/ingestion/domain/entities"
	ingestionVO "github.com/LerianStudio/matcher/internal/ingestion/domain/value_objects"
)

func TestFormatIngestionTimeUsesUTCAndRFC3339Nano(t *testing.T) {
	input := time.Date(2026, time.May, 4, 10, 11, 12, 13, time.FixedZone("BRT", -3*60*60))

	formatted := formatIngestionTime(input)

	parsed, err := time.Parse(time.RFC3339Nano, formatted)
	require.NoError(t, err)
	assert.Equal(t, input.UTC(), parsed)
}

func TestEmitIngestionTerminalEventsUseEventSpecificTimestampFields(t *testing.T) {
	tenantID := "018f4f95-0000-7000-8000-000000000001"
	completedAt := time.Date(2026, time.May, 4, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		name          string
		definitionKey string
		status        ingestionVO.JobStatus
		wantKey       string
		forbiddenKey  string
	}{
		{
			name:          "completed only includes completed_at",
			definitionKey: "ingestion.completed",
			status:        ingestionVO.JobStatusCompleted,
			wantKey:       "completed_at",
			forbiddenKey:  "failed_at",
		},
		{
			name:          "failed only includes failed_at",
			definitionKey: "ingestion.failed",
			status:        ingestionVO.JobStatusFailed,
			wantKey:       "failed_at",
			forbiddenKey:  "completed_at",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			emitter := streamingtest.NewMockEmitter()
			uc := &UseCase{streamEmitter: emitter}
			ctx := tmcore.ContextWithTenantID(context.Background(), tenantID)

			uc.emitIngestionEvent(ctx, nil, tt.definitionKey, &entities.IngestionJob{
				ID:          uuid.New(),
				ContextID:   uuid.New(),
				SourceID:    uuid.New(),
				Status:      tt.status,
				CompletedAt: &completedAt,
			}, nil)

			requests := emitter.Requests()
			require.Len(t, requests, 1)

			var payload map[string]any
			require.NoError(t, json.Unmarshal(requests[0].Payload, &payload))
			require.Contains(t, payload, tt.wantKey)
			require.NotContains(t, payload, tt.forbiddenKey)
		})
	}
}
