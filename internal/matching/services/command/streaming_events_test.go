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

	matchingEntities "github.com/LerianStudio/matcher/internal/matching/domain/entities"
)

func TestMatchItemTransactionIDsSkipsNilItems(t *testing.T) {
	firstID := uuid.New()
	secondID := uuid.New()
	items := []*matchingEntities.MatchItem{
		{TransactionID: firstID},
		nil,
		{TransactionID: secondID},
	}

	ids := matchItemTransactionIDs(items)

	assert.Equal(t, []string{firstID.String(), secondID.String()}, ids)
}

func TestFormatMatchingTimeUsesUTCAndRFC3339Nano(t *testing.T) {
	input := time.Date(2026, time.May, 4, 10, 11, 12, 13, time.FixedZone("BRT", -3*60*60))

	formatted := formatMatchingTime(input)

	parsed, err := time.Parse(time.RFC3339Nano, formatted)
	require.NoError(t, err)
	assert.Equal(t, input.UTC(), parsed)
}

func TestEmitTransactionMatchStatusUsesEventSpecificTimestampFields(t *testing.T) {
	tenantID := "018f4f95-0000-7000-8000-000000000001"
	run := &matchingEntities.MatchRun{ID: uuid.New(), ContextID: uuid.New()}
	tests := []struct {
		name          string
		definitionKey string
		status        string
		wantKey       string
		forbiddenKey  string
	}{
		{
			name:          "matched only includes matched_at",
			definitionKey: "transaction.matched",
			status:        "MATCHED",
			wantKey:       "matched_at",
			forbiddenKey:  "pending_review_at",
		},
		{
			name:          "pending review only includes pending_review_at",
			definitionKey: "transaction.pending_review",
			status:        "PENDING_REVIEW",
			wantKey:       "pending_review_at",
			forbiddenKey:  "matched_at",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			emitter := streamingtest.NewMockEmitter()
			uc := &UseCase{streamEmitter: emitter}
			ctx := tmcore.ContextWithTenantID(context.Background(), tenantID)

			uc.emitTransactionMatchStatus(ctx, nil, tt.definitionKey, uuid.New(), run, nil, nil, tt.status)

			requests := emitter.Requests()
			require.Len(t, requests, 1)

			var payload map[string]any
			require.NoError(t, json.Unmarshal(requests[0].Payload, &payload))
			require.Contains(t, payload, tt.wantKey)
			require.NotContains(t, payload, tt.forbiddenKey)
		})
	}
}

func TestEmitMatchGroupUnmatchedUsesPersistedGroupUpdatedAt(t *testing.T) {
	tenantID := "018f4f95-0000-7000-8000-000000000001"
	emitter := streamingtest.NewMockEmitter()
	uc := &UseCase{streamEmitter: emitter}
	persistedUpdatedAt := time.Date(2026, time.May, 4, 12, 30, 45, 123, time.UTC)
	group := &matchingEntities.MatchGroup{
		ID:        uuid.New(),
		RunID:     uuid.New(),
		ContextID: uuid.New(),
		RuleID:    uuid.New(),
		UpdatedAt: persistedUpdatedAt,
		Items: []*matchingEntities.MatchItem{
			{TransactionID: uuid.New()},
		},
	}

	ctx := tmcore.ContextWithTenantID(context.Background(), tenantID)
	uc.emitMatchGroupUnmatched(ctx, nil, group, "operator correction")

	requests := emitter.Requests()
	require.Len(t, requests, 1)

	var payload map[string]any
	require.NoError(t, json.Unmarshal(requests[0].Payload, &payload))
	assert.Equal(t, formatMatchingTime(persistedUpdatedAt), payload["unmatched_at"])
}
