//go:build unit

package entities

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	matchingVO "github.com/LerianStudio/matcher/internal/matching/domain/value_objects"
)

func TestMatchConfirmedEvent_JSONStability(t *testing.T) {
	t.Parallel()

	tenantID := uuid.MustParse("11111111-1111-1111-1111-111111111111")
	contextID := uuid.MustParse("22222222-2222-2222-2222-222222222222")
	runID := uuid.MustParse("33333333-3333-3333-3333-333333333333")
	matchID := uuid.MustParse("44444444-4444-4444-4444-444444444444")
	ruleID := uuid.MustParse("55555555-5555-5555-5555-555555555555")
	txA := uuid.MustParse("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
	txB := uuid.MustParse("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb")

	confirmedAt := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	timestamp := time.Date(2026, 1, 2, 3, 4, 6, 0, time.UTC)

	confidence, err := matchingVO.ParseConfidenceScore(95)
	require.NoError(t, err)

	group := &MatchGroup{
		ID:         matchID,
		ContextID:  contextID,
		RunID:      runID,
		RuleID:     ruleID,
		Confidence: confidence,
		Items: []*MatchItem{
			{TransactionID: txB},
			{TransactionID: txA},
		},
		ConfirmedAt: &confirmedAt,
	}

	ev, err := NewMatchConfirmedEvent(t.Context(), tenantID, "default", group, timestamp)
	require.NoError(t, err)

	raw, err := json.Marshal(ev)
	require.NoError(t, err)

	expected := `{"eventType":"matching.match_confirmed","tenantId":"11111111-1111-1111-1111-111111111111","tenantSlug":"default","contextId":"22222222-2222-2222-2222-222222222222","runId":"33333333-3333-3333-3333-333333333333","matchId":"44444444-4444-4444-4444-444444444444","ruleId":"55555555-5555-5555-5555-555555555555","transactionIds":["aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa","bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"],"confidence":95,"confirmedAt":"2026-01-02T03:04:05Z","timestamp":"2026-01-02T03:04:06Z"}`
	require.Equal(t, expected, string(raw))
}

func TestNewMatchConfirmedEvent_ErrorPaths(t *testing.T) {
	t.Parallel()

	confirmedAt := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	timestamp := time.Date(2026, 1, 2, 3, 4, 6, 0, time.UTC)
	tenantID := uuid.MustParse("11111111-1111-1111-1111-111111111111")
	confidence, _ := matchingVO.ParseConfidenceScore(95)

	validGroup := func() *MatchGroup {
		return &MatchGroup{
			ID:         uuid.MustParse("44444444-4444-4444-4444-444444444444"),
			ContextID:  uuid.MustParse("22222222-2222-2222-2222-222222222222"),
			RunID:      uuid.MustParse("33333333-3333-3333-3333-333333333333"),
			RuleID:     uuid.MustParse("55555555-5555-5555-5555-555555555555"),
			Confidence: confidence,
			Items: []*MatchItem{
				{TransactionID: uuid.MustParse("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")},
			},
			ConfirmedAt: &confirmedAt,
		}
	}

	tests := []struct {
		name           string
		tenantID       uuid.UUID
		group          func() *MatchGroup
		expectedErr    error
		expectedErrMsg string
	}{
		{
			name:           "nil tenant ID",
			tenantID:       uuid.Nil,
			group:          validGroup,
			expectedErrMsg: ErrMatchConfirmedTenantIDRequired.Error(),
		},
		{
			name:           "nil group",
			tenantID:       tenantID,
			group:          func() *MatchGroup { return nil },
			expectedErrMsg: ErrMatchConfirmedGroupRequired.Error(),
		},
		{
			name:        "nil context ID",
			tenantID:    tenantID,
			group:       func() *MatchGroup { g := validGroup(); g.ContextID = uuid.Nil; return g },
			expectedErr: ErrMatchConfirmedContextIDRequired,
		},
		{
			name:        "nil run ID",
			tenantID:    tenantID,
			group:       func() *MatchGroup { g := validGroup(); g.RunID = uuid.Nil; return g },
			expectedErr: ErrMatchConfirmedRunIDRequired,
		},
		{
			name:        "nil match ID",
			tenantID:    tenantID,
			group:       func() *MatchGroup { g := validGroup(); g.ID = uuid.Nil; return g },
			expectedErr: ErrMatchConfirmedMatchIDRequired,
		},
		{
			name:        "nil confirmed at",
			tenantID:    tenantID,
			group:       func() *MatchGroup { g := validGroup(); g.ConfirmedAt = nil; return g },
			expectedErr: ErrMatchConfirmedConfirmedAtRequired,
		},
		{
			name:        "empty items",
			tenantID:    tenantID,
			group:       func() *MatchGroup { g := validGroup(); g.Items = nil; return g },
			expectedErr: ErrMatchConfirmedTransactionIDsRequired,
		},
		{
			name:        "items with nil transaction IDs",
			tenantID:    tenantID,
			group:       func() *MatchGroup { g := validGroup(); g.Items = []*MatchItem{{TransactionID: uuid.Nil}}; return g },
			expectedErr: ErrMatchConfirmedTransactionIDsRequired,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			_, err := NewMatchConfirmedEvent(
				context.Background(),
				tc.tenantID,
				"default",
				tc.group(),
				timestamp,
			)
			require.Error(t, err)

			if tc.expectedErr != nil {
				require.ErrorIs(t, err, tc.expectedErr)
			}

			if tc.expectedErrMsg != "" {
				require.Contains(t, err.Error(), tc.expectedErrMsg)
			}
		})
	}
}

func TestMatchUnmatchedEvent_JSONStability(t *testing.T) {
	t.Parallel()

	tenantID := uuid.MustParse("11111111-1111-1111-1111-111111111111")
	contextID := uuid.MustParse("22222222-2222-2222-2222-222222222222")
	runID := uuid.MustParse("33333333-3333-3333-3333-333333333333")
	matchID := uuid.MustParse("44444444-4444-4444-4444-444444444444")
	ruleID := uuid.MustParse("55555555-5555-5555-5555-555555555555")
	txA := uuid.MustParse("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
	txB := uuid.MustParse("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb")

	timestamp := time.Date(2026, 1, 2, 3, 4, 6, 0, time.UTC)

	group := &MatchGroup{
		ID:        matchID,
		ContextID: contextID,
		RunID:     runID,
		RuleID:    ruleID,
		Items: []*MatchItem{
			{TransactionID: txB},
			{TransactionID: txA},
		},
	}

	ev, err := NewMatchUnmatchedEvent(t.Context(), tenantID, "default", group, "duplicate detected", timestamp)
	require.NoError(t, err)

	raw, err := json.Marshal(ev)
	require.NoError(t, err)

	expected := `{"eventType":"matching.match_unmatched","tenantId":"11111111-1111-1111-1111-111111111111","tenantSlug":"default","contextId":"22222222-2222-2222-2222-222222222222","runId":"33333333-3333-3333-3333-333333333333","matchId":"44444444-4444-4444-4444-444444444444","ruleId":"55555555-5555-5555-5555-555555555555","transactionIds":["aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa","bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"],"reason":"duplicate detected","timestamp":"2026-01-02T03:04:06Z"}`
	require.Equal(t, expected, string(raw))
}

func TestMatchUnmatchedEvent_ID(t *testing.T) {
	t.Parallel()

	matchID := uuid.MustParse("44444444-4444-4444-4444-444444444444")
	ev := MatchUnmatchedEvent{MatchID: matchID}
	require.Equal(t, matchID, ev.ID())
}

func TestNewMatchUnmatchedEvent_Success(t *testing.T) {
	t.Parallel()

	tenantID := uuid.MustParse("11111111-1111-1111-1111-111111111111")
	timestamp := time.Date(2026, 1, 2, 3, 4, 6, 0, time.UTC)

	group := &MatchGroup{
		ID:        uuid.MustParse("44444444-4444-4444-4444-444444444444"),
		ContextID: uuid.MustParse("22222222-2222-2222-2222-222222222222"),
		RunID:     uuid.MustParse("33333333-3333-3333-3333-333333333333"),
		RuleID:    uuid.MustParse("55555555-5555-5555-5555-555555555555"),
		Items: []*MatchItem{
			{TransactionID: uuid.MustParse("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")},
		},
	}

	ev, err := NewMatchUnmatchedEvent(t.Context(), tenantID, "default", group, "revoked by admin", timestamp)
	require.NoError(t, err)
	require.NotNil(t, ev)
	require.Equal(t, EventTypeMatchUnmatched, ev.EventType)
	require.Equal(t, tenantID, ev.TenantID)
	require.Equal(t, "default", ev.TenantSlug)
	require.Equal(t, group.ContextID, ev.ContextID)
	require.Equal(t, group.RunID, ev.RunID)
	require.Equal(t, group.ID, ev.MatchID)
	require.Equal(t, group.RuleID, ev.RuleID)
	require.Equal(t, "revoked by admin", ev.Reason)
	require.Equal(t, timestamp.UTC(), ev.Timestamp)
	require.Len(t, ev.TransactionIDs, 1)
}

func TestNewMatchUnmatchedEvent_TrimsReasonWhitespace(t *testing.T) {
	t.Parallel()

	tenantID := uuid.MustParse("11111111-1111-1111-1111-111111111111")
	timestamp := time.Date(2026, 1, 2, 3, 4, 6, 0, time.UTC)

	group := &MatchGroup{
		ID:        uuid.MustParse("44444444-4444-4444-4444-444444444444"),
		ContextID: uuid.MustParse("22222222-2222-2222-2222-222222222222"),
		RunID:     uuid.MustParse("33333333-3333-3333-3333-333333333333"),
		Items: []*MatchItem{
			{TransactionID: uuid.MustParse("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")},
		},
	}

	ev, err := NewMatchUnmatchedEvent(t.Context(), tenantID, "default", group, "  padded reason  ", timestamp)
	require.NoError(t, err)
	require.Equal(t, "padded reason", ev.Reason)
}

func TestNewMatchUnmatchedEvent_ErrorPaths(t *testing.T) {
	t.Parallel()

	timestamp := time.Date(2026, 1, 2, 3, 4, 6, 0, time.UTC)
	tenantID := uuid.MustParse("11111111-1111-1111-1111-111111111111")

	validGroup := func() *MatchGroup {
		return &MatchGroup{
			ID:        uuid.MustParse("44444444-4444-4444-4444-444444444444"),
			ContextID: uuid.MustParse("22222222-2222-2222-2222-222222222222"),
			RunID:     uuid.MustParse("33333333-3333-3333-3333-333333333333"),
			RuleID:    uuid.MustParse("55555555-5555-5555-5555-555555555555"),
			Items: []*MatchItem{
				{TransactionID: uuid.MustParse("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")},
			},
		}
	}

	tests := []struct {
		name           string
		tenantID       uuid.UUID
		group          func() *MatchGroup
		reason         string
		expectedErr    error
		expectedErrMsg string
	}{
		{
			name:           "nil tenant ID",
			tenantID:       uuid.Nil,
			group:          validGroup,
			reason:         "some reason",
			expectedErrMsg: ErrMatchUnmatchedTenantIDRequired.Error(),
		},
		{
			name:           "nil group",
			tenantID:       tenantID,
			group:          func() *MatchGroup { return nil },
			reason:         "some reason",
			expectedErrMsg: ErrMatchUnmatchedGroupRequired.Error(),
		},
		{
			name:        "nil context ID",
			tenantID:    tenantID,
			group:       func() *MatchGroup { g := validGroup(); g.ContextID = uuid.Nil; return g },
			reason:      "some reason",
			expectedErr: ErrMatchUnmatchedContextIDRequired,
		},
		{
			name:        "nil run ID",
			tenantID:    tenantID,
			group:       func() *MatchGroup { g := validGroup(); g.RunID = uuid.Nil; return g },
			reason:      "some reason",
			expectedErr: ErrMatchUnmatchedRunIDRequired,
		},
		{
			name:        "nil match ID",
			tenantID:    tenantID,
			group:       func() *MatchGroup { g := validGroup(); g.ID = uuid.Nil; return g },
			reason:      "some reason",
			expectedErr: ErrMatchUnmatchedMatchIDRequired,
		},
		{
			name:        "empty items",
			tenantID:    tenantID,
			group:       func() *MatchGroup { g := validGroup(); g.Items = nil; return g },
			reason:      "some reason",
			expectedErr: ErrMatchUnmatchedTransactionIDsRequired,
		},
		{
			name:        "items with nil transaction IDs",
			tenantID:    tenantID,
			group:       func() *MatchGroup { g := validGroup(); g.Items = []*MatchItem{{TransactionID: uuid.Nil}}; return g },
			reason:      "some reason",
			expectedErr: ErrMatchUnmatchedTransactionIDsRequired,
		},
		{
			name:        "empty reason",
			tenantID:    tenantID,
			group:       validGroup,
			reason:      "",
			expectedErr: ErrMatchUnmatchedReasonRequired,
		},
		{
			name:        "whitespace-only reason",
			tenantID:    tenantID,
			group:       validGroup,
			reason:      "   \t\n  ",
			expectedErr: ErrMatchUnmatchedReasonRequired,
		},
		{
			name:        "reason exceeds max length",
			tenantID:    tenantID,
			group:       validGroup,
			reason:      strings.Repeat("a", maxReasonLength+1),
			expectedErr: ErrMatchUnmatchedReasonTooLong,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			_, err := NewMatchUnmatchedEvent(
				context.Background(),
				tc.tenantID,
				"default",
				tc.group(),
				tc.reason,
				timestamp,
			)
			require.Error(t, err)

			if tc.expectedErr != nil {
				require.ErrorIs(t, err, tc.expectedErr)
			}

			if tc.expectedErrMsg != "" {
				require.Contains(t, err.Error(), tc.expectedErrMsg)
			}
		})
	}
}

func TestNewMatchUnmatchedEvent_ReasonAtMaxLength(t *testing.T) {
	t.Parallel()

	tenantID := uuid.MustParse("11111111-1111-1111-1111-111111111111")
	timestamp := time.Date(2026, 1, 2, 3, 4, 6, 0, time.UTC)

	group := &MatchGroup{
		ID:        uuid.MustParse("44444444-4444-4444-4444-444444444444"),
		ContextID: uuid.MustParse("22222222-2222-2222-2222-222222222222"),
		RunID:     uuid.MustParse("33333333-3333-3333-3333-333333333333"),
		Items: []*MatchItem{
			{TransactionID: uuid.MustParse("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")},
		},
	}

	maxReason := strings.Repeat("r", maxReasonLength)
	ev, err := NewMatchUnmatchedEvent(t.Context(), tenantID, "default", group, maxReason, timestamp)
	require.NoError(t, err)
	require.Equal(t, maxReason, ev.Reason)
}
