// Package entities provides domain entities for the matching bounded context.
package entities

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/LerianStudio/lib-commons/v5/commons/assert"

	"github.com/LerianStudio/matcher/internal/shared/constants"
	sharedDomain "github.com/LerianStudio/matcher/internal/shared/domain"
)

// Event type constants re-exported from shared domain for convenience.
const (
	EventTypeMatchConfirmed = sharedDomain.EventTypeMatchConfirmed
	EventTypeMatchUnmatched = sharedDomain.EventTypeMatchUnmatched
)

// MatchConfirmedEvent is an alias to the shared domain type for internal use.
type MatchConfirmedEvent = sharedDomain.MatchConfirmedEvent

// Sentinel errors for MatchConfirmedEvent validation.
var (
	ErrMatchConfirmedGroupRequired          = errors.New("match group is required")
	ErrMatchConfirmedTenantIDRequired       = errors.New("tenant_id is required")
	ErrMatchConfirmedContextIDRequired      = errors.New("context_id is required")
	ErrMatchConfirmedRunIDRequired          = errors.New("run_id is required")
	ErrMatchConfirmedMatchIDRequired        = errors.New("match_id is required")
	ErrMatchConfirmedTransactionIDsRequired = errors.New("transaction_ids is required")
	ErrMatchConfirmedConfirmedAtRequired    = errors.New("confirmed_at is required")
)

// MatchUnmatchedEvent is an alias to the shared domain type for internal use.
type MatchUnmatchedEvent = sharedDomain.MatchUnmatchedEvent

// maxReasonLength is the maximum length allowed for reason fields in events and domain operations.
const maxReasonLength = 1024

// Sentinel errors for MatchUnmatchedEvent validation.
var (
	ErrMatchUnmatchedGroupRequired          = errors.New("match group is required for unmatched event")
	ErrMatchUnmatchedTenantIDRequired       = errors.New("tenant_id is required for unmatched event")
	ErrMatchUnmatchedContextIDRequired      = errors.New("context_id is required for unmatched event")
	ErrMatchUnmatchedRunIDRequired          = errors.New("run_id is required for unmatched event")
	ErrMatchUnmatchedMatchIDRequired        = errors.New("match_id is required for unmatched event")
	ErrMatchUnmatchedTransactionIDsRequired = errors.New("transaction_ids is required for unmatched event")
	ErrMatchUnmatchedReasonRequired         = errors.New("reason is required for unmatched event")
	ErrMatchUnmatchedReasonTooLong          = errors.New("reason exceeds maximum length for unmatched event")
)

// NewMatchUnmatchedEvent builds a MatchUnmatchedEvent from a revoked MatchGroup.
// Caller provides timestamp for deterministic testing.
func NewMatchUnmatchedEvent(
	ctx context.Context,
	tenantID uuid.UUID,
	tenantSlug string,
	group *MatchGroup,
	reason string,
	timestamp time.Time,
) (*MatchUnmatchedEvent, error) {
	asserter := assert.New(
		ctx,
		nil,
		constants.ApplicationName,
		"matching.events.match_unmatched.new",
	)

	if err := asserter.That(ctx, tenantID != uuid.Nil, ErrMatchUnmatchedTenantIDRequired.Error()); err != nil {
		return nil, fmt.Errorf("match unmatched tenant id: %w", err)
	}

	if err := asserter.NotNil(ctx, group, ErrMatchUnmatchedGroupRequired.Error()); err != nil {
		return nil, fmt.Errorf("match unmatched group: %w", err)
	}

	if group.ContextID == uuid.Nil {
		return nil, ErrMatchUnmatchedContextIDRequired
	}

	if group.RunID == uuid.Nil {
		return nil, ErrMatchUnmatchedRunIDRequired
	}

	if group.ID == uuid.Nil {
		return nil, ErrMatchUnmatchedMatchIDRequired
	}

	txIDs := collectTransactionIDsSorted(group.Items)
	if len(txIDs) == 0 {
		return nil, ErrMatchUnmatchedTransactionIDsRequired
	}

	reason = strings.TrimSpace(reason)

	if reason == "" {
		return nil, ErrMatchUnmatchedReasonRequired
	}

	if len(reason) > maxReasonLength {
		return nil, ErrMatchUnmatchedReasonTooLong
	}

	return &MatchUnmatchedEvent{
		EventType:      EventTypeMatchUnmatched,
		TenantID:       tenantID,
		TenantSlug:     tenantSlug,
		ContextID:      group.ContextID,
		RunID:          group.RunID,
		MatchID:        group.ID,
		RuleID:         group.RuleID,
		TransactionIDs: txIDs,
		Reason:         reason,
		Timestamp:      timestamp.UTC(),
	}, nil
}

// NewMatchConfirmedEvent builds a MatchConfirmedEvent from a confirmed MatchGroup.
// Caller provides timestamp for deterministic testing.
func NewMatchConfirmedEvent(
	ctx context.Context,
	tenantID uuid.UUID,
	tenantSlug string,
	group *MatchGroup,
	timestamp time.Time,
) (*MatchConfirmedEvent, error) {
	asserter := assert.New(
		ctx,
		nil,
		constants.ApplicationName,
		"matching.events.match_confirmed.new",
	)

	if err := asserter.That(ctx, tenantID != uuid.Nil, ErrMatchConfirmedTenantIDRequired.Error()); err != nil {
		return nil, fmt.Errorf("match confirmed tenant id: %w", err)
	}

	if err := asserter.NotNil(ctx, group, ErrMatchConfirmedGroupRequired.Error()); err != nil {
		return nil, fmt.Errorf("match confirmed group: %w", err)
	}

	if group.ContextID == uuid.Nil {
		return nil, ErrMatchConfirmedContextIDRequired
	}

	if group.RunID == uuid.Nil {
		return nil, ErrMatchConfirmedRunIDRequired
	}

	if group.ID == uuid.Nil {
		return nil, ErrMatchConfirmedMatchIDRequired
	}

	// RuleID may be uuid.Nil for manual matches; only require it for rule-based matches.

	if group.ConfirmedAt == nil {
		return nil, ErrMatchConfirmedConfirmedAtRequired
	}

	txIDs := collectTransactionIDsSorted(group.Items)
	if len(txIDs) == 0 {
		return nil, ErrMatchConfirmedTransactionIDsRequired
	}

	confidence := group.Confidence.Value()
	if err := asserter.That(ctx, confidence >= 0 && confidence <= 100, "confidence out of bounds", "confidence", confidence); err != nil {
		return nil, fmt.Errorf("match confirmed confidence bounds: %w", err)
	}

	return &MatchConfirmedEvent{
		EventType:      EventTypeMatchConfirmed,
		TenantID:       tenantID,
		TenantSlug:     tenantSlug,
		ContextID:      group.ContextID,
		RunID:          group.RunID,
		MatchID:        group.ID,
		RuleID:         group.RuleID,
		TransactionIDs: txIDs,
		Confidence:     confidence,
		ConfirmedAt:    group.ConfirmedAt.UTC(),
		Timestamp:      timestamp.UTC(),
	}, nil
}

func collectTransactionIDsSorted(items []*MatchItem) []uuid.UUID {
	out := make([]uuid.UUID, 0, len(items))
	for _, item := range items {
		if item == nil {
			continue
		}

		if item.TransactionID == uuid.Nil {
			continue
		}

		out = append(out, item.TransactionID)
	}

	sort.Slice(out, func(i, j int) bool {
		return out[i].String() < out[j].String()
	})

	return out
}
