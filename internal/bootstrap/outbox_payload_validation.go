// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package bootstrap

import (
	"errors"
	"fmt"

	"github.com/google/uuid"

	sharedDomain "github.com/LerianStudio/matcher/internal/shared/domain"
)

// requireNonZeroUUID returns a wrapped sentinel error when id is uuid.Nil.
// The returned error wraps both ctxName (as a non-%w descriptor) and sentinel (%w)
// so that errors.Is(err, sentinel) works for retry classification.
func requireNonZeroUUID(id uuid.UUID, sentinel error, ctxName string) error {
	if id == uuid.Nil {
		return fmt.Errorf("%s: %w", ctxName, sentinel)
	}

	return nil
}

// requireNonEmptyString returns a wrapped sentinel error when s is empty.
func requireNonEmptyString(s string, sentinel error, ctxName string) error {
	if s == "" {
		return fmt.Errorf("%s: %w", ctxName, sentinel)
	}

	return nil
}

// Payload validators.

func validateIngestionCompletedPayload(payload sharedDomain.IngestionCompletedEvent) error {
	return errors.Join(
		requireNonZeroUUID(payload.JobID, errMissingJobID, "ingestion completed"),
		requireNonZeroUUID(payload.ContextID, errMissingContextID, "ingestion completed"),
		requireNonZeroUUID(payload.SourceID, errMissingSourceID, "ingestion completed"),
	)
}

func validateIngestionFailedPayload(payload sharedDomain.IngestionFailedEvent) error {
	return errors.Join(
		requireNonZeroUUID(payload.JobID, errMissingJobID, "ingestion failed"),
		requireNonZeroUUID(payload.ContextID, errMissingContextID, "ingestion failed"),
		requireNonZeroUUID(payload.SourceID, errMissingSourceID, "ingestion failed"),
		requireNonEmptyString(payload.Error, errMissingError, "ingestion failed"),
	)
}

// requireAllNonZeroUUIDs returns a wrapped sentinel error when ids is empty or
// contains any uuid.Nil entry. Prevents malformed slices from masquerading as
// valid payloads and short-circuiting permanent-error classification.
func requireAllNonZeroUUIDs(ids []uuid.UUID, sentinel error, ctxName string) error {
	if len(ids) == 0 {
		return fmt.Errorf("%s: %w", ctxName, sentinel)
	}

	for _, id := range ids {
		if id == uuid.Nil {
			return fmt.Errorf("%s: %w", ctxName, sentinel)
		}
	}

	return nil
}

func validateMatchConfirmedPayload(payload sharedDomain.MatchConfirmedEvent) error {
	return errors.Join(
		requireNonZeroUUID(payload.TenantID, errMissingTenantID, "match confirmed"),
		requireNonZeroUUID(payload.ContextID, errMissingContextID, "match confirmed"),
		requireNonZeroUUID(payload.RunID, errMissingMatchRunID, "match confirmed"),
		requireNonZeroUUID(payload.MatchID, errMissingMatchID, "match confirmed"),
		requireNonZeroUUID(payload.RuleID, errMissingMatchRuleID, "match confirmed"),
		requireAllNonZeroUUIDs(payload.TransactionIDs, errMissingTransactionIDs, "match confirmed"),
	)
}

func validateMatchUnmatchedPayload(payload sharedDomain.MatchUnmatchedEvent) error {
	return errors.Join(
		requireNonZeroUUID(payload.TenantID, errMissingTenantID, "match unmatched"),
		requireNonZeroUUID(payload.ContextID, errMissingContextID, "match unmatched"),
		requireNonZeroUUID(payload.RunID, errMissingMatchRunID, "match unmatched"),
		requireNonZeroUUID(payload.MatchID, errMissingMatchID, "match unmatched"),
		requireAllNonZeroUUIDs(payload.TransactionIDs, errMissingTransactionIDs, "match unmatched"),
		requireNonEmptyString(payload.Reason, errMissingReason, "match unmatched"),
	)
}

func validateAuditLogCreatedPayload(payload sharedDomain.AuditLogCreatedEvent) error {
	return errors.Join(
		requireNonZeroUUID(payload.TenantID, errMissingTenantID, "audit log created"),
		requireNonEmptyString(payload.EntityType, errMissingEntityType, "audit log created"),
		requireNonZeroUUID(payload.EntityID, errMissingEntityID, "audit log created"),
		requireNonEmptyString(payload.Action, errMissingAction, "audit log created"),
	)
}
