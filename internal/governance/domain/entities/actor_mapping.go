// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

// Package entities defines governance domain types and validation logic.
package entities

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/LerianStudio/lib-commons/v5/commons/assert"

	"github.com/LerianStudio/matcher/internal/shared/constants"
)

// MaxActorMappingActorIDLength matches the database column VARCHAR(255) constraint.
const MaxActorMappingActorIDLength = 255

// redactedValue is the placeholder written by the repository when it
// pseudonymizes PII fields for GDPR compliance. The value is duplicated in
// the SQL UPDATE that performs the pseudonymization (see
// actor_mapping.postgresql.go) and in the database column comments in
// migrations/000001_init_schema.up.sql — that SQL path is the single source
// of truth for what a redacted value looks like. This const exists so
// IsRedacted() can recognise rows that have been redacted without an
// additional round-trip.
const redactedValue = "[REDACTED]"

// Sentinel errors for actor mapping validation.
var (
	ErrActorIDRequired           = errors.New("actor id is required")
	ErrActorIDExceedsMaxLen      = errors.New("actor id exceeds maximum length")
	ErrNilActorMappingRepository = errors.New("actor mapping repository is required")
)

// SafeActorIDPrefix returns a truncated prefix of the actor ID for safe logging.
// Actor IDs may contain PII (emails, employee IDs) and must not be logged in full
// per GDPR Article 5(1)(c) data minimization requirements.
func SafeActorIDPrefix(actorID string) string {
	const maxPrefix = 4

	if actorID == "" {
		return "***"
	}

	if len(actorID) <= maxPrefix {
		return actorID[:1] + "***"
	}

	return actorID[:maxPrefix] + "***"
}

// ActorMapping maps an opaque actor ID to PII (display name, email).
// This table is mutable by design for GDPR compliance: it supports
// pseudonymization (UPDATE) and right-to-erasure (DELETE).
type ActorMapping struct {
	ActorID     string
	DisplayName *string
	Email       *string
	CreatedAt   time.Time
	UpdatedAt   time.Time
}

// NewActorMapping validates inputs and returns a new actor mapping.
func NewActorMapping(
	ctx context.Context,
	actorID string,
	displayName, email *string,
) (*ActorMapping, error) {
	asserter := assert.New(ctx, nil, constants.ApplicationName, "governance.actor_mapping.new")

	trimmedActorID := strings.TrimSpace(actorID)
	if err := asserter.NotEmpty(ctx, trimmedActorID, "actor id is required"); err != nil {
		return nil, ErrActorIDRequired
	}

	if err := asserter.That(ctx, len(trimmedActorID) <= MaxActorMappingActorIDLength, "actor id exceeds maximum length"); err != nil {
		return nil, ErrActorIDExceedsMaxLen
	}

	now := time.Now().UTC()

	return &ActorMapping{
		ActorID:     trimmedActorID,
		DisplayName: displayName,
		Email:       email,
		CreatedAt:   now,
		UpdatedAt:   now,
	}, nil
}

// IsRedacted returns true if both DisplayName and Email are set to [REDACTED].
func (am *ActorMapping) IsRedacted() bool {
	if am == nil {
		return false
	}

	if am.DisplayName == nil || am.Email == nil {
		return false
	}

	return *am.DisplayName == redactedValue && *am.Email == redactedValue
}
