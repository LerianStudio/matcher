// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package ports

import (
	"context"

	"github.com/google/uuid"

	shared "github.com/LerianStudio/matcher/internal/shared/domain"
)

//go:generate mockgen -destination=mocks/match_rule_provider_mock.go -package=mocks . MatchRuleProvider

// MatchRuleProvider loads configured match rules for a context.
// Contract:
// - Tenant scoping and authorization happen in adapters; caller provides contextID.
// - Returns a non-nil slice (empty when no rules).
// - Uses shared.MatchRule from the shared kernel.
type MatchRuleProvider interface {
	ListByContextID(ctx context.Context, contextID uuid.UUID) (shared.MatchRules, error)
}
