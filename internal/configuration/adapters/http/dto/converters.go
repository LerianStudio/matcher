// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package dto

import (
	"time"

	"github.com/google/uuid"

	"github.com/LerianStudio/matcher/internal/configuration/domain/entities"
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
)

// ReconciliationContextToResponse converts a domain entity to a response DTO.
// Returns an empty struct for nil input to ensure consistent JSON structure.
func ReconciliationContextToResponse(
	ctx *entities.ReconciliationContext,
) ReconciliationContextResponse {
	if ctx == nil {
		return ReconciliationContextResponse{}
	}

	var feeNormalization string
	if ctx.FeeNormalization != nil {
		feeNormalization = *ctx.FeeNormalization
	}

	return ReconciliationContextResponse{
		ID:                ctx.ID.String(),
		TenantID:          ctx.TenantID.String(),
		Name:              ctx.Name,
		Type:              ctx.Type.String(),
		Interval:          ctx.Interval,
		Status:            ctx.Status.String(),
		FeeToleranceAbs:   ctx.FeeToleranceAbs.String(),
		FeeTolerancePct:   ctx.FeeTolerancePct.String(),
		FeeNormalization:  feeNormalization,
		AutoMatchOnUpload: ctx.AutoMatchOnUpload,
		CreatedAt:         ctx.CreatedAt.Format(time.RFC3339),
		UpdatedAt:         ctx.UpdatedAt.Format(time.RFC3339),
	}
}

// ReconciliationContextsToResponse converts a slice of entities to response DTOs.
// Always returns an initialized slice (never nil) for consistent JSON serialization.
func ReconciliationContextsToResponse(
	contexts []*entities.ReconciliationContext,
) []ReconciliationContextResponse {
	result := make([]ReconciliationContextResponse, 0, len(contexts))

	for _, ctx := range contexts {
		if ctx != nil {
			result = append(result, ReconciliationContextToResponse(ctx))
		}
	}

	return result
}

// ReconciliationSourceToResponse converts a domain entity to a response DTO.
// Returns an empty struct for nil input to ensure consistent JSON structure.
func ReconciliationSourceToResponse(
	src *entities.ReconciliationSource,
) ReconciliationSourceResponse {
	if src == nil {
		return ReconciliationSourceResponse{Config: map[string]any{}}
	}

	config := src.Config

	if config == nil {
		config = map[string]any{}
	}

	return ReconciliationSourceResponse{
		ID:        src.ID.String(),
		ContextID: src.ContextID.String(),
		Name:      src.Name,
		Type:      src.Type.String(),
		Side:      string(src.Side),
		Config:    config,
		CreatedAt: src.CreatedAt.Format(time.RFC3339),
		UpdatedAt: src.UpdatedAt.Format(time.RFC3339),
	}
}

// SourceWithFieldMapStatusToResponse converts a source entity with field map status.
// Returns an empty struct for nil input to ensure consistent JSON structure.
func SourceWithFieldMapStatusToResponse(
	src *entities.ReconciliationSource,
	hasFieldMaps bool,
) SourceWithFieldMapStatusResponse {
	if src == nil {
		return SourceWithFieldMapStatusResponse{
			ReconciliationSourceResponse: ReconciliationSourceResponse{Config: map[string]any{}},
		}
	}

	return SourceWithFieldMapStatusResponse{
		ReconciliationSourceResponse: ReconciliationSourceToResponse(src),
		HasFieldMaps:                 hasFieldMaps,
	}
}

// SourcesToResponseWithFieldMaps converts a slice of sources with field map existence info.
// Always returns an initialized slice (never nil) for consistent JSON serialization.
func SourcesToResponseWithFieldMaps(
	sources []*entities.ReconciliationSource,
	fieldMapsExist map[uuid.UUID]bool,
) []SourceWithFieldMapStatusResponse {
	result := make([]SourceWithFieldMapStatusResponse, 0, len(sources))

	for _, src := range sources {
		if src != nil {
			hasFieldMaps := false

			if fieldMapsExist != nil {
				hasFieldMaps = fieldMapsExist[src.ID]
			}

			result = append(result, SourceWithFieldMapStatusToResponse(src, hasFieldMaps))
		}
	}

	return result
}

// FieldMapToResponse converts a domain entity to a response DTO.
// Returns an empty struct for nil input to ensure consistent JSON structure.
func FieldMapToResponse(fm *shared.FieldMap) FieldMapResponse {
	if fm == nil {
		return FieldMapResponse{Mapping: map[string]any{}}
	}

	mapping := fm.Mapping

	if mapping == nil {
		mapping = map[string]any{}
	}

	return FieldMapResponse{
		ID:        fm.ID.String(),
		ContextID: fm.ContextID.String(),
		SourceID:  fm.SourceID.String(),
		Mapping:   mapping,
		Version:   fm.Version,
		CreatedAt: fm.CreatedAt.Format(time.RFC3339),
		UpdatedAt: fm.UpdatedAt.Format(time.RFC3339),
	}
}

// FieldMapsToResponse converts a slice of entities to response DTOs.
// Always returns an initialized slice (never nil) for consistent JSON serialization.
func FieldMapsToResponse(fieldMaps []*shared.FieldMap) []FieldMapResponse {
	result := make([]FieldMapResponse, 0, len(fieldMaps))

	for _, fm := range fieldMaps {
		if fm != nil {
			result = append(result, FieldMapToResponse(fm))
		}
	}

	return result
}

// MatchRuleToResponse converts a domain entity to a response DTO.
// Returns an empty struct for nil input to ensure consistent JSON structure.
func MatchRuleToResponse(rule *shared.MatchRule) MatchRuleResponse {
	if rule == nil {
		return MatchRuleResponse{Config: map[string]any{}}
	}

	config := rule.Config

	if config == nil {
		config = map[string]any{}
	}

	return MatchRuleResponse{
		ID:        rule.ID.String(),
		ContextID: rule.ContextID.String(),
		Priority:  rule.Priority,
		Type:      rule.Type.String(),
		Config:    config,
		CreatedAt: rule.CreatedAt.Format(time.RFC3339),
		UpdatedAt: rule.UpdatedAt.Format(time.RFC3339),
	}
}

// MatchRulesToResponse converts a slice of entities to response DTOs.
// Always returns an initialized slice (never nil) for consistent JSON serialization.
func MatchRulesToResponse(rules []*shared.MatchRule) []MatchRuleResponse {
	result := make([]MatchRuleResponse, 0, len(rules))

	for _, rule := range rules {
		if rule != nil {
			result = append(result, MatchRuleToResponse(rule))
		}
	}

	return result
}
