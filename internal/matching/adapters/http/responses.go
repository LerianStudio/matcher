// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

// Package http provides HTTP handlers for the matching domain.
package http

import (
	"github.com/LerianStudio/matcher/internal/matching/adapters/http/dto"
	sharedhttp "github.com/LerianStudio/matcher/internal/shared/adapters/http"
)

// ListMatchGroupsResponse represents a cursor-paginated list of match groups.
// @Description Cursor-paginated list of match groups
type ListMatchGroupsResponse struct {
	// List of match groups
	Items []dto.MatchGroupResponse `json:"items" validate:"omitempty,max=200" maxItems:"200"`
	// Cursor pagination fields
	sharedhttp.CursorResponse
}

// ListMatchRunsResponse represents a cursor-paginated list of match runs.
// @Description Cursor-paginated list of match runs
type ListMatchRunsResponse struct {
	// List of match runs
	Items []dto.MatchRunResponse `json:"items" validate:"omitempty,max=200" maxItems:"200"`
	// Cursor pagination fields
	sharedhttp.CursorResponse
}
