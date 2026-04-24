// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

// Package shared contains domain types shared across multiple bounded contexts.
package shared

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/LerianStudio/lib-commons/v5/commons/assert"

	"github.com/LerianStudio/matcher/internal/shared/constants"
)

// Field map sentinel errors.
var (
	// ErrFieldMapNil is returned when the field map is nil.
	ErrFieldMapNil = errors.New("field map is nil")
	// ErrFieldMapContextRequired is returned when the context_id is not provided.
	ErrFieldMapContextRequired = errors.New("context_id is required")
	// ErrFieldMapSourceRequired is returned when the source_id is not provided.
	ErrFieldMapSourceRequired = errors.New("source_id is required")
	// ErrFieldMapMappingRequired is returned when the mapping is not provided.
	ErrFieldMapMappingRequired = errors.New("mapping is required")
	// ErrFieldMapMappingValueEmpty is returned when a mapping value is an empty string.
	ErrFieldMapMappingValueEmpty = errors.New("mapping values must be non-empty strings")
)

// FieldMap represents the mapping rules for a reconciliation source.
// This is a shared kernel type used by both Configuration and Ingestion contexts.
type FieldMap struct {
	ID        uuid.UUID
	ContextID uuid.UUID
	SourceID  uuid.UUID
	Mapping   map[string]any
	Version   int
	CreatedAt time.Time
	UpdatedAt time.Time
}

// CreateFieldMapInput defines the input required to create a field map.
type CreateFieldMapInput struct {
	Mapping map[string]any `json:"mapping" validate:"required" swaggertype:"object"`
}

// UpdateFieldMapInput defines the fields that can be updated on a field map.
type UpdateFieldMapInput struct {
	Mapping map[string]any `json:"mapping" swaggertype:"object"`
}

// NewFieldMap validates input and returns a new field map entity.
func NewFieldMap(
	ctx context.Context,
	contextID, sourceID uuid.UUID,
	input CreateFieldMapInput,
) (*FieldMap, error) {
	asserter := assert.New(ctx, nil, constants.ApplicationName, "configuration.field_map.new")

	if contextID == uuid.Nil {
		return nil, ErrFieldMapContextRequired
	}

	if sourceID == uuid.Nil {
		return nil, ErrFieldMapSourceRequired
	}

	if err := asserter.That(ctx, len(input.Mapping) > 0, "mapping is required"); err != nil {
		return nil, fmt.Errorf("field map mapping: %w", err)
	}

	if err := validateFieldMapMappingValues(input.Mapping); err != nil {
		return nil, err
	}

	now := time.Now().UTC()

	return &FieldMap{
		ID:        uuid.New(),
		ContextID: contextID,
		SourceID:  sourceID,
		Mapping:   input.Mapping,
		Version:   1,
		CreatedAt: now,
		UpdatedAt: now,
	}, nil
}

// Update applies field map updates and increments the version.
func (fm *FieldMap) Update(ctx context.Context, input UpdateFieldMapInput) error {
	asserter := assert.New(ctx, nil, constants.ApplicationName, "configuration.field_map.update")

	if err := asserter.NotNil(ctx, fm, "field map is required"); err != nil {
		return ErrFieldMapNil
	}

	if err := asserter.That(ctx, len(input.Mapping) > 0, "mapping is required"); err != nil {
		return ErrFieldMapMappingRequired
	}

	if err := validateFieldMapMappingValues(input.Mapping); err != nil {
		return err
	}

	fm.Mapping = input.Mapping
	fm.Version++
	fm.UpdatedAt = time.Now().UTC()

	return nil
}

// validateFieldMapMappingValues checks that all string-typed mapping values are
// non-empty. Non-string values (nested objects, numbers, booleans) are allowed
// through without additional checks, as the ingestion context is responsible
// for detailed validation.
func validateFieldMapMappingValues(mapping map[string]any) error {
	for key, value := range mapping {
		strVal, ok := value.(string)
		if ok && strVal == "" {
			return fmt.Errorf("%w: key %q has empty string value", ErrFieldMapMappingValueEmpty, key)
		}
	}

	return nil
}

// MappingJSON marshals the field map mapping to JSON.
func (fm *FieldMap) MappingJSON() ([]byte, error) {
	if fm == nil {
		return json.Marshal(nil)
	}

	return json.Marshal(fm.Mapping)
}
