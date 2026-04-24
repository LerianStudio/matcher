// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

// Package schema provides PostgreSQL repository implementation for DiscoveredSchema entities.
package schema

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/LerianStudio/matcher/internal/discovery/domain/entities"
)

// SchemaModel is the PostgreSQL representation of a DiscoveredSchema.
type SchemaModel struct {
	ID           uuid.UUID `db:"id"`
	ConnectionID uuid.UUID `db:"connection_id"`
	TableName    string    `db:"table_name"`
	Columns      []byte    `db:"columns"` // JSONB
	DiscoveredAt time.Time `db:"discovered_at"`
}

// ToDomain converts the PostgreSQL model to a domain entity.
func (model *SchemaModel) ToDomain() (*entities.DiscoveredSchema, error) {
	if model == nil {
		return nil, ErrModelRequired
	}

	var columns []entities.ColumnInfo
	if len(model.Columns) > 0 {
		if err := json.Unmarshal(model.Columns, &columns); err != nil {
			return nil, fmt.Errorf("unmarshal columns: %w", err)
		}
	}

	return &entities.DiscoveredSchema{
		ID:           model.ID,
		ConnectionID: model.ConnectionID,
		TableName:    model.TableName,
		Columns:      columns,
		DiscoveredAt: model.DiscoveredAt,
	}, nil
}

// FromDomain converts a domain entity to a PostgreSQL model.
func FromDomain(entity *entities.DiscoveredSchema) (*SchemaModel, error) {
	if entity == nil {
		return nil, ErrEntityRequired
	}

	columnsJSON, err := entity.ColumnsJSON()
	if err != nil {
		return nil, fmt.Errorf("marshal columns: %w", err)
	}

	return &SchemaModel{
		ID:           entity.ID,
		ConnectionID: entity.ConnectionID,
		TableName:    entity.TableName,
		Columns:      columnsJSON,
		DiscoveredAt: entity.DiscoveredAt,
	}, nil
}
