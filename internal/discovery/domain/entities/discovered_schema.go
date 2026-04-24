// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package entities

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/LerianStudio/lib-commons/v5/commons/assert"

	"github.com/LerianStudio/matcher/internal/shared/constants"
)

// ColumnInfo represents a single column in a discovered database schema.
// Note: Type and Nullable are historical fields -- the current Fetcher API
// only returns flat field names. These fields remain for backward
// compatibility with persisted data and are omitted from responses when empty.
type ColumnInfo struct {
	Name     string `json:"name"`
	Type     string `json:"type,omitempty"`
	Nullable bool   `json:"nullable,omitempty"`
}

// DiscoveredSchema represents the schema of a table discovered from a Fetcher connection.
type DiscoveredSchema struct {
	ID           uuid.UUID
	ConnectionID uuid.UUID
	TableName    string
	Columns      []ColumnInfo
	DiscoveredAt time.Time
}

// NewDiscoveredSchema creates a new DiscoveredSchema with validated invariants.
func NewDiscoveredSchema(
	ctx context.Context,
	connectionID uuid.UUID,
	tableName string,
	columns []ColumnInfo,
) (*DiscoveredSchema, error) {
	asserter := assert.New(ctx, nil, constants.ApplicationName, "discovery.discovered_schema.new")

	if err := asserter.That(ctx, connectionID != uuid.Nil, "connection id required"); err != nil {
		return nil, fmt.Errorf("discovered schema connection id: %w", err)
	}

	if err := asserter.NotEmpty(ctx, tableName, "table name required"); err != nil {
		return nil, fmt.Errorf("discovered schema table name: %w", err)
	}

	if columns == nil {
		columns = []ColumnInfo{}
	}

	return &DiscoveredSchema{
		ID:           uuid.New(),
		ConnectionID: connectionID,
		TableName:    tableName,
		Columns:      columns,
		DiscoveredAt: time.Now().UTC(),
	}, nil
}

// ColumnsJSON returns the columns serialized as JSON for database storage.
func (ds *DiscoveredSchema) ColumnsJSON() ([]byte, error) {
	if ds == nil || ds.Columns == nil {
		return []byte("[]"), nil
	}

	data, err := json.Marshal(ds.Columns)
	if err != nil {
		return nil, fmt.Errorf("marshal columns: %w", err)
	}

	return data, nil
}
