// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package repositories

import (
	"context"

	"github.com/google/uuid"

	"github.com/LerianStudio/matcher/internal/discovery/domain/entities"
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

// SchemaRepository defines persistence operations for DiscoveredSchema entities.
type SchemaRepository interface {
	// UpsertBatch replaces the persisted schema snapshot for every connection
	// present in the batch.
	UpsertBatch(ctx context.Context, schemas []*entities.DiscoveredSchema) error
	// UpsertBatchWithTx replaces the persisted schema snapshot for every
	// connection present in the batch within an existing transaction.
	UpsertBatchWithTx(ctx context.Context, tx sharedPorts.Tx, schemas []*entities.DiscoveredSchema) error
	// FindByConnectionID retrieves all schemas discovered for a given connection.
	FindByConnectionID(ctx context.Context, connectionID uuid.UUID) ([]*entities.DiscoveredSchema, error)
	// DeleteByConnectionID removes all schemas associated with a connection.
	DeleteByConnectionID(ctx context.Context, connectionID uuid.UUID) error
	// DeleteByConnectionIDWithTx removes schemas for a connection within an existing transaction.
	DeleteByConnectionIDWithTx(ctx context.Context, tx sharedPorts.Tx, connectionID uuid.UUID) error
}
