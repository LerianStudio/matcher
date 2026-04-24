// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package job

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/ingestion/domain/entities"
)

func TestNewJobPostgreSQLModel_RoundTrip(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	entity, err := entities.NewIngestionJob(ctx, uuid.New(), uuid.New(), "file.csv", 100)
	require.NoError(t, err)
	require.NoError(t, entity.Start(ctx))

	model, err := NewJobPostgreSQLModel(entity)
	require.NoError(t, err)
	require.NotNil(t, model)

	restored, err := jobModelToEntity(model)
	require.NoError(t, err)
	require.Equal(t, entity.ID, restored.ID)
}
