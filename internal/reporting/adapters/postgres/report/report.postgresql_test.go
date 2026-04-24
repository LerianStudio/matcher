// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package report

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/reporting/domain/entities"
)

func TestRepository_NilProvider(t *testing.T) {
	t.Parallel()

	repo := NewRepository(nil)

	filter := entities.ReportFilter{
		ContextID: uuid.New(),
	}

	err := repo.validateFilter(&filter)
	require.ErrorIs(t, err, ErrRepositoryNotInitialized)
}
