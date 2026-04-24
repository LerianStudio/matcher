// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package shared_test

import (
	"context"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	shared "github.com/LerianStudio/matcher/internal/shared/domain"
	sharedfee "github.com/LerianStudio/matcher/internal/shared/domain/fee"
)

// reconciliationSourceMaxNameLength mirrors the unexported constant guarding
// source name length in the shared domain. Duplicated here as a test contract
// rather than exported from the domain package.
const reconciliationSourceMaxNameLength = 50

func TestNewReconciliationSource(t *testing.T) {
	t.Parallel()

	contextID := uuid.New()

	t.Run("creates valid source", func(t *testing.T) {
		t.Parallel()

		input := shared.CreateReconciliationSourceInput{
			Name: "Primary Ledger",
			Type: shared.SourceTypeLedger,
			Side: sharedfee.MatchingSideLeft,
			Config: map[string]any{
				"endpoint": "https://ledger.example.com",
			},
		}

		ctx := context.Background()
		source, err := shared.NewReconciliationSource(ctx, contextID, input)
		require.NoError(t, err)
		assert.NotEqual(t, uuid.Nil, source.ID)
		assert.Equal(t, contextID, source.ContextID)
		assert.Equal(t, "Primary Ledger", source.Name)
		assert.Equal(t, shared.SourceTypeLedger, source.Type)
		assert.Equal(t, sharedfee.MatchingSideLeft, source.Side)
		assert.NotEmpty(t, source.Config)
	})

	t.Run("fails with nil context", func(t *testing.T) {
		t.Parallel()

		input := shared.CreateReconciliationSourceInput{
			Name: "Bank",
			Type: shared.SourceTypeBank,
			Side: sharedfee.MatchingSideLeft,
		}

		_, err := shared.NewReconciliationSource(context.Background(), uuid.Nil, input)
		require.Error(t, err)
		assert.Equal(t, shared.ErrSourceContextRequired, err)
	})

	t.Run("fails with empty name", func(t *testing.T) {
		t.Parallel()

		input := shared.CreateReconciliationSourceInput{
			Name: "",
			Type: shared.SourceTypeBank,
			Side: sharedfee.MatchingSideLeft,
		}

		_, err := shared.NewReconciliationSource(context.Background(), contextID, input)
		require.Error(t, err)
		assert.Equal(t, shared.ErrSourceNameRequired, err)
	})

	t.Run("fails with whitespace-only name", func(t *testing.T) {
		t.Parallel()

		input := shared.CreateReconciliationSourceInput{
			Name: "   \t\n  ",
			Type: shared.SourceTypeBank,
			Side: sharedfee.MatchingSideLeft,
		}

		_, err := shared.NewReconciliationSource(context.Background(), contextID, input)
		require.Error(t, err)
		assert.Equal(t, shared.ErrSourceNameRequired, err)
	})

	t.Run("trims whitespace from name", func(t *testing.T) {
		t.Parallel()

		input := shared.CreateReconciliationSourceInput{
			Name: "  Bank Source  ",
			Type: shared.SourceTypeBank,
			Side: sharedfee.MatchingSideLeft,
		}

		source, err := shared.NewReconciliationSource(context.Background(), contextID, input)
		require.NoError(t, err)
		assert.Equal(t, "Bank Source", source.Name)
	})

	t.Run("succeeds with name at max length", func(t *testing.T) {
		t.Parallel()

		maxLengthName := strings.Repeat("a", reconciliationSourceMaxNameLength)
		input := shared.CreateReconciliationSourceInput{
			Name: maxLengthName,
			Type: shared.SourceTypeBank,
			Side: sharedfee.MatchingSideLeft,
		}

		source, err := shared.NewReconciliationSource(context.Background(), contextID, input)
		require.NoError(t, err)
		assert.Equal(t, maxLengthName, source.Name)
	})

	t.Run("fails with name exceeding max length", func(t *testing.T) {
		t.Parallel()

		tooLongName := strings.Repeat("a", reconciliationSourceMaxNameLength+1)
		input := shared.CreateReconciliationSourceInput{
			Name: tooLongName,
			Type: shared.SourceTypeBank,
			Side: sharedfee.MatchingSideLeft,
		}

		_, err := shared.NewReconciliationSource(context.Background(), contextID, input)
		require.Error(t, err)
		assert.Equal(t, shared.ErrSourceNameTooLong, err)
	})

	t.Run("fails with invalid type", func(t *testing.T) {
		t.Parallel()

		input := shared.CreateReconciliationSourceInput{
			Name: "Gateway",
			Type: shared.SourceType("INVALID"),
			Side: sharedfee.MatchingSideLeft,
		}

		_, err := shared.NewReconciliationSource(context.Background(), contextID, input)
		require.Error(t, err)
		assert.Equal(t, shared.ErrSourceTypeInvalid, err)
		t.Run("fails with missing side", func(t *testing.T) {
			t.Parallel()

			input := shared.CreateReconciliationSourceInput{
				Name: "Gateway",
				Type: shared.SourceTypeGateway,
			}

			_, err := shared.NewReconciliationSource(context.Background(), contextID, input)
			require.Error(t, err)
			assert.Equal(t, shared.ErrSourceSideRequired, err)
		})

		t.Run("fails with invalid side", func(t *testing.T) {
			t.Parallel()

			input := shared.CreateReconciliationSourceInput{
				Name: "Gateway",
				Type: shared.SourceTypeGateway,
				Side: sharedfee.MatchingSideAny,
			}

			_, err := shared.NewReconciliationSource(context.Background(), contextID, input)
			require.Error(t, err)
			assert.Equal(t, shared.ErrSourceSideInvalid, err)
		})
	})
}

func TestReconciliationSource_Update(t *testing.T) {
	t.Parallel()
	createSource := func(t *testing.T) *shared.ReconciliationSource {
		t.Helper()

		contextID := uuid.New()
		input := shared.CreateReconciliationSourceInput{
			Name: "Gateway",
			Type: shared.SourceTypeGateway,
			Side: sharedfee.MatchingSideLeft,
			Config: map[string]any{
				"token": "secret",
			},
		}
		ctx := context.Background()
		source, err := shared.NewReconciliationSource(ctx, contextID, input)
		require.NoError(t, err)

		return source
	}

	t.Run("updates name", func(t *testing.T) {
		t.Parallel()

		source := createSource(t)
		newName := "Updated Gateway"
		err := source.Update(context.Background(), shared.UpdateReconciliationSourceInput{Name: &newName})
		require.NoError(t, err)
		assert.Equal(t, "Updated Gateway", source.Name)
	})

	t.Run("updates type", func(t *testing.T) {
		t.Parallel()

		source := createSource(t)
		newType := shared.SourceTypeBank
		err := source.Update(context.Background(), shared.UpdateReconciliationSourceInput{Type: &newType})
		require.NoError(t, err)
		assert.Equal(t, shared.SourceTypeBank, source.Type)
	})

	t.Run("updates side", func(t *testing.T) {
		t.Parallel()

		source := createSource(t)
		newSide := sharedfee.MatchingSideRight
		err := source.Update(context.Background(), shared.UpdateReconciliationSourceInput{Side: &newSide})
		require.NoError(t, err)
		assert.Equal(t, sharedfee.MatchingSideRight, source.Side)
	})

	t.Run("updates config", func(t *testing.T) {
		t.Parallel()

		source := createSource(t)
		newConfig := map[string]any{"region": "us-east-1"}
		err := source.Update(
			context.Background(),
			shared.UpdateReconciliationSourceInput{Config: newConfig},
		)
		require.NoError(t, err)
		assert.Equal(t, newConfig, source.Config)
	})

	t.Run("fails with empty name", func(t *testing.T) {
		t.Parallel()

		source := createSource(t)
		originalName := source.Name
		originalType := source.Type
		originalConfig := source.Config
		originalUpdatedAt := source.UpdatedAt

		emptyName := ""
		err := source.Update(
			context.Background(),
			shared.UpdateReconciliationSourceInput{Name: &emptyName},
		)
		require.Error(t, err)
		assert.Equal(t, shared.ErrSourceNameRequired, err)
		assert.Equal(t, originalName, source.Name)
		assert.Equal(t, originalType, source.Type)
		assert.Equal(t, originalConfig, source.Config)
		assert.Equal(t, originalUpdatedAt, source.UpdatedAt)
	})

	t.Run("fails with whitespace-only name", func(t *testing.T) {
		t.Parallel()

		source := createSource(t)
		originalName := source.Name

		wsName := "   \t  "
		err := source.Update(
			context.Background(),
			shared.UpdateReconciliationSourceInput{Name: &wsName},
		)
		require.Error(t, err)
		assert.Equal(t, shared.ErrSourceNameRequired, err)
		assert.Equal(t, originalName, source.Name)
	})

	t.Run("fails with invalid type", func(t *testing.T) {
		t.Parallel()

		source := createSource(t)
		originalName := source.Name
		originalType := source.Type
		originalConfig := source.Config
		originalUpdatedAt := source.UpdatedAt

		invalidType := shared.SourceType("INVALID")
		err := source.Update(
			context.Background(),
			shared.UpdateReconciliationSourceInput{Type: &invalidType},
		)
		require.Error(t, err)
		assert.Equal(t, shared.ErrSourceTypeInvalid, err)
		assert.Equal(t, originalName, source.Name)
		assert.Equal(t, originalType, source.Type)
		assert.Equal(t, originalConfig, source.Config)
		assert.Equal(t, originalUpdatedAt, source.UpdatedAt)
	})

	t.Run("fails with invalid side", func(t *testing.T) {
		t.Parallel()

		source := createSource(t)
		invalidSide := sharedfee.MatchingSideAny
		err := source.Update(context.Background(), shared.UpdateReconciliationSourceInput{Side: &invalidSide})
		require.Error(t, err)
		assert.Equal(t, shared.ErrSourceSideInvalid, err)
	})

	t.Run("fails with nil receiver", func(t *testing.T) {
		t.Parallel()

		newName := "Updated"
		err := (*shared.ReconciliationSource)(
			nil,
		).Update(context.Background(), shared.UpdateReconciliationSourceInput{Name: &newName})
		require.Error(t, err)
		assert.Equal(t, shared.ErrNilReconciliationSource, err)
	})
}
