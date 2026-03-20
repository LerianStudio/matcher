//go:build unit

package entities

import (
	"context"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/configuration/domain/value_objects"
	sharedfee "github.com/LerianStudio/matcher/internal/shared/domain/fee"
)

func TestNewReconciliationSource(t *testing.T) {
	t.Parallel()

	contextID := uuid.New()

	t.Run("creates valid source", func(t *testing.T) {
		t.Parallel()

		input := CreateReconciliationSourceInput{
			Name: "Primary Ledger",
			Type: value_objects.SourceTypeLedger,
			Side: sharedfee.MatchingSideLeft,
			Config: map[string]any{
				"endpoint": "https://ledger.example.com",
			},
		}

		ctx := context.Background()
		source, err := NewReconciliationSource(ctx, contextID, input)
		require.NoError(t, err)
		assert.NotEqual(t, uuid.Nil, source.ID)
		assert.Equal(t, contextID, source.ContextID)
		assert.Equal(t, "Primary Ledger", source.Name)
		assert.Equal(t, value_objects.SourceTypeLedger, source.Type)
		assert.Equal(t, sharedfee.MatchingSideLeft, source.Side)
		assert.NotEmpty(t, source.Config)
	})

	t.Run("fails with nil context", func(t *testing.T) {
		t.Parallel()

		input := CreateReconciliationSourceInput{
			Name: "Bank",
			Type: value_objects.SourceTypeBank,
			Side: sharedfee.MatchingSideLeft,
		}

		_, err := NewReconciliationSource(context.Background(), uuid.Nil, input)
		require.Error(t, err)
		assert.Equal(t, ErrSourceContextRequired, err)
	})

	t.Run("fails with empty name", func(t *testing.T) {
		t.Parallel()

		input := CreateReconciliationSourceInput{
			Name: "",
			Type: value_objects.SourceTypeBank,
			Side: sharedfee.MatchingSideLeft,
		}

		_, err := NewReconciliationSource(context.Background(), contextID, input)
		require.Error(t, err)
		assert.Equal(t, ErrSourceNameRequired, err)
	})

	t.Run("succeeds with name at max length", func(t *testing.T) {
		t.Parallel()

		maxLengthName := strings.Repeat("a", maxSourceNameLength)
		input := CreateReconciliationSourceInput{
			Name: maxLengthName,
			Type: value_objects.SourceTypeBank,
			Side: sharedfee.MatchingSideLeft,
		}

		source, err := NewReconciliationSource(context.Background(), contextID, input)
		require.NoError(t, err)
		assert.Equal(t, maxLengthName, source.Name)
	})

	t.Run("fails with name exceeding max length", func(t *testing.T) {
		t.Parallel()

		tooLongName := strings.Repeat("a", maxSourceNameLength+1)
		input := CreateReconciliationSourceInput{
			Name: tooLongName,
			Type: value_objects.SourceTypeBank,
			Side: sharedfee.MatchingSideLeft,
		}

		_, err := NewReconciliationSource(context.Background(), contextID, input)
		require.Error(t, err)
		assert.Equal(t, ErrSourceNameTooLong, err)
	})

	t.Run("fails with invalid type", func(t *testing.T) {
		t.Parallel()

		input := CreateReconciliationSourceInput{
			Name: "Gateway",
			Type: value_objects.SourceType("INVALID"),
			Side: sharedfee.MatchingSideLeft,
		}

		_, err := NewReconciliationSource(context.Background(), contextID, input)
		require.Error(t, err)
		assert.Equal(t, ErrSourceTypeInvalid, err)
		t.Run("fails with missing side", func(t *testing.T) {
			t.Parallel()

			input := CreateReconciliationSourceInput{
				Name: "Gateway",
				Type: value_objects.SourceTypeGateway,
			}

			_, err := NewReconciliationSource(context.Background(), contextID, input)
			require.Error(t, err)
			assert.Equal(t, ErrSourceSideRequired, err)
		})

		t.Run("fails with invalid side", func(t *testing.T) {
			t.Parallel()

			input := CreateReconciliationSourceInput{
				Name: "Gateway",
				Type: value_objects.SourceTypeGateway,
				Side: sharedfee.MatchingSideAny,
			}

			_, err := NewReconciliationSource(context.Background(), contextID, input)
			require.Error(t, err)
			assert.Equal(t, ErrSourceSideInvalid, err)
		})

	})
}

func TestReconciliationSource_Update(t *testing.T) {
	t.Parallel()
	createSource := func(t *testing.T) *ReconciliationSource {
		t.Helper()

		contextID := uuid.New()
		input := CreateReconciliationSourceInput{
			Name: "Gateway",
			Type: value_objects.SourceTypeGateway,
			Side: sharedfee.MatchingSideLeft,
			Config: map[string]any{
				"token": "secret",
			},
		}
		ctx := context.Background()
		source, err := NewReconciliationSource(ctx, contextID, input)
		require.NoError(t, err)

		return source
	}

	t.Run("updates name", func(t *testing.T) {
		t.Parallel()

		source := createSource(t)
		newName := "Updated Gateway"
		err := source.Update(context.Background(), UpdateReconciliationSourceInput{Name: &newName})
		require.NoError(t, err)
		assert.Equal(t, "Updated Gateway", source.Name)
	})

	t.Run("updates type", func(t *testing.T) {
		t.Parallel()

		source := createSource(t)
		newType := value_objects.SourceTypeBank
		err := source.Update(context.Background(), UpdateReconciliationSourceInput{Type: &newType})
		require.NoError(t, err)
		assert.Equal(t, value_objects.SourceTypeBank, source.Type)
	})

	t.Run("updates side", func(t *testing.T) {
		t.Parallel()

		source := createSource(t)
		newSide := sharedfee.MatchingSideRight
		err := source.Update(context.Background(), UpdateReconciliationSourceInput{Side: &newSide})
		require.NoError(t, err)
		assert.Equal(t, sharedfee.MatchingSideRight, source.Side)
	})

	t.Run("updates config", func(t *testing.T) {
		t.Parallel()

		source := createSource(t)
		newConfig := map[string]any{"region": "us-east-1"}
		err := source.Update(
			context.Background(),
			UpdateReconciliationSourceInput{Config: newConfig},
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
			UpdateReconciliationSourceInput{Name: &emptyName},
		)
		require.Error(t, err)
		assert.Equal(t, ErrSourceNameRequired, err)
		assert.Equal(t, originalName, source.Name)
		assert.Equal(t, originalType, source.Type)
		assert.Equal(t, originalConfig, source.Config)
		assert.Equal(t, originalUpdatedAt, source.UpdatedAt)
	})

	t.Run("fails with invalid type", func(t *testing.T) {
		t.Parallel()

		source := createSource(t)
		originalName := source.Name
		originalType := source.Type
		originalConfig := source.Config
		originalUpdatedAt := source.UpdatedAt

		invalidType := value_objects.SourceType("INVALID")
		err := source.Update(
			context.Background(),
			UpdateReconciliationSourceInput{Type: &invalidType},
		)
		require.Error(t, err)
		assert.Equal(t, ErrSourceTypeInvalid, err)
		assert.Equal(t, originalName, source.Name)
		assert.Equal(t, originalType, source.Type)
		assert.Equal(t, originalConfig, source.Config)
		assert.Equal(t, originalUpdatedAt, source.UpdatedAt)
	})

	t.Run("fails with invalid side", func(t *testing.T) {
		t.Parallel()

		source := createSource(t)
		invalidSide := sharedfee.MatchingSideAny
		err := source.Update(context.Background(), UpdateReconciliationSourceInput{Side: &invalidSide})
		require.Error(t, err)
		assert.Equal(t, ErrSourceSideInvalid, err)
	})

	t.Run("fails with nil receiver", func(t *testing.T) {
		t.Parallel()

		newName := "Updated"
		err := (*ReconciliationSource)(
			nil,
		).Update(context.Background(), UpdateReconciliationSourceInput{Name: &newName})
		require.Error(t, err)
		assert.Equal(t, ErrNilReconciliationSource, err)
	})
}
