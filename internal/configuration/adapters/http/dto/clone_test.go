//go:build unit

package dto

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"

	"github.com/LerianStudio/matcher/internal/configuration/domain/entities"
	"github.com/LerianStudio/matcher/internal/configuration/domain/value_objects"
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
)

func TestCloneResultToResponse_Nil(t *testing.T) {
	t.Parallel()

	resp := CloneResultToResponse(nil)
	assert.Equal(t, CloneContextResponse{
		Context: ReconciliationContextResponse{},
	}, resp)
}

func TestCloneResultToResponse_WithData(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()

	result := &entities.CloneResult{
		Context: &entities.ReconciliationContext{
			ID:              uuid.New(),
			TenantID:        uuid.New(),
			Name:            "Cloned Context",
			Type:            shared.ContextType("1:1"),
			Interval:        "daily",
			Status:          value_objects.ContextStatusActive,
			FeeToleranceAbs: decimal.Zero,
			FeeTolerancePct: decimal.Zero,
			CreatedAt:       now,
			UpdatedAt:       now,
		},
		SourcesCloned:   3,
		RulesCloned:     5,
		FeeRulesCloned:  2,
		FieldMapsCloned: 2,
	}

	resp := CloneResultToResponse(result)

	assert.Equal(t, "Cloned Context", resp.Context.Name)
	assert.Equal(t, 3, resp.SourcesCloned)
	assert.Equal(t, 5, resp.RulesCloned)
	assert.Equal(t, 2, resp.FeeRulesCloned)
	assert.Equal(t, 2, resp.FieldMapsCloned)
}
