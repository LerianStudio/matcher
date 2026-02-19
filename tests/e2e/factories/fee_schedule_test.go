//go:build e2e

package factories

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/LerianStudio/matcher/tests/e2e/client"
)

func TestFeeScheduleBuilder_Defaults(t *testing.T) {
	t.Parallel()

	b := &FeeScheduleBuilder{
		req: client.CreateFeeScheduleRequest{
			Name:             "test-schedule",
			Currency:         "USD",
			ApplicationOrder: "PARALLEL",
			RoundingScale:    2,
			RoundingMode:     "HALF_UP",
			Items:            []client.CreateFeeScheduleItemRequest{},
		},
	}

	req := b.GetRequest()
	assert.Equal(t, "USD", req.Currency)
	assert.Equal(t, "PARALLEL", req.ApplicationOrder)
	assert.Equal(t, 2, req.RoundingScale)
	assert.Equal(t, "HALF_UP", req.RoundingMode)
}

func TestFeeScheduleBuilder_WithMethods(t *testing.T) {
	t.Parallel()

	b := &FeeScheduleBuilder{
		req: client.CreateFeeScheduleRequest{
			Items: []client.CreateFeeScheduleItemRequest{},
		},
	}

	b.WithRawName("my-schedule").
		WithCurrency("EUR").
		Cascading().
		WithRoundingScale(4).
		WithRoundingMode("BANKERS").
		WithFlatFee("flat", 1, "1.00").
		WithPercentageFee("pct", 2, "2.5")

	req := b.GetRequest()
	assert.Equal(t, "my-schedule", req.Name)
	assert.Equal(t, "EUR", req.Currency)
	assert.Equal(t, "CASCADING", req.ApplicationOrder)
	assert.Equal(t, 4, req.RoundingScale)
	assert.Equal(t, "BANKERS", req.RoundingMode)
	assert.Len(t, req.Items, 2)
	assert.Equal(t, "FLAT", req.Items[0].StructureType)
	assert.Equal(t, "PERCENTAGE", req.Items[1].StructureType)
}
