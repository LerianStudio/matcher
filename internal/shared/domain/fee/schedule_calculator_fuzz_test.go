//go:build unit

package fee

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"
)

func FuzzCalculateGrossFromNet_RoundTrip(f *testing.F) {
	f.Add(int64(10000), int64(150), int64(30), false)
	f.Add(int64(250000), int64(275), int64(99), true)
	f.Add(int64(999999), int64(50), int64(0), false)

	f.Fuzz(func(t *testing.T, netCents int64, rateBasisPoints int64, flatFeeCents int64, cascading bool) {
		if netCents < 0 || rateBasisPoints < 0 || rateBasisPoints > 9000 || flatFeeCents < 0 {
			t.Skip()
		}

		ctx := context.Background()
		order := ApplicationOrderParallel
		if cascading {
			order = ApplicationOrderCascading
		}

		net := Money{Amount: decimal.NewFromInt(netCents).Shift(-2), Currency: "USD"}
		rate := decimal.NewFromInt(rateBasisPoints).Shift(-4)
		flat := decimal.NewFromInt(flatFeeCents).Shift(-2)

		schedule := &FeeSchedule{
			ID:               uuid.New(),
			Currency:         "USD",
			ApplicationOrder: order,
			RoundingScale:    2,
			RoundingMode:     RoundingModeHalfUp,
			Items: []FeeScheduleItem{
				{
					ID:        uuid.New(),
					Name:      "percentage",
					Priority:  1,
					Structure: PercentageFee{Rate: rate},
				},
				{
					ID:        uuid.New(),
					Name:      "flat",
					Priority:  2,
					Structure: FlatFee{Amount: flat},
				},
			},
		}

		gross, breakdown, err := CalculateGrossFromNet(ctx, net, schedule)
		require.NoError(t, err, "bounded flat+percentage schedules should converge")

		require.NotNil(t, breakdown)
		require.Equal(t, net.Currency, gross.Currency)
		require.Equal(t, net.Currency, breakdown.TotalFee.Currency)
		require.Equal(t, net.Currency, breakdown.NetAmount.Currency)

		recalculated, err := CalculateSchedule(ctx, gross, schedule)
		require.NoError(t, err)
		require.NotNil(t, recalculated)

		diff := recalculated.NetAmount.Amount.Sub(net.Amount).Abs()
		maxAllowedDiff := decimal.RequireFromString("0.01")
		if diff.GreaterThan(maxAllowedDiff) {
			t.Fatalf("round-trip net mismatch: got %s want %s diff %s", recalculated.NetAmount.Amount, net.Amount, diff)
		}
	})
}
