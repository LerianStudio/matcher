// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package exports

import (
	"testing"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/reporting/domain/entities"
)

func TestNewVarianceExportRow(t *testing.T) {
	t.Parallel()

	t.Run("nil row returns nil", func(t *testing.T) {
		t.Parallel()

		assert.Nil(t, NewVarianceExportRow(nil))
	})

	t.Run("maps stable export fields only", func(t *testing.T) {
		t.Parallel()

		variancePct := decimal.RequireFromString("5.25")
		row := &entities.VarianceReportRow{
			SourceID:        uuid.MustParse("00000000-0000-0000-0000-000000000001"),
			Currency:        "USD",
			FeeScheduleID:   uuid.MustParse("00000000-0000-0000-0000-000000000002"),
			FeeScheduleName: "Visa Domestic",
			TotalExpected:   decimal.RequireFromString("10.00"),
			TotalActual:     decimal.RequireFromString("12.00"),
			NetVariance:     decimal.RequireFromString("2.00"),
			VariancePct:     &variancePct,
		}

		exportRow := NewVarianceExportRow(row)
		require.NotNil(t, exportRow)
		assert.Equal(t, row.SourceID.String(), exportRow.SourceID)
		assert.Equal(t, row.Currency, exportRow.Currency)
		assert.Equal(t, row.FeeScheduleName, exportRow.FeeScheduleName)
		assert.Equal(t, row.TotalExpected.String(), exportRow.TotalExpected)
		assert.Equal(t, row.TotalActual.String(), exportRow.TotalActual)
		assert.Equal(t, row.NetVariance.String(), exportRow.NetVariance)
		require.NotNil(t, exportRow.VariancePct)
		assert.Equal(t, row.VariancePct.String(), *exportRow.VariancePct)
	})
}
