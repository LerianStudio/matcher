// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package exports

import (
	"github.com/LerianStudio/matcher/internal/reporting/domain/entities"
)

// VarianceExportRow defines the stable machine-readable export contract for variance reports.
type VarianceExportRow struct {
	SourceID        string  `json:"source_id" xml:"source_id"`
	Currency        string  `json:"currency" xml:"currency"`
	FeeScheduleID   string  `json:"fee_schedule_id" xml:"fee_schedule_id"`
	FeeScheduleName string  `json:"fee_schedule_name" xml:"fee_schedule_name"`
	TotalExpected   string  `json:"total_expected" xml:"total_expected"`
	TotalActual     string  `json:"total_actual" xml:"total_actual"`
	NetVariance     string  `json:"net_variance" xml:"net_variance"`
	VariancePct     *string `json:"variance_pct,omitempty" xml:"variance_pct,omitempty"`
}

// NewVarianceExportRow maps an internal variance row to the stable machine-readable export shape.
func NewVarianceExportRow(row *entities.VarianceReportRow) *VarianceExportRow {
	if row == nil {
		return nil
	}

	result := &VarianceExportRow{
		SourceID:        row.SourceID.String(),
		Currency:        row.Currency,
		FeeScheduleID:   row.FeeScheduleID.String(),
		FeeScheduleName: row.FeeScheduleName,
		TotalExpected:   row.TotalExpected.String(),
		TotalActual:     row.TotalActual.String(),
		NetVariance:     row.NetVariance.String(),
	}

	if row.VariancePct != nil {
		variancePct := row.VariancePct.String()
		result.VariancePct = &variancePct
	}

	return result
}
