// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package fee

// NormalizationMode defines how fee schedules are applied to normalize transaction amounts.
type NormalizationMode string

const (
	// NormalizationModeNone means no fee normalization is applied.
	NormalizationModeNone NormalizationMode = ""
	// NormalizationModeNet means gross → net (deduct fees from amount).
	NormalizationModeNet NormalizationMode = "NET"
	// NormalizationModeGross means net → gross (add fees back to amount).
	NormalizationModeGross NormalizationMode = "GROSS"
)

// IsValid returns true if the normalization mode is a recognized value.
func (m NormalizationMode) IsValid() bool {
	switch m {
	case NormalizationModeNone, NormalizationModeNet, NormalizationModeGross:
		return true
	default:
		return false
	}
}
