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
