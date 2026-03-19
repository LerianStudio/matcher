package fee

// MatchingSide indicates which side of a match run a fee rule applies to.
type MatchingSide string

const (
	// MatchingSideLeft applies only to left-side transactions.
	MatchingSideLeft MatchingSide = "LEFT"
	// MatchingSideRight applies only to right-side transactions.
	MatchingSideRight MatchingSide = "RIGHT"
	// MatchingSideAny applies to transactions on both sides.
	MatchingSideAny MatchingSide = "ANY"
)

// IsValid returns true if the matching side is a recognized value.
func (s MatchingSide) IsValid() bool {
	switch s {
	case MatchingSideLeft, MatchingSideRight, MatchingSideAny:
		return true
	default:
		return false
	}
}

// AppliesToLeft returns true if this side applies to left-side transactions.
func (s MatchingSide) AppliesToLeft() bool {
	return s == MatchingSideLeft || s == MatchingSideAny
}

// AppliesToRight returns true if this side applies to right-side transactions.
func (s MatchingSide) AppliesToRight() bool {
	return s == MatchingSideRight || s == MatchingSideAny
}
