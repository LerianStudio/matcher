package value_objects

import (
	"errors"
	"fmt"
	"strings"
)

// ErrInvalidSourceType indicates an invalid source type value.
var ErrInvalidSourceType = errors.New("invalid source type")

// SourceType defines the supported reconciliation source categories.
// @Description Category of data source for reconciliation
// @Enum LEDGER,BANK,GATEWAY,CUSTOM,FETCHER
// swagger:enum SourceType
type SourceType string

// Supported source type values.
const (
	// SourceTypeLedger indicates a ledger source.
	SourceTypeLedger SourceType = "LEDGER"
	// SourceTypeBank indicates a bank source.
	SourceTypeBank SourceType = "BANK"
	// SourceTypeGateway indicates a gateway source.
	SourceTypeGateway SourceType = "GATEWAY"
	// SourceTypeCustom indicates a user-defined custom source.
	SourceTypeCustom SourceType = "CUSTOM"
	// SourceTypeFetcher indicates a fetcher source.
	SourceTypeFetcher SourceType = "FETCHER"
)

// Valid reports whether the source type is supported.
func (st SourceType) Valid() bool {
	switch st {
	case SourceTypeLedger, SourceTypeBank, SourceTypeGateway, SourceTypeCustom, SourceTypeFetcher:
		return true
	}

	return false
}

// IsValid reports whether the source type is supported.
// This is an alias for Valid() to maintain API consistency.
func (st SourceType) IsValid() bool {
	return st.Valid()
}

// String returns the string representation of the source type.
func (st SourceType) String() string {
	return string(st)
}

// ParseSourceType parses a string into a SourceType.
// Input is normalized to uppercase for defense-in-depth,
// matching the pattern used by ParseContextStatus.
func ParseSourceType(s string) (SourceType, error) {
	st := SourceType(strings.ToUpper(s))
	if !st.Valid() {
		return "", fmt.Errorf("%w: %s", ErrInvalidSourceType, s)
	}

	return st, nil
}
