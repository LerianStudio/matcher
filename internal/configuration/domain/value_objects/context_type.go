package value_objects

import (
	"fmt"

	shared "github.com/LerianStudio/matcher/internal/shared/domain"
)

// ContextType is an alias to the shared kernel ContextType.
type ContextType = shared.ContextType

// Re-export constants (as vars to avoid swag duplicate enum detection).
//
//nolint:gochecknoglobals // intentional re-export of shared constants
var (
	ContextTypeOneToOne   = shared.ContextTypeOneToOne
	ContextTypeOneToMany  = shared.ContextTypeOneToMany
	ContextTypeManyToMany = shared.ContextTypeManyToMany
)

// ErrInvalidContextType re-exports the shared kernel invalid context type error.
var ErrInvalidContextType = shared.ErrInvalidContextType

// ParseContextType parses a string into a ContextType.
func ParseContextType(s string) (ContextType, error) {
	ct, err := shared.ParseContextType(s)
	if err != nil {
		return "", fmt.Errorf("parsing context type: %w", err)
	}

	return ct, nil
}
