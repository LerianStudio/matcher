//go:build unit

package value_objects_test

import (
	"errors"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/LerianStudio/matcher/internal/configuration/domain/value_objects"
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
)

// TestSourceTypeAlias verifies that value_objects.SourceType is a Go type
// alias for shared.SourceType (not a distinct new type). Compile-time
// identity is asserted via direct assignment across the alias boundary.
// Full behaviour tests for the underlying type live at
// internal/shared/domain/source_type_test.go — duplicating them here would
// exercise the same code via the alias and add no signal.
//
// See docs/handoffs/simplify/T-007-followup.md for the consolidation rationale.
func TestSourceTypeAlias(t *testing.T) {
	t.Parallel()

	// Alias identity: a value typed as value_objects.SourceType is directly
	// assignable to shared.SourceType with zero conversion.
	var vo value_objects.SourceType = value_objects.SourceTypeLedger
	var sh shared.SourceType = vo
	_ = sh

	// Re-exported constants point at the canonical shared kernel values.
	assert.Equal(t, shared.SourceTypeLedger, value_objects.SourceTypeLedger)
	assert.Equal(t, shared.SourceTypeBank, value_objects.SourceTypeBank)
	assert.Equal(t, shared.SourceTypeGateway, value_objects.SourceTypeGateway)
	assert.Equal(t, shared.SourceTypeCustom, value_objects.SourceTypeCustom)
	assert.Equal(t, shared.SourceTypeFetcher, value_objects.SourceTypeFetcher)

	// Sentinel error is the same instance (var re-export).
	assert.True(t, errors.Is(value_objects.ErrInvalidSourceType, shared.ErrInvalidSourceType))

	// ParseSourceType is re-exported via `var`, so the function value must
	// be pointer-identical to the shared kernel version.
	assert.Equal(t,
		reflect.ValueOf(shared.ParseSourceType).Pointer(),
		reflect.ValueOf(value_objects.ParseSourceType).Pointer(),
	)
}
