package value_objects

import (
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
)

// SourceType is a type alias for the canonical shared.SourceType.
// The canonical definition lives in internal/shared/domain so that both
// Configuration and Ingestion contexts can reference the same type without
// either owning it. The alias is preserved here for backward-compatibility
// of the many call sites that import value_objects.SourceType.
//
// See docs/handoffs/simplify/T-007-followup.md for the promotion rationale.
type SourceType = shared.SourceType

// Supported source type values — re-exported from the shared kernel.
const (
	SourceTypeLedger  = shared.SourceTypeLedger
	SourceTypeBank    = shared.SourceTypeBank
	SourceTypeGateway = shared.SourceTypeGateway
	SourceTypeCustom  = shared.SourceTypeCustom
	SourceTypeFetcher = shared.SourceTypeFetcher
)

// ErrInvalidSourceType is re-exported from the shared kernel.
var ErrInvalidSourceType = shared.ErrInvalidSourceType

// ParseSourceType is re-exported from the shared kernel via a function value.
// Go does not permit aliasing functions with `type` or `const`, so a `var`
// holding the function reference is used. Call sites see an identical API.
var ParseSourceType = shared.ParseSourceType
