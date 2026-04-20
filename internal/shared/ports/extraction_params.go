package ports

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

// ErrInvalidExtractionFilters indicates the filter payload does not match the supported schema.
var ErrInvalidExtractionFilters = errors.New("invalid extraction filters")

const extractionFiltersEqualsKey = "equals"

// fetcherOperatorEq is the JSON tag Fetcher uses for equality conditions.
// See FilterCondition.Equals in fetcher/pkg/model/job/job_queue.go.
const fetcherOperatorEq = "eq"

// ExtractionFilters defines the supported extraction filter DSL.
// Date ranges are expressed at the top-level extraction request via StartDate/EndDate.
// Filters are constrained to equality matches only.
type ExtractionFilters struct {
	Equals map[string]string `json:"equals,omitempty"`
}

// UnmarshalJSON rejects unknown keys and enforces string equality values.
//
// The accepted wire format is {"equals": {"column": "value", ...}}.
// Internally, filters are persisted and transmitted to Fetcher using the
// column-operator-values format produced by ToMap (e.g. {"col": {"eq": ["val"]}}).
// ExtractionFiltersFromMap accepts both formats defensively for cache/deploy
// edge cases, even though the greenfield deployment has no prior persisted data
// in the legacy shape.
func (filters *ExtractionFilters) UnmarshalJSON(data []byte) error {
	if len(data) == 0 || string(data) == "null" {
		*filters = ExtractionFilters{}

		return nil
	}

	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return fmt.Errorf("%w: decode filter object: %w", ErrInvalidExtractionFilters, err)
	}

	result := ExtractionFilters{}

	for key, value := range raw {
		switch key {
		case extractionFiltersEqualsKey:
			if err := json.Unmarshal(value, &result.Equals); err != nil {
				return fmt.Errorf("%w: equals must be an object of string values", ErrInvalidExtractionFilters)
			}

			trimmed := make(map[string]string, len(result.Equals))

			for equalsKey, equalsValue := range result.Equals {
				k := strings.TrimSpace(equalsKey)
				if k == "" {
					return fmt.Errorf("%w: equals keys must not be blank", ErrInvalidExtractionFilters)
				}

				trimmed[k] = strings.TrimSpace(equalsValue)
			}

			result.Equals = trimmed
		default:
			return fmt.Errorf("%w: unsupported filter key %q", ErrInvalidExtractionFilters, key)
		}
	}

	*filters = result

	return nil
}

// ToMap converts typed filters into Fetcher's wire format: column -> {operator: [values]}.
// This matches the FilterCondition struct in Fetcher's job_queue.go where
// operator keys are JSON tags ("eq", "gt", etc.) and values are always slices.
//
// Example output: {"currency": {"eq": ["USD"]}, "status": {"eq": ["active"]}}.
//
// ExtractionFiltersFromMap can parse both this format and the legacy
// {"equals": {"k": "v"}} shape for backward compatibility.
func (filters *ExtractionFilters) ToMap() map[string]any {
	if filters == nil {
		return nil
	}

	result := make(map[string]any, len(filters.Equals))

	for column, value := range filters.Equals {
		result[column] = map[string]any{
			fetcherOperatorEq: []any{value},
		}
	}

	if len(result) == 0 {
		return nil
	}

	return result
}

// ExtractionFiltersFromMap reverses ToMap(), converting Fetcher wire format
// (column -> {"eq": [values]}) back into the typed ExtractionFilters.
//
// Defense-in-depth: transparently accepts the legacy {"equals": {"col": "val"}}
// format that shipped in earlier experimental builds. No prior persisted data is
// expected in this format (greenfield confirmed), but handling it defensively
// prevents hard failures from cache staleness, in-flight deploys, or inspection
// tools that write in the older shape.
//
// Detection: if the map has exactly one key "equals" whose value is a map,
// treat it as the legacy format. Otherwise, assume the new column-operator-values format.
func ExtractionFiltersFromMap(raw map[string]any) (*ExtractionFilters, error) {
	if len(raw) == 0 {
		return nil, nil
	}

	// Legacy format detection: exactly one top-level key "equals" with a map value.
	if len(raw) == 1 {
		if nested, ok := raw[extractionFiltersEqualsKey].(map[string]any); ok {
			return legacyEqualsFromMap(nested)
		}
	}

	equals := make(map[string]string, len(raw))

	for column, condRaw := range raw {
		cond, ok := condRaw.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("%w: column %q: condition must be an object", ErrInvalidExtractionFilters, column)
		}

		eqRaw, ok := cond[fetcherOperatorEq]
		if !ok {
			return nil, fmt.Errorf("%w: column %q: missing %q operator", ErrInvalidExtractionFilters, column, fetcherOperatorEq)
		}

		eqSlice, ok := eqRaw.([]any)
		if !ok {
			return nil, fmt.Errorf("%w: column %q: %q must be an array", ErrInvalidExtractionFilters, column, fetcherOperatorEq)
		}

		if len(eqSlice) == 0 {
			return nil, fmt.Errorf("%w: column %q: %q must not be empty", ErrInvalidExtractionFilters, column, fetcherOperatorEq)
		}

		val, ok := eqSlice[0].(string)
		if !ok {
			return nil, fmt.Errorf("%w: column %q: %q values must be strings", ErrInvalidExtractionFilters, column, fetcherOperatorEq)
		}

		equals[column] = val
	}

	if len(equals) == 0 {
		return nil, nil
	}

	return &ExtractionFilters{Equals: equals}, nil
}

// legacyEqualsFromMap converts the old {"equals": {"col": "val"}} shape into
// the new ExtractionFilters representation. Scalar string values become single-
// element slices; array values are flattened. Non-string values are silently
// skipped — this is a best-effort compatibility path, not a validation gate.
func legacyEqualsFromMap(equals map[string]any) (*ExtractionFilters, error) {
	filters := &ExtractionFilters{Equals: make(map[string]string, len(equals))}

	for col, v := range equals {
		if strings.TrimSpace(col) == "" {
			continue
		}

		switch val := v.(type) {
		case string:
			filters.Equals[col] = val
		case []any:
			for _, item := range val {
				if s, ok := item.(string); ok {
					filters.Equals[col] = s

					break // take first string value, matching ExtractionFilters single-value model
				}
			}
		default:
			// skip unsupported types silently — this is a legacy compatibility path
		}
	}

	if len(filters.Equals) == 0 {
		return nil, nil
	}

	return filters, nil
}

// ExtractionParams holds parameters for starting a Fetcher extraction job.
//
// StartDate and EndDate are validated (date format, chronological order) and persisted
// in the ExtractionRequest entity for audit/history purposes, but they are NOT forwarded
// to Fetcher's ExtractionJobInput wire format. Fetcher uses column-level equality filters
// (via Filters) rather than top-level date ranges.
type ExtractionParams struct {
	StartDate string
	EndDate   string
	Filters   *ExtractionFilters
}
