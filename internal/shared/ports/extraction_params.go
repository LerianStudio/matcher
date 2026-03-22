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

// ExtractionFilters defines the supported extraction filter DSL.
// Date ranges are expressed at the top-level extraction request via StartDate/EndDate.
// Filters are constrained to equality matches only.
type ExtractionFilters struct {
	Equals map[string]string `json:"equals,omitempty"`
}

// UnmarshalJSON rejects unknown keys and enforces string equality values.
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

			for equalsKey, equalsValue := range result.Equals {
				if strings.TrimSpace(equalsKey) == "" {
					return fmt.Errorf("%w: equals keys must not be blank", ErrInvalidExtractionFilters)
				}

				result.Equals[equalsKey] = strings.TrimSpace(equalsValue)
			}
		default:
			return fmt.Errorf("%w: unsupported filter key %q", ErrInvalidExtractionFilters, key)
		}
	}

	*filters = result

	return nil
}

// ToMap converts typed filters into the persisted/shared map form.
func (filters *ExtractionFilters) ToMap() map[string]any {
	if filters == nil {
		return nil
	}

	result := make(map[string]any)

	if len(filters.Equals) > 0 {
		equals := make(map[string]any, len(filters.Equals))
		for key, value := range filters.Equals {
			equals[key] = value
		}

		result[extractionFiltersEqualsKey] = equals
	}

	if len(result) == 0 {
		return nil
	}

	return result
}

// ExtractionFiltersFromMap converts persisted/shared map data into the typed filter form.
func ExtractionFiltersFromMap(raw map[string]any) (*ExtractionFilters, error) {
	if len(raw) == 0 {
		return nil, nil
	}

	data, err := json.Marshal(raw)
	if err != nil {
		return nil, fmt.Errorf("marshal extraction filters: %w", err)
	}

	var filters ExtractionFilters
	if err := json.Unmarshal(data, &filters); err != nil {
		return nil, err
	}

	return &filters, nil
}

// ExtractionParams holds parameters for starting a Fetcher extraction job.
type ExtractionParams struct {
	StartDate string
	EndDate   string
	Filters   *ExtractionFilters
}
