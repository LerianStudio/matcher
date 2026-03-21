// Copyright 2025 Lerian Studio.

package postgres

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

var errNilJSONPayload = errors.New("postgres store: json payload is nil")

// decodeJSONValue decodes JSON into Go values while preserving integer values
// as int when representable, instead of defaulting all numbers to float64.
func decodeJSONValue(raw []byte) (any, error) {
	if raw == nil {
		return nil, errNilJSONPayload
	}

	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()

	var value any
	if err := dec.Decode(&value); err != nil {
		return nil, fmt.Errorf("decode json payload: %w", err)
	}

	return normalizeJSONValue(value)
}

func normalizeJSONValue(value any) (any, error) {
	switch typedValue := value.(type) {
	case json.Number:
		return normalizeJSONNumber(typedValue)
	case map[string]any:
		normalized := make(map[string]any, len(typedValue))
		for key, item := range typedValue {
			norm, err := normalizeJSONValue(item)
			if err != nil {
				return nil, err
			}

			normalized[key] = norm
		}

		return normalized, nil
	case []any:
		normalized := make([]any, len(typedValue))
		for index := range typedValue {
			norm, err := normalizeJSONValue(typedValue[index])
			if err != nil {
				return nil, err
			}

			normalized[index] = norm
		}

		return normalized, nil
	default:
		return value, nil
	}
}

func normalizeJSONNumber(number json.Number) (any, error) {
	text := number.String()
	if strings.ContainsAny(text, ".eE") {
		floatValue, err := number.Float64()
		if err != nil {
			return nil, fmt.Errorf("parse float number %q: %w", text, err)
		}

		return floatValue, nil
	}

	int64Value, err := number.Int64()
	if err != nil {
		floatValue, floatErr := number.Float64()
		if floatErr != nil {
			return nil, fmt.Errorf("parse number %q: %w", text, err)
		}

		return floatValue, nil
	}

	maxInt := int64(^uint(0) >> 1)
	minInt := -maxInt - 1

	if int64Value >= minInt && int64Value <= maxInt {
		return int(int64Value), nil
	}

	return int64Value, nil
}
