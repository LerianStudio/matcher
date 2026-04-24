// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package services

import (
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"strconv"
	"strings"

	"github.com/shopspring/decimal"
)

func getBool(configMap map[string]any, key string, def bool) (bool, error) {
	value, ok := configMap[key]
	if !ok || value == nil {
		return def, nil
	}

	boolVal, ok := value.(bool)
	if !ok {
		return def, wrapDecodeErr(key + " must be bool")
	}

	return boolVal, nil
}

// hasExplicitFalse checks if a key exists in the config and is explicitly set to false.
// Returns false if the key is missing or set to any other value.
func hasExplicitFalse(configMap map[string]any, key string) bool {
	value, ok := configMap[key]
	if !ok {
		return false
	}

	boolVal, ok := value.(bool)
	if !ok {
		return false
	}

	return !boolVal
}

func getInt(configMap map[string]any, key string, def int) (int, error) {
	value, ok := configMap[key]
	if !ok || value == nil {
		return def, nil
	}

	parsed, err := parseIntValue(value)
	if err != nil {
		return def, wrapDecodeErr(key + ": " + err.Error())
	}

	return parsed, nil
}

func parseIntValue(value any) (int, error) {
	switch num := value.(type) {
	case int:
		return num, nil
	case int32:
		return int(num), nil
	case int64:
		return parseInt64Value(num)
	case float64:
		return parseFloat64Value(num)
	case json.Number:
		return parseJSONNumberValue(num)
	case string:
		return parseStringValue(num)
	default:
		return parseReflectValue(value)
	}
}

func parseInt64Value(num int64) (int, error) {
	if int64(int(num)) != num {
		return 0, errOutOfRange
	}

	return int(num), nil
}

func parseFloat64Value(num float64) (int, error) {
	maxInt := float64(^uint(0) >> 1)
	minInt := -maxInt - 1

	if math.Trunc(num) != num {
		return 0, errMustBeInt
	}

	if num > maxInt || num < minInt {
		return 0, errOutOfRange
	}

	return int(num), nil
}

func parseJSONNumberValue(num json.Number) (int, error) {
	parsed, err := num.Int64()
	if err != nil {
		return 0, errMustBeInt
	}

	if int64(int(parsed)) != parsed {
		return 0, errOutOfRange
	}

	return int(parsed), nil
}

func parseStringValue(num string) (int, error) {
	parsed, err := strconv.Atoi(strings.TrimSpace(num))
	if err != nil {
		return 0, errMustBeInt
	}

	return parsed, nil
}

func parseReflectValue(value any) (int, error) {
	v := reflect.ValueOf(value)
	if !v.IsValid() || v.Kind() < reflect.Int || v.Kind() > reflect.Int64 {
		return 0, errMustBeInt
	}

	intVal := v.Int()
	if int64(int(intVal)) != intVal {
		return 0, errOutOfRange
	}

	return int(intVal), nil
}

func getDecimal(
	configMap map[string]any,
	key string,
	def decimal.Decimal,
) (decimal.Decimal, error) {
	value, ok := configMap[key]
	if !ok || value == nil {
		return def, nil
	}

	switch typedVal := value.(type) {
	case string:
		decVal, err := decimal.NewFromString(typedVal)
		if err != nil {
			return decimal.Decimal{}, wrapDecodeErr(key + " invalid decimal string")
		}

		return decVal, nil
	case float64:
		floatStr := strconv.FormatFloat(typedVal, 'f', -1, 64)

		decVal, err := decimal.NewFromString(floatStr)
		if err != nil {
			return decimal.Decimal{}, wrapDecodeErr(key + " invalid float value")
		}

		return decVal, nil
	case int:
		return decimal.NewFromInt(int64(typedVal)), nil
	case int32:
		return decimal.NewFromInt(int64(typedVal)), nil
	case int64:
		return decimal.NewFromInt(typedVal), nil
	case json.Number:
		decVal, err := decimal.NewFromString(typedVal.String())
		if err != nil {
			return decimal.Decimal{}, wrapDecodeErr(key + " invalid json.Number")
		}

		return decVal, nil
	default:
		return decimal.Decimal{}, wrapDecodeErr(key + " unsupported type")
	}
}

func wrapDecodeErr(msg string) error {
	return fmt.Errorf("%w: %s", ErrRuleConfigDecode, msg)
}

func validateScore(score int) error {
	if score < 0 || score > 100 {
		return wrapDecodeErr("score must be between 0 and 100")
	}

	return nil
}
