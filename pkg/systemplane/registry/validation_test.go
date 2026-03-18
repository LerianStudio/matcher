//go:build unit

// Copyright 2025 Lerian Studio.

package registry

import (
	"errors"
	"math"
	"testing"

	"github.com/LerianStudio/matcher/pkg/systemplane/domain"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCheckValueType_String_Valid(t *testing.T) {
	t.Parallel()

	err := checkValueType("hello", domain.ValueTypeString)

	require.NoError(t, err)
}

func TestCheckValueType_String_Invalid(t *testing.T) {
	t.Parallel()

	err := checkValueType(42, domain.ValueTypeString)

	require.Error(t, err)
	assert.ErrorIs(t, err, domain.ErrValueInvalid)
	assert.Contains(t, err.Error(), "expected string")
}

func TestCheckValueType_Int_WithInt(t *testing.T) {
	t.Parallel()

	err := checkValueType(42, domain.ValueTypeInt)

	require.NoError(t, err)
}

func TestCheckValueType_Int_WithInt64(t *testing.T) {
	t.Parallel()

	err := checkValueType(int64(42), domain.ValueTypeInt)

	require.NoError(t, err)
}

func TestCheckValueType_Int_WithFloat64NoFraction(t *testing.T) {
	t.Parallel()

	// JSON numbers arrive as float64; 42.0 is a valid int.
	err := checkValueType(float64(42), domain.ValueTypeInt)

	require.NoError(t, err)
}

func TestCheckValueType_Int_WithFloat64Fraction_Rejects(t *testing.T) {
	t.Parallel()

	err := checkValueType(3.14, domain.ValueTypeInt)

	require.Error(t, err)
	assert.ErrorIs(t, err, domain.ErrValueInvalid)
}

func TestCheckValueType_Int_RejectsString(t *testing.T) {
	t.Parallel()

	err := checkValueType("42", domain.ValueTypeInt)

	require.Error(t, err)
	assert.ErrorIs(t, err, domain.ErrValueInvalid)
}

func TestCheckValueType_Int_RejectsBool(t *testing.T) {
	t.Parallel()

	err := checkValueType(true, domain.ValueTypeInt)

	require.Error(t, err)
	assert.ErrorIs(t, err, domain.ErrValueInvalid)
}

func TestCheckValueType_Bool_Valid(t *testing.T) {
	t.Parallel()

	err := checkValueType(true, domain.ValueTypeBool)

	require.NoError(t, err)
}

func TestCheckValueType_Bool_ValidFalse(t *testing.T) {
	t.Parallel()

	err := checkValueType(false, domain.ValueTypeBool)

	require.NoError(t, err)
}

func TestCheckValueType_Bool_Invalid(t *testing.T) {
	t.Parallel()

	err := checkValueType("true", domain.ValueTypeBool)

	require.Error(t, err)
	assert.ErrorIs(t, err, domain.ErrValueInvalid)
	assert.Contains(t, err.Error(), "expected bool")
}

func TestCheckValueType_Float_WithFloat64(t *testing.T) {
	t.Parallel()

	err := checkValueType(3.14, domain.ValueTypeFloat)

	require.NoError(t, err)
}

func TestCheckValueType_Float_WithFloat32(t *testing.T) {
	t.Parallel()

	err := checkValueType(float32(3.14), domain.ValueTypeFloat)

	require.NoError(t, err)
}

func TestCheckValueType_Float_WithInt(t *testing.T) {
	t.Parallel()

	// int is widened to float.
	err := checkValueType(42, domain.ValueTypeFloat)

	require.NoError(t, err)
}

func TestCheckValueType_Float_WithInt64(t *testing.T) {
	t.Parallel()

	err := checkValueType(int64(42), domain.ValueTypeFloat)

	require.NoError(t, err)
}

func TestCheckValueType_Float_RejectsString(t *testing.T) {
	t.Parallel()

	err := checkValueType("3.14", domain.ValueTypeFloat)

	require.Error(t, err)
	assert.ErrorIs(t, err, domain.ErrValueInvalid)
}

func TestCheckValueType_Float_RejectsBool(t *testing.T) {
	t.Parallel()

	err := checkValueType(true, domain.ValueTypeFloat)

	require.Error(t, err)
	assert.ErrorIs(t, err, domain.ErrValueInvalid)
}

func TestCheckValueType_Object_WithMap(t *testing.T) {
	t.Parallel()

	err := checkValueType(map[string]any{"enabled": true}, domain.ValueTypeObject)

	require.NoError(t, err)
}

func TestCheckValueType_Object_RejectsScalar(t *testing.T) {
	t.Parallel()

	err := checkValueType("not-an-object", domain.ValueTypeObject)

	require.Error(t, err)
	assert.ErrorIs(t, err, domain.ErrValueInvalid)
}

func TestCheckValueType_Array_WithSlice(t *testing.T) {
	t.Parallel()

	err := checkValueType([]any{"a", 1, true}, domain.ValueTypeArray)

	require.NoError(t, err)
}

func TestCheckValueType_Array_WithTypedArray(t *testing.T) {
	t.Parallel()

	err := checkValueType([2]int{1, 2}, domain.ValueTypeArray)

	require.NoError(t, err)
}

func TestCheckValueType_Array_RejectsScalar(t *testing.T) {
	t.Parallel()

	err := checkValueType(42, domain.ValueTypeArray)

	require.Error(t, err)
	assert.ErrorIs(t, err, domain.ErrValueInvalid)
}

func TestCheckValueType_Nil_AlwaysValid(t *testing.T) {
	t.Parallel()

	types := []domain.ValueType{
		domain.ValueTypeString,
		domain.ValueTypeInt,
		domain.ValueTypeBool,
		domain.ValueTypeFloat,
	}

	for _, vt := range types {
		t.Run(string(vt), func(t *testing.T) {
			t.Parallel()

			err := checkValueType(nil, vt)

			require.NoError(t, err)
		})
	}
}

func TestCheckValueType_UnsupportedType_ReturnsError(t *testing.T) {
	t.Parallel()

	err := checkValueType("value", domain.ValueType("bytes"))

	require.Error(t, err)
	assert.ErrorIs(t, err, domain.ErrValueInvalid)
	assert.Contains(t, err.Error(), "unsupported value type")
}

func TestValidateValue_PassingCustomValidator(t *testing.T) {
	t.Parallel()

	def := domain.KeyDef{
		Key:           "app.port",
		Kind:          domain.KindConfig,
		AllowedScopes: []domain.Scope{domain.ScopeGlobal},
		ValueType:     domain.ValueTypeInt,
		Validator: func(value any) error {
			return nil
		},
		ApplyBehavior: domain.ApplyLiveRead,
	}

	err := validateValue(def, 8080)

	require.NoError(t, err)
}

func TestValidateValue_FailingCustomValidator(t *testing.T) {
	t.Parallel()

	def := domain.KeyDef{
		Key:           "app.port",
		Kind:          domain.KindConfig,
		AllowedScopes: []domain.Scope{domain.ScopeGlobal},
		ValueType:     domain.ValueTypeInt,
		Validator: func(value any) error {
			v, _ := value.(int)
			if v < 1024 {
				return errors.New("port must be >= 1024")
			}

			return nil
		},
		ApplyBehavior: domain.ApplyLiveRead,
	}

	err := validateValue(def, 80)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "port must be >= 1024")
}

func TestValidateValue_NilValidator_Passes(t *testing.T) {
	t.Parallel()

	def := domain.KeyDef{
		Key:           "app.name",
		Kind:          domain.KindConfig,
		AllowedScopes: []domain.Scope{domain.ScopeGlobal},
		ValueType:     domain.ValueTypeString,
		Validator:     nil,
		ApplyBehavior: domain.ApplyLiveRead,
	}

	err := validateValue(def, "my-app")

	require.NoError(t, err)
}

func TestValidateValue_TypeMismatch_SkipsValidator(t *testing.T) {
	t.Parallel()

	validatorCalled := false

	def := domain.KeyDef{
		Key:           "app.port",
		Kind:          domain.KindConfig,
		AllowedScopes: []domain.Scope{domain.ScopeGlobal},
		ValueType:     domain.ValueTypeInt,
		Validator: func(_ any) error {
			validatorCalled = true

			return nil
		},
		ApplyBehavior: domain.ApplyLiveRead,
	}

	err := validateValue(def, "not-an-int")

	require.Error(t, err)
	assert.ErrorIs(t, err, domain.ErrValueInvalid)
	assert.False(t, validatorCalled, "custom validator should not be called when type check fails")
}

func TestCheckValueType_Int_RejectsInfinity(t *testing.T) {
	t.Parallel()

	posInf := math.Inf(1)

	err := checkValueType(posInf, domain.ValueTypeInt)

	require.Error(t, err)
	assert.ErrorIs(t, err, domain.ErrValueInvalid)
}

func TestCheckValueType_Int_RejectsNaN(t *testing.T) {
	t.Parallel()

	nan := math.NaN()

	err := checkValueType(nan, domain.ValueTypeInt)

	require.Error(t, err)
	assert.ErrorIs(t, err, domain.ErrValueInvalid)
}

func TestCheckValueType_Int_LargeFloat64WithoutFraction(t *testing.T) {
	t.Parallel()

	// Large round number that fits in float64 without fraction.
	err := checkValueType(float64(1e15), domain.ValueTypeInt)

	require.NoError(t, err)
}

func TestCheckValueType_Int_NegativeFloat64WithoutFraction(t *testing.T) {
	t.Parallel()

	err := checkValueType(float64(-100), domain.ValueTypeInt)

	require.NoError(t, err)
}

func TestCheckValueType_Int_ZeroFloat64(t *testing.T) {
	t.Parallel()

	err := checkValueType(float64(0), domain.ValueTypeInt)

	require.NoError(t, err)
}
