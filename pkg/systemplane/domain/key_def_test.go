//go:build unit

// Copyright 2025 Lerian Studio.

package domain

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func validKeyDef() KeyDef {
	return KeyDef{
		Key:              "postgres.max_open_conns",
		Kind:             KindConfig,
		AllowedScopes:    []Scope{ScopeGlobal, ScopeTenant},
		ValueType:        ValueTypeInt,
		DefaultValue:     25,
		RedactPolicy:     RedactNone,
		ApplyBehavior:    ApplyBundleRebuild,
		MutableAtRuntime: true,
		Description:      "Maximum number of open database connections.",
		Group:            "database",
	}
}

func TestKeyDef_Validate_Valid(t *testing.T) {
	t.Parallel()

	err := validKeyDef().Validate()

	require.NoError(t, err)
}

func TestKeyDef_Validate_EmptyKey(t *testing.T) {
	t.Parallel()

	kd := validKeyDef()
	kd.Key = ""

	err := kd.Validate()

	require.Error(t, err)
	assert.ErrorIs(t, err, ErrKeyUnknown)
}

func TestKeyDef_Validate_InvalidKind(t *testing.T) {
	t.Parallel()

	kd := validKeyDef()
	kd.Kind = Kind("bogus")

	err := kd.Validate()

	require.Error(t, err)
	assert.ErrorIs(t, err, ErrInvalidKind)
}

func TestKeyDef_Validate_NoAllowedScopes(t *testing.T) {
	t.Parallel()

	kd := validKeyDef()
	kd.AllowedScopes = nil

	err := kd.Validate()

	require.Error(t, err)
	assert.ErrorIs(t, err, ErrScopeInvalid)
}

func TestKeyDef_Validate_InvalidScope(t *testing.T) {
	t.Parallel()

	kd := validKeyDef()
	kd.AllowedScopes = []Scope{ScopeGlobal, Scope("nope")}

	err := kd.Validate()

	require.Error(t, err)
	assert.ErrorIs(t, err, ErrInvalidScope)
}

func TestKeyDef_Validate_InvalidValueType(t *testing.T) {
	t.Parallel()

	kd := validKeyDef()
	kd.ValueType = ValueType("bytes")

	err := kd.Validate()

	require.Error(t, err)
	assert.ErrorIs(t, err, ErrInvalidValueType)
}

func TestKeyDef_Validate_InvalidApplyBehavior(t *testing.T) {
	t.Parallel()

	kd := validKeyDef()
	kd.ApplyBehavior = ApplyBehavior("magic")

	err := kd.Validate()

	require.Error(t, err)
	assert.ErrorIs(t, err, ErrInvalidApplyBehavior)
}

func TestKeyDef_Validate_EmptyAllowedScopes(t *testing.T) {
	t.Parallel()

	kd := validKeyDef()
	kd.AllowedScopes = []Scope{}

	err := kd.Validate()

	require.Error(t, err)
	assert.ErrorIs(t, err, ErrScopeInvalid)
}

func TestValueType_IsValid(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		vt   ValueType
		want bool
	}{
		{name: "string is valid", vt: ValueTypeString, want: true},
		{name: "int is valid", vt: ValueTypeInt, want: true},
		{name: "bool is valid", vt: ValueTypeBool, want: true},
		{name: "float is valid", vt: ValueTypeFloat, want: true},
		{name: "object is valid", vt: ValueTypeObject, want: true},
		{name: "array is valid", vt: ValueTypeArray, want: true},
		{name: "empty is invalid", vt: ValueType(""), want: false},
		{name: "unknown is invalid", vt: ValueType("bytes"), want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, tt.vt.IsValid())
		})
	}
}

func TestParseValueType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		input   string
		want    ValueType
		wantErr bool
	}{
		{name: "string", input: "string", want: ValueTypeString},
		{name: "int", input: "int", want: ValueTypeInt},
		{name: "bool", input: "bool", want: ValueTypeBool},
		{name: "float", input: "float", want: ValueTypeFloat},
		{name: "object", input: "object", want: ValueTypeObject},
		{name: "array", input: "array", want: ValueTypeArray},
		{name: "invalid", input: "bytes", wantErr: true},
		{name: "empty", input: "", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := ParseValueType(tt.input)
			if tt.wantErr {
				require.Error(t, err)
				assert.ErrorIs(t, err, ErrInvalidValueType)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestKeyDef_ValidatorFunc(t *testing.T) {
	t.Parallel()

	kd := validKeyDef()
	kd.Validator = func(value any) error {
		return nil
	}

	err := kd.Validate()

	require.NoError(t, err)
	assert.NotNil(t, kd.Validator)
	assert.NoError(t, kd.Validator(25))
}
