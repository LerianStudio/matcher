//go:build unit

// Copyright 2025 Lerian Studio.

package registry

import (
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/LerianStudio/matcher/pkg/systemplane/domain"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// testKeyDef returns a minimal valid KeyDef for testing.
func testKeyDef(key string, kind domain.Kind) domain.KeyDef {
	return domain.KeyDef{
		Key:              key,
		Kind:             kind,
		AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
		ValueType:        domain.ValueTypeString,
		DefaultValue:     "default",
		RedactPolicy:     domain.RedactNone,
		ApplyBehavior:    domain.ApplyLiveRead,
		MutableAtRuntime: true,
		Description:      "test key",
	}
}

func TestRegister_ValidDef_Succeeds(t *testing.T) {
	t.Parallel()

	reg := New()

	err := reg.Register(testKeyDef("app.name", domain.KindConfig))

	require.NoError(t, err)
}

func TestRegister_DuplicateKey_Fails(t *testing.T) {
	t.Parallel()

	reg := New()
	def := testKeyDef("app.name", domain.KindConfig)

	err := reg.Register(def)
	require.NoError(t, err)

	err = reg.Register(def)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "already registered")
}

func TestRegister_InvalidDef_EmptyKey_Fails(t *testing.T) {
	t.Parallel()

	reg := New()
	def := testKeyDef("", domain.KindConfig)

	err := reg.Register(def)

	require.Error(t, err)
	assert.ErrorIs(t, err, domain.ErrKeyUnknown)
}

func TestRegister_InvalidDef_BadKind_Fails(t *testing.T) {
	t.Parallel()

	reg := New()
	def := testKeyDef("app.name", domain.Kind("bogus"))

	err := reg.Register(def)

	require.Error(t, err)
	assert.ErrorIs(t, err, domain.ErrInvalidKind)
}

func TestMustRegister_PanicsOnDuplicate(t *testing.T) {
	t.Parallel()

	reg := New()
	def := testKeyDef("app.name", domain.KindConfig)
	reg.MustRegister(def)

	assert.Panics(t, func() {
		reg.MustRegister(def)
	})
}

func TestMustRegister_PanicsOnInvalidDef(t *testing.T) {
	t.Parallel()

	reg := New()
	def := testKeyDef("", domain.KindConfig) // empty key

	assert.Panics(t, func() {
		reg.MustRegister(def)
	})
}

func TestGet_RegisteredKey_ReturnsDefAndTrue(t *testing.T) {
	t.Parallel()

	reg := New()
	def := testKeyDef("app.name", domain.KindConfig)
	reg.MustRegister(def)

	got, ok := reg.Get("app.name")

	assert.True(t, ok)
	assert.Equal(t, "app.name", got.Key)
	assert.Equal(t, domain.KindConfig, got.Kind)
}

func TestGet_UnknownKey_ReturnsFalse(t *testing.T) {
	t.Parallel()

	reg := New()

	_, ok := reg.Get("no.such.key")

	assert.False(t, ok)
}

func TestList_FiltersByKind(t *testing.T) {
	t.Parallel()

	reg := New()
	reg.MustRegister(testKeyDef("cfg.a", domain.KindConfig))
	reg.MustRegister(testKeyDef("cfg.b", domain.KindConfig))
	reg.MustRegister(testKeyDef("set.x", domain.KindSetting))

	configs := reg.List(domain.KindConfig)
	settings := reg.List(domain.KindSetting)

	assert.Len(t, configs, 2)
	assert.Len(t, settings, 1)
	assert.Equal(t, "set.x", settings[0].Key)
}

func TestList_ReturnsSortedByKeyName(t *testing.T) {
	t.Parallel()

	reg := New()
	reg.MustRegister(testKeyDef("z.last", domain.KindConfig))
	reg.MustRegister(testKeyDef("a.first", domain.KindConfig))
	reg.MustRegister(testKeyDef("m.middle", domain.KindConfig))

	result := reg.List(domain.KindConfig)

	require.Len(t, result, 3)
	assert.Equal(t, "a.first", result[0].Key)
	assert.Equal(t, "m.middle", result[1].Key)
	assert.Equal(t, "z.last", result[2].Key)
}

func TestList_EmptyForNonMatchingKind(t *testing.T) {
	t.Parallel()

	reg := New()
	reg.MustRegister(testKeyDef("cfg.a", domain.KindConfig))

	result := reg.List(domain.KindSetting)

	assert.Empty(t, result)
}

func TestValidate_ValidValue_Passes(t *testing.T) {
	t.Parallel()

	reg := New()
	reg.MustRegister(testKeyDef("app.name", domain.KindConfig))

	err := reg.Validate("app.name", "my-app")

	require.NoError(t, err)
}

func TestValidate_UnknownKey_Fails(t *testing.T) {
	t.Parallel()

	reg := New()

	err := reg.Validate("no.such.key", "value")

	require.Error(t, err)
	assert.ErrorIs(t, err, domain.ErrKeyUnknown)
}

func TestValidate_WrongType_Fails(t *testing.T) {
	t.Parallel()

	reg := New()
	reg.MustRegister(testKeyDef("app.name", domain.KindConfig)) // expects string

	err := reg.Validate("app.name", 42)

	require.Error(t, err)
	assert.ErrorIs(t, err, domain.ErrValueInvalid)
}

func TestValidate_NilValue_Reset_AlwaysPasses(t *testing.T) {
	t.Parallel()

	reg := New()
	reg.MustRegister(testKeyDef("app.name", domain.KindConfig))

	err := reg.Validate("app.name", nil)

	require.NoError(t, err)
}

func TestValidate_CustomValidator_Called(t *testing.T) {
	t.Parallel()

	reg := New()
	def := testKeyDef("app.name", domain.KindConfig)
	def.Validator = func(value any) error {
		s, _ := value.(string)
		if s == "forbidden" {
			return errors.New("value not allowed")
		}

		return nil
	}
	reg.MustRegister(def)

	err := reg.Validate("app.name", "forbidden")

	require.Error(t, err)
	assert.Contains(t, err.Error(), "value not allowed")
}

func TestValidate_CustomValidator_Passes(t *testing.T) {
	t.Parallel()

	reg := New()
	def := testKeyDef("app.name", domain.KindConfig)
	def.Validator = func(_ any) error { return nil }
	reg.MustRegister(def)

	err := reg.Validate("app.name", "allowed")

	require.NoError(t, err)
}

func TestConcurrent_RegisterAndGet(t *testing.T) {
	t.Parallel()

	reg := New()

	const numGoroutines = 50

	var wg sync.WaitGroup

	wg.Add(numGoroutines)

	// Each goroutine registers a unique key and then reads it back.
	for i := range numGoroutines {
		go func(idx int) {
			defer wg.Done()

			def := testKeyDef(
				fmt.Sprintf("concurrent.key.%d", idx),
				domain.KindConfig,
			)

			err := reg.Register(def)
			assert.NoError(t, err)

			got, ok := reg.Get(def.Key)
			assert.True(t, ok)
			assert.Equal(t, def.Key, got.Key)
		}(i)
	}

	wg.Wait()

	assert.Len(t, reg.List(domain.KindConfig), numGoroutines)
}

func TestConcurrent_RegisterDuplicate(t *testing.T) {
	t.Parallel()

	reg := New()

	const numGoroutines = 20

	var (
		wg       sync.WaitGroup
		mu       sync.Mutex
		errCount int
	)

	wg.Add(numGoroutines)

	// All goroutines try to register the same key; exactly one should succeed.
	for range numGoroutines {
		go func() {
			defer wg.Done()

			def := testKeyDef("shared.key", domain.KindConfig)

			if err := reg.Register(def); err != nil {
				mu.Lock()
				errCount++
				mu.Unlock()
			}
		}()
	}

	wg.Wait()

	// Exactly (numGoroutines - 1) should have failed.
	assert.Equal(t, numGoroutines-1, errCount)

	_, ok := reg.Get("shared.key")
	assert.True(t, ok)
}
