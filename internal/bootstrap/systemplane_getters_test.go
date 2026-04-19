// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package bootstrap

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/lib-commons/v5/commons/systemplane"
)

// newGetterTestClient builds a real systemplane Client backed by the shared
// in-memory noop store and immediately starts it so Set is callable. The
// caller is expected to Register keys before Start via registerFn.
//
// The start transition is required because systemplane.Client.Set returns
// ErrNotStarted before Start is called — tests that exercise the full
// Register → Start → Set → Get roundtrip go through this helper.
//
// Distinct from newStartedTestClient (systemplane_overrides_test.go) which
// always registers the full matcher key set via RegisterMatcherKeys — this
// helper takes a callback so tests can register a single ad-hoc key with a
// controlled type and default value.
func newGetterTestClient(t *testing.T, registerFn func(c *systemplane.Client)) *systemplane.Client {
	t.Helper()

	client, err := systemplane.NewForTesting(&noopSystemplaneStore{})
	require.NoError(t, err)

	t.Cleanup(func() { _ = client.Close() })

	if registerFn != nil {
		registerFn(client)
	}

	require.NoError(t, client.Start(context.Background()))

	return client
}

// --- SystemplaneGetString ---

// TestSystemplaneGetString_UnregisteredKey asserts the not-found branch
// returns the fallback when the key was never registered on the client.
func TestSystemplaneGetString_UnregisteredKey(t *testing.T) {
	t.Parallel()

	client := newGetterTestClient(t, nil)

	got := SystemplaneGetString(client, "missing.key", "fallback")

	assert.Equal(t, "fallback", got)
}

// TestSystemplaneGetString_CorrectType asserts a registered string key is
// returned as-is.
func TestSystemplaneGetString_CorrectType(t *testing.T) {
	t.Parallel()

	client := newGetterTestClient(t, func(c *systemplane.Client) {
		require.NoError(t, c.Register(systemplaneNamespace, "string.key", "hello"))
	})

	got := SystemplaneGetString(client, "string.key", "fallback")

	assert.Equal(t, "hello", got)
}

// TestSystemplaneGetString_WrongType asserts a registered non-string key
// falls through to the fallback (type-assertion default branch).
func TestSystemplaneGetString_WrongType(t *testing.T) {
	t.Parallel()

	client := newGetterTestClient(t, func(c *systemplane.Client) {
		// Registered as int; SystemplaneGetString must reject it.
		require.NoError(t, c.Register(systemplaneNamespace, "wrong.type.string", 42))
	})

	got := SystemplaneGetString(client, "wrong.type.string", "fallback")

	assert.Equal(t, "fallback", got)
}

// --- SystemplaneGetInt ---

// TestSystemplaneGetInt_UnregisteredKey asserts the not-found branch returns
// the fallback.
func TestSystemplaneGetInt_UnregisteredKey(t *testing.T) {
	t.Parallel()

	client := newGetterTestClient(t, nil)

	got := SystemplaneGetInt(client, "missing.key", 99)

	assert.Equal(t, 99, got)
}

// TestSystemplaneGetInt_IntBranch asserts the int case of the type-switch
// is exercised. Before Set is called, the registered default (an int literal)
// is returned directly without going through JSON marshal, so the cached
// value is int — not float64.
func TestSystemplaneGetInt_IntBranch(t *testing.T) {
	t.Parallel()

	client := newGetterTestClient(t, func(c *systemplane.Client) {
		require.NoError(t, c.Register(systemplaneNamespace, "int.key", 1234))
	})

	got := SystemplaneGetInt(client, "int.key", 0)

	assert.Equal(t, 1234, got, "registered int default must be returned via the int branch")
}

// TestSystemplaneGetInt_Float64Branch asserts the float64 case of the
// type-switch is exercised. After Set, the canonical cached value is
// round-tripped through json.Unmarshal(..., any) which decodes numbers as
// float64 — so a subsequent Get delivers float64, not int.
func TestSystemplaneGetInt_Float64Branch(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	client := newGetterTestClient(t, func(c *systemplane.Client) {
		require.NoError(t, c.Register(systemplaneNamespace, "int.via.set", 0))
	})

	require.NoError(t, client.Set(ctx, systemplaneNamespace, "int.via.set", 987, "test"))

	got := SystemplaneGetInt(client, "int.via.set", 0)

	assert.Equal(t, 987, got, "JSON-roundtripped int must decode through the float64 branch")
}

// TestSystemplaneGetInt_WrongType asserts a registered non-int key falls
// through to the fallback (type-switch default branch).
func TestSystemplaneGetInt_WrongType(t *testing.T) {
	t.Parallel()

	client := newGetterTestClient(t, func(c *systemplane.Client) {
		require.NoError(t, c.Register(systemplaneNamespace, "wrong.type.int", "not-a-number"))
	})

	got := SystemplaneGetInt(client, "wrong.type.int", 42)

	assert.Equal(t, 42, got, "string value must fall through to fallback")
}

// --- SystemplaneGetInt64 ---

// TestSystemplaneGetInt64_NilClient asserts nil-client returns the fallback
// without panicking. SystemplaneGetInt64 had zero coverage before this suite
// per the Gate 3 audit — this fills the most fundamental gap.
func TestSystemplaneGetInt64_NilClient(t *testing.T) {
	t.Parallel()

	got := SystemplaneGetInt64(nil, "some.key", int64(42))

	assert.Equal(t, int64(42), got)
}

// TestSystemplaneGetInt64_UnregisteredKey asserts the not-found branch
// returns the fallback.
func TestSystemplaneGetInt64_UnregisteredKey(t *testing.T) {
	t.Parallel()

	client := newGetterTestClient(t, nil)

	got := SystemplaneGetInt64(client, "missing.key", int64(999))

	assert.Equal(t, int64(999), got)
}

// TestSystemplaneGetInt64_Int64Branch asserts the int64 case of the
// type-switch. An explicit int64 literal routes through the first case arm.
func TestSystemplaneGetInt64_Int64Branch(t *testing.T) {
	t.Parallel()

	client := newGetterTestClient(t, func(c *systemplane.Client) {
		require.NoError(t, c.Register(systemplaneNamespace, "int64.key", int64(1234567890)))
	})

	got := SystemplaneGetInt64(client, "int64.key", 0)

	assert.Equal(t, int64(1234567890), got)
}

// TestSystemplaneGetInt64_IntBranch asserts an int-registered key is widened
// through the int branch of the type-switch.
func TestSystemplaneGetInt64_IntBranch(t *testing.T) {
	t.Parallel()

	client := newGetterTestClient(t, func(c *systemplane.Client) {
		require.NoError(t, c.Register(systemplaneNamespace, "int.as.int64", 1234))
	})

	got := SystemplaneGetInt64(client, "int.as.int64", 0)

	assert.Equal(t, int64(1234), got)
}

// TestSystemplaneGetInt64_Float64Branch asserts the float64 case of the
// type-switch. Values written through Set are round-tripped via JSON and
// returned as float64 on subsequent Gets.
func TestSystemplaneGetInt64_Float64Branch(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	client := newGetterTestClient(t, func(c *systemplane.Client) {
		require.NoError(t, c.Register(systemplaneNamespace, "int64.via.set", int64(0)))
	})

	require.NoError(t, client.Set(ctx, systemplaneNamespace, "int64.via.set", int64(5_000_000_000), "test"))

	got := SystemplaneGetInt64(client, "int64.via.set", 0)

	assert.Equal(t, int64(5_000_000_000), got, "JSON-roundtripped int64 must decode through the float64 branch")
}

// TestSystemplaneGetInt64_WrongType asserts a non-numeric registered key
// falls through to the fallback (type-switch default branch).
func TestSystemplaneGetInt64_WrongType(t *testing.T) {
	t.Parallel()

	client := newGetterTestClient(t, func(c *systemplane.Client) {
		require.NoError(t, c.Register(systemplaneNamespace, "wrong.type.int64", "not-a-number"))
	})

	got := SystemplaneGetInt64(client, "wrong.type.int64", int64(7))

	assert.Equal(t, int64(7), got)
}

// --- SystemplaneGetBool ---

// TestSystemplaneGetBool_UnregisteredKey asserts the not-found branch
// returns the fallback.
func TestSystemplaneGetBool_UnregisteredKey(t *testing.T) {
	t.Parallel()

	client := newGetterTestClient(t, nil)

	got := SystemplaneGetBool(client, "missing.key", true)

	assert.True(t, got)
}

// TestSystemplaneGetBool_CorrectType asserts a registered bool key is
// returned as-is.
func TestSystemplaneGetBool_CorrectType(t *testing.T) {
	t.Parallel()

	client := newGetterTestClient(t, func(c *systemplane.Client) {
		require.NoError(t, c.Register(systemplaneNamespace, "bool.key", true))
	})

	got := SystemplaneGetBool(client, "bool.key", false)

	assert.True(t, got)
}

// TestSystemplaneGetBool_WrongType asserts a registered non-bool key falls
// through to the fallback (type-assertion default branch).
func TestSystemplaneGetBool_WrongType(t *testing.T) {
	t.Parallel()

	client := newGetterTestClient(t, func(c *systemplane.Client) {
		require.NoError(t, c.Register(systemplaneNamespace, "wrong.type.bool", "yes"))
	})

	got := SystemplaneGetBool(client, "wrong.type.bool", true)

	assert.True(t, got, "string value must fall through to fallback (true)")
}
