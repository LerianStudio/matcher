//go:build unit

// Copyright 2025 Lerian Studio.

package mongodb

import (
	"testing"

	"github.com/LerianStudio/matcher/pkg/systemplane/domain"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.mongodb.org/mongo-driver/v2/bson"
)

// ---------------------------------------------------------------------------
// bsonLookupString
// ---------------------------------------------------------------------------

func TestBsonLookupString_ExistingKey(t *testing.T) {
	t.Parallel()

	doc := &bson.D{{Key: "name", Value: "alice"}}

	result := bsonLookupString(doc, "name")

	assert.Equal(t, "alice", result)
}

func TestBsonLookupString_MissingKey(t *testing.T) {
	t.Parallel()

	doc := &bson.D{{Key: "name", Value: "alice"}}

	result := bsonLookupString(doc, "age")

	assert.Equal(t, "", result)
}

func TestBsonLookupString_NonStringValue(t *testing.T) {
	t.Parallel()

	doc := &bson.D{{Key: "count", Value: int64(42)}}

	result := bsonLookupString(doc, "count")

	assert.Equal(t, "", result)
}

func TestBsonLookupString_NilDoc(t *testing.T) {
	t.Parallel()

	result := bsonLookupString(nil, "key")

	assert.Equal(t, "", result)
}

func TestBsonLookupString_EmptyDoc(t *testing.T) {
	t.Parallel()

	doc := &bson.D{}

	result := bsonLookupString(doc, "key")

	assert.Equal(t, "", result)
}

func TestBsonLookupString_MultipleKeys(t *testing.T) {
	t.Parallel()

	doc := &bson.D{
		{Key: "first", Value: "one"},
		{Key: "second", Value: "two"},
		{Key: "third", Value: "three"},
	}

	assert.Equal(t, "one", bsonLookupString(doc, "first"))
	assert.Equal(t, "two", bsonLookupString(doc, "second"))
	assert.Equal(t, "three", bsonLookupString(doc, "third"))
}

func TestBsonLookupString_BoolValue(t *testing.T) {
	t.Parallel()

	doc := &bson.D{{Key: "active", Value: true}}

	result := bsonLookupString(doc, "active")

	assert.Equal(t, "", result)
}

// ---------------------------------------------------------------------------
// bsonLookupUint64
// ---------------------------------------------------------------------------

func TestBsonLookupUint64_Int32(t *testing.T) {
	t.Parallel()

	doc := &bson.D{{Key: "revision", Value: int32(42)}}

	val, ok := bsonLookupUint64(doc, "revision")

	require.True(t, ok)
	assert.Equal(t, uint64(42), val)
}

func TestBsonLookupUint64_Int64(t *testing.T) {
	t.Parallel()

	doc := &bson.D{{Key: "revision", Value: int64(999)}}

	val, ok := bsonLookupUint64(doc, "revision")

	require.True(t, ok)
	assert.Equal(t, uint64(999), val)
}

func TestBsonLookupUint64_Uint64(t *testing.T) {
	t.Parallel()

	doc := &bson.D{{Key: "revision", Value: uint64(1234)}}

	val, ok := bsonLookupUint64(doc, "revision")

	require.True(t, ok)
	assert.Equal(t, uint64(1234), val)
}

func TestBsonLookupUint64_NegativeInt32(t *testing.T) {
	t.Parallel()

	doc := &bson.D{{Key: "revision", Value: int32(-1)}}

	val, ok := bsonLookupUint64(doc, "revision")

	assert.False(t, ok)
	assert.Equal(t, uint64(0), val)
}

func TestBsonLookupUint64_NegativeInt64(t *testing.T) {
	t.Parallel()

	doc := &bson.D{{Key: "revision", Value: int64(-100)}}

	val, ok := bsonLookupUint64(doc, "revision")

	assert.False(t, ok)
	assert.Equal(t, uint64(0), val)
}

func TestBsonLookupUint64_Float64(t *testing.T) {
	t.Parallel()

	doc := &bson.D{{Key: "revision", Value: float64(3.14)}}

	val, ok := bsonLookupUint64(doc, "revision")

	assert.False(t, ok)
	assert.Equal(t, uint64(0), val)
}

func TestBsonLookupUint64_String(t *testing.T) {
	t.Parallel()

	doc := &bson.D{{Key: "revision", Value: "not-a-number"}}

	val, ok := bsonLookupUint64(doc, "revision")

	assert.False(t, ok)
	assert.Equal(t, uint64(0), val)
}

func TestBsonLookupUint64_MissingKey(t *testing.T) {
	t.Parallel()

	doc := &bson.D{{Key: "other", Value: int64(1)}}

	val, ok := bsonLookupUint64(doc, "revision")

	assert.False(t, ok)
	assert.Equal(t, uint64(0), val)
}

func TestBsonLookupUint64_NilDoc(t *testing.T) {
	t.Parallel()

	val, ok := bsonLookupUint64(nil, "revision")

	assert.False(t, ok)
	assert.Equal(t, uint64(0), val)
}

func TestBsonLookupUint64_EmptyDoc(t *testing.T) {
	t.Parallel()

	doc := &bson.D{}

	val, ok := bsonLookupUint64(doc, "revision")

	assert.False(t, ok)
	assert.Equal(t, uint64(0), val)
}

func TestBsonLookupUint64_ZeroInt32(t *testing.T) {
	t.Parallel()

	doc := &bson.D{{Key: "revision", Value: int32(0)}}

	val, ok := bsonLookupUint64(doc, "revision")

	require.True(t, ok)
	assert.Equal(t, uint64(0), val)
}

func TestBsonLookupUint64_ZeroInt64(t *testing.T) {
	t.Parallel()

	doc := &bson.D{{Key: "revision", Value: int64(0)}}

	val, ok := bsonLookupUint64(doc, "revision")

	require.True(t, ok)
	assert.Equal(t, uint64(0), val)
}

// ---------------------------------------------------------------------------
// targetFromDoc
// ---------------------------------------------------------------------------

func TestTargetFromDoc_ValidGlobalConfig(t *testing.T) {
	t.Parallel()

	doc := &bson.D{
		{Key: "kind", Value: "config"},
		{Key: "scope", Value: "global"},
		{Key: "subject", Value: ""},
		{Key: "revision", Value: int64(10)},
	}

	target, rev, _, ok := targetFromDoc(doc)

	require.True(t, ok)
	assert.Equal(t, domain.KindConfig, target.Kind)
	assert.Equal(t, domain.ScopeGlobal, target.Scope)
	assert.Equal(t, "", target.SubjectID)
	assert.Equal(t, domain.Revision(10), rev)
}

func TestTargetFromDoc_ValidTenantSetting(t *testing.T) {
	t.Parallel()

	doc := &bson.D{
		{Key: "kind", Value: "setting"},
		{Key: "scope", Value: "tenant"},
		{Key: "subject", Value: "tenant-xyz"},
		{Key: "revision", Value: int64(7)},
	}

	target, rev, _, ok := targetFromDoc(doc)

	require.True(t, ok)
	assert.Equal(t, domain.KindSetting, target.Kind)
	assert.Equal(t, domain.ScopeTenant, target.Scope)
	assert.Equal(t, "tenant-xyz", target.SubjectID)
	assert.Equal(t, domain.Revision(7), rev)
}

func TestTargetFromDoc_WithApplyBehavior(t *testing.T) {
	t.Parallel()

	doc := &bson.D{
		{Key: "kind", Value: "config"},
		{Key: "scope", Value: "global"},
		{Key: "subject", Value: ""},
		{Key: "revision", Value: int64(3)},
		{Key: "apply_behavior", Value: "worker-reconcile"},
	}

	_, _, behavior, ok := targetFromDoc(doc)

	require.True(t, ok)
	assert.Equal(t, domain.ApplyWorkerReconcile, behavior)
}

func TestTargetFromDoc_MissingApplyBehavior_DefaultsToZeroValue(t *testing.T) {
	t.Parallel()

	doc := &bson.D{
		{Key: "kind", Value: "config"},
		{Key: "scope", Value: "global"},
		{Key: "subject", Value: ""},
		{Key: "revision", Value: int64(1)},
	}

	_, _, behavior, ok := targetFromDoc(doc)

	require.True(t, ok)
	assert.Equal(t, domain.ApplyBehavior(""), behavior)
}

func TestTargetFromDoc_InvalidKind_Bson(t *testing.T) {
	t.Parallel()

	doc := &bson.D{
		{Key: "kind", Value: "bogus"},
		{Key: "scope", Value: "global"},
		{Key: "subject", Value: ""},
		{Key: "revision", Value: int64(1)},
	}

	_, _, _, ok := targetFromDoc(doc)

	assert.False(t, ok)
}

func TestTargetFromDoc_InvalidScope_Bson(t *testing.T) {
	t.Parallel()

	doc := &bson.D{
		{Key: "kind", Value: "config"},
		{Key: "scope", Value: "invalid"},
		{Key: "subject", Value: ""},
		{Key: "revision", Value: int64(1)},
	}

	_, _, _, ok := targetFromDoc(doc)

	assert.False(t, ok)
}

func TestTargetFromDoc_MissingFields_Bson(t *testing.T) {
	t.Parallel()

	doc := &bson.D{
		{Key: "revision", Value: int64(1)},
	}

	_, _, _, ok := targetFromDoc(doc)

	assert.False(t, ok)
}

func TestTargetFromDoc_MissingRevision_Bson(t *testing.T) {
	t.Parallel()

	doc := &bson.D{
		{Key: "kind", Value: "config"},
		{Key: "scope", Value: "global"},
		{Key: "subject", Value: ""},
	}

	_, _, _, ok := targetFromDoc(doc)

	assert.False(t, ok)
}

func TestTargetFromDoc_TenantScopeMissingSubject_Bson(t *testing.T) {
	t.Parallel()

	doc := &bson.D{
		{Key: "kind", Value: "setting"},
		{Key: "scope", Value: "tenant"},
		{Key: "subject", Value: ""},
		{Key: "revision", Value: int64(1)},
	}

	_, _, _, ok := targetFromDoc(doc)

	assert.False(t, ok)
}

func TestTargetFromDoc_GlobalScopeWithSubject_Bson(t *testing.T) {
	t.Parallel()

	doc := &bson.D{
		{Key: "kind", Value: "config"},
		{Key: "scope", Value: "global"},
		{Key: "subject", Value: "unexpected-id"},
		{Key: "revision", Value: int64(1)},
	}

	_, _, _, ok := targetFromDoc(doc)

	assert.False(t, ok)
}

func TestTargetFromDoc_Int32Revision(t *testing.T) {
	t.Parallel()

	doc := &bson.D{
		{Key: "kind", Value: "config"},
		{Key: "scope", Value: "global"},
		{Key: "subject", Value: ""},
		{Key: "revision", Value: int32(42)},
	}

	target, rev, _, ok := targetFromDoc(doc)

	require.True(t, ok)
	assert.Equal(t, domain.KindConfig, target.Kind)
	assert.Equal(t, domain.Revision(42), rev)
}

func TestTargetFromDoc_NegativeRevision(t *testing.T) {
	t.Parallel()

	doc := &bson.D{
		{Key: "kind", Value: "config"},
		{Key: "scope", Value: "global"},
		{Key: "subject", Value: ""},
		{Key: "revision", Value: int64(-5)},
	}

	_, _, _, ok := targetFromDoc(doc)

	assert.False(t, ok, "negative revision should be rejected by bsonLookupUint64")
}
