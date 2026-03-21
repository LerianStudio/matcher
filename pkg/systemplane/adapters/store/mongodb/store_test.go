// Copyright 2025 Lerian Studio.

//go:build unit

package mongodb

import (
	"context"
	"testing"

	"github.com/LerianStudio/matcher/pkg/systemplane/domain"
	"github.com/LerianStudio/matcher/pkg/systemplane/ports"
	"github.com/stretchr/testify/assert"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

// TestStoreImplementsInterface verifies compile-time interface compliance.
func TestStoreImplementsInterface(t *testing.T) {
	t.Parallel()

	// This is a compile-time check enforced by the package-level var.
	// Re-asserting here makes the test output explicit.
	var _ ports.Store = (*Store)(nil)
}

func TestTargetFilter_GlobalScope(t *testing.T) {
	t.Parallel()

	target := domain.Target{
		Kind:      domain.KindConfig,
		Scope:     domain.ScopeGlobal,
		SubjectID: "",
	}

	filter := targetFilter(target)

	expected := bson.D{
		{Key: "kind", Value: "config"},
		{Key: "scope", Value: "global"},
		{Key: "subject", Value: ""},
	}

	assert.Equal(t, expected, filter)
}

func TestTargetFilter_TenantScope(t *testing.T) {
	t.Parallel()

	target := domain.Target{
		Kind:      domain.KindSetting,
		Scope:     domain.ScopeTenant,
		SubjectID: "tenant-456",
	}

	filter := targetFilter(target)

	expected := bson.D{
		{Key: "kind", Value: "setting"},
		{Key: "scope", Value: "tenant"},
		{Key: "subject", Value: "tenant-456"},
	}

	assert.Equal(t, expected, filter)
}

func TestRevisionMetaFilter(t *testing.T) {
	t.Parallel()

	target := domain.Target{
		Kind:      domain.KindConfig,
		Scope:     domain.ScopeGlobal,
		SubjectID: "",
	}

	filter := revisionMetaFilter(target)

	expected := bson.D{
		{Key: "kind", Value: "config"},
		{Key: "scope", Value: "global"},
		{Key: "subject", Value: ""},
		{Key: "key", Value: revisionMetaKey},
	}

	assert.Equal(t, expected, filter)
}

func TestRevisionMetaFilter_TenantScope(t *testing.T) {
	t.Parallel()

	target := domain.Target{
		Kind:      domain.KindSetting,
		Scope:     domain.ScopeTenant,
		SubjectID: "tenant-789",
	}

	filter := revisionMetaFilter(target)

	expected := bson.D{
		{Key: "kind", Value: "setting"},
		{Key: "scope", Value: "tenant"},
		{Key: "subject", Value: "tenant-789"},
		{Key: "key", Value: revisionMetaKey},
	}

	assert.Equal(t, expected, filter)
}

func TestStoreStructFieldsNotNil(t *testing.T) {
	t.Parallel()

	// A zero-value Store has nil fields. Verify that using a zero-value
	// would not compile into something unexpected -- the struct layout is
	// as expected.
	s := &Store{}

	assert.Nil(t, s.client)
	assert.Nil(t, s.entries)
	assert.Nil(t, s.history)
}

func TestStore_Get_NilDependenciesReturnSentinelErrors(t *testing.T) {
	t.Parallel()

	target := domain.Target{Kind: domain.KindConfig, Scope: domain.ScopeGlobal}

	var nilStore *Store
	_, err := nilStore.Get(context.Background(), target)
	assert.ErrorIs(t, err, ErrNilStore)

	// Zero-value Store has nil client — validated before entries.
	zeroValueStore := &Store{}
	_, err = zeroValueStore.Get(context.Background(), target)
	assert.ErrorIs(t, err, ErrNilClient)

	// Non-nil client but nil entries collection.
	clientOnlyStore := &Store{client: &mongo.Client{}}
	_, err = clientOnlyStore.Get(context.Background(), target)
	assert.ErrorIs(t, err, ErrNilEntries)
}

func TestStore_Put_NilDependenciesReturnSentinelErrors(t *testing.T) {
	t.Parallel()

	target := domain.Target{Kind: domain.KindConfig, Scope: domain.ScopeGlobal}

	var nilStore *Store
	_, err := nilStore.Put(context.Background(), target, nil, domain.RevisionZero, domain.Actor{}, "test")
	assert.ErrorIs(t, err, ErrNilStore)

	zeroValueStore := &Store{}
	_, err = zeroValueStore.Put(context.Background(), target, nil, domain.RevisionZero, domain.Actor{}, "test")
	assert.ErrorIs(t, err, ErrNilClient)
}

func TestStore_CurrentRevisionInCollection_NilCollection(t *testing.T) {
	t.Parallel()

	store := &Store{}
	target := domain.Target{Kind: domain.KindConfig, Scope: domain.ScopeGlobal}

	_, err := store.currentRevisionInCollection(context.Background(), nil, target)
	assert.ErrorIs(t, err, ErrNilEntries)
}

func TestStore_ReadEntriesInCollection_NilCollection(t *testing.T) {
	t.Parallel()

	store := &Store{}
	target := domain.Target{Kind: domain.KindConfig, Scope: domain.ScopeGlobal}

	_, err := store.readEntriesInCollection(context.Background(), nil, target)
	assert.ErrorIs(t, err, ErrNilEntries)
}
