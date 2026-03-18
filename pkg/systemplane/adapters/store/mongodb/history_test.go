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
)

// TestHistoryStoreImplementsInterface verifies compile-time interface compliance.
func TestHistoryStoreImplementsInterface(t *testing.T) {
	t.Parallel()

	var _ ports.HistoryStore = (*HistoryStore)(nil)
}

func TestBuildHistoryFilter_AllFieldsSet(t *testing.T) {
	t.Parallel()

	filter := ports.HistoryFilter{
		Kind:      domain.KindConfig,
		Scope:     domain.ScopeGlobal,
		SubjectID: "subject-1",
		Key:       "timeout_ms",
		Limit:     10,
		Offset:    5,
	}

	got := buildHistoryFilter(filter)

	// Limit and Offset are applied via Find options, not in the BSON filter.
	expected := bson.D{
		{Key: "kind", Value: "config"},
		{Key: "scope", Value: "global"},
		{Key: "subject", Value: "subject-1"},
		{Key: "key", Value: "timeout_ms"},
	}

	assert.Equal(t, expected, got)
}

func TestBuildHistoryFilter_Empty(t *testing.T) {
	t.Parallel()

	filter := ports.HistoryFilter{}

	got := buildHistoryFilter(filter)

	// An empty filter should produce an empty BSON document (match all).
	assert.Equal(t, bson.D{}, got)
}

func TestBuildHistoryFilter_OnlyKey(t *testing.T) {
	t.Parallel()

	filter := ports.HistoryFilter{
		Key: "max_retries",
	}

	got := buildHistoryFilter(filter)

	expected := bson.D{
		{Key: "key", Value: "max_retries"},
	}

	assert.Equal(t, expected, got)
}

func TestBuildHistoryFilter_OnlyScope(t *testing.T) {
	t.Parallel()

	filter := ports.HistoryFilter{
		Scope: domain.ScopeTenant,
	}

	got := buildHistoryFilter(filter)

	expected := bson.D{
		{Key: "scope", Value: "tenant"},
	}

	assert.Equal(t, expected, got)
}

func TestBuildHistoryFilter_KindAndSubject(t *testing.T) {
	t.Parallel()

	filter := ports.HistoryFilter{
		Kind:      domain.KindSetting,
		SubjectID: "tenant-abc",
	}

	got := buildHistoryFilter(filter)

	expected := bson.D{
		{Key: "kind", Value: "setting"},
		{Key: "subject", Value: "tenant-abc"},
	}

	assert.Equal(t, expected, got)
}

func TestBuildHistoryFilter_ScopeAndKey(t *testing.T) {
	t.Parallel()

	filter := ports.HistoryFilter{
		Scope: domain.ScopeGlobal,
		Key:   "feature_flag",
	}

	got := buildHistoryFilter(filter)

	expected := bson.D{
		{Key: "scope", Value: "global"},
		{Key: "key", Value: "feature_flag"},
	}

	assert.Equal(t, expected, got)
}

func TestHistoryStoreStructFieldsNotNil(t *testing.T) {
	t.Parallel()

	// Zero-value HistoryStore should have nil collection.
	h := &HistoryStore{}

	assert.Nil(t, h.history)
}

func TestHistoryStore_ListHistory_NilDependenciesReturnSentinelErrors(t *testing.T) {
	t.Parallel()

	var nilHistoryStore *HistoryStore
	_, err := nilHistoryStore.ListHistory(context.Background(), ports.HistoryFilter{})
	assert.ErrorIs(t, err, ErrNilStore)

	zeroValueHistoryStore := &HistoryStore{}
	_, err = zeroValueHistoryStore.ListHistory(context.Background(), ports.HistoryFilter{})
	assert.ErrorIs(t, err, ErrNilHistory)
}
