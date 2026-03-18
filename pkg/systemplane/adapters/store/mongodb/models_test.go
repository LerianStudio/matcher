// Copyright 2025 Lerian Studio.

//go:build unit

package mongodb

import (
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/LerianStudio/matcher/pkg/systemplane/domain"
	"github.com/LerianStudio/matcher/pkg/systemplane/ports"
	"github.com/stretchr/testify/assert"
)

func TestEntryDoc_ToDomainEntry(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()

	tests := []struct {
		name string
		doc  entryDoc
		want domain.Entry
	}{
		{
			name: "full entry with string value",
			doc: entryDoc{
				Kind:      "config",
				Scope:     "global",
				Subject:   "",
				Key:       "max_retries",
				Value:     5,
				Revision:  3,
				UpdatedAt: now,
				UpdatedBy: "admin",
				Source:    "api",
			},
			want: domain.Entry{
				Kind:      domain.KindConfig,
				Scope:     domain.ScopeGlobal,
				Subject:   "",
				Key:       "max_retries",
				Value:     5,
				Revision:  domain.Revision(3),
				UpdatedAt: now,
				UpdatedBy: "admin",
				Source:    "api",
			},
		},
		{
			name: "tenant scoped entry",
			doc: entryDoc{
				Kind:      "setting",
				Scope:     "tenant",
				Subject:   "tenant-123",
				Key:       "theme",
				Value:     "dark",
				Revision:  1,
				UpdatedAt: now,
				UpdatedBy: "user-456",
				Source:    "ui",
			},
			want: domain.Entry{
				Kind:      domain.KindSetting,
				Scope:     domain.ScopeTenant,
				Subject:   "tenant-123",
				Key:       "theme",
				Value:     "dark",
				Revision:  domain.Revision(1),
				UpdatedAt: now,
				UpdatedBy: "user-456",
				Source:    "ui",
			},
		},
		{
			name: "entry normalizes bson values",
			doc: entryDoc{
				Kind:      "config",
				Scope:     "global",
				Subject:   "",
				Key:       "complex_value",
				Value:     bson.D{{Key: "limit", Value: int32(5)}, {Key: "items", Value: bson.A{int64(2), bson.D{{Key: "enabled", Value: true}}}}},
				Revision:  4,
				UpdatedAt: now,
				UpdatedBy: "admin",
				Source:    "api",
			},
			want: domain.Entry{
				Kind:      domain.KindConfig,
				Scope:     domain.ScopeGlobal,
				Subject:   "",
				Key:       "complex_value",
				Value:     map[string]any{"limit": 5, "items": []any{2, map[string]any{"enabled": true}}},
				Revision:  domain.Revision(4),
				UpdatedAt: now,
				UpdatedBy: "admin",
				Source:    "api",
			},
		},
		{
			name: "entry with nil value",
			doc: entryDoc{
				Kind:      "config",
				Scope:     "global",
				Subject:   "",
				Key:       "nullable_key",
				Value:     nil,
				Revision:  0,
				UpdatedAt: now,
				UpdatedBy: "",
				Source:    "",
			},
			want: domain.Entry{
				Kind:      domain.KindConfig,
				Scope:     domain.ScopeGlobal,
				Subject:   "",
				Key:       "nullable_key",
				Value:     nil,
				Revision:  domain.RevisionZero,
				UpdatedAt: now,
				UpdatedBy: "",
				Source:    "",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := tt.doc.toDomainEntry()
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestHistoryDoc_ToHistoryEntry(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()

	tests := []struct {
		name string
		doc  historyDoc
		want ports.HistoryEntry
	}{
		{
			name: "full history entry with old and new values",
			doc: historyDoc{
				Kind:      "config",
				Scope:     "global",
				Subject:   "",
				Key:       "timeout_ms",
				OldValue:  1000,
				NewValue:  2000,
				Revision:  2,
				ActorID:   "admin",
				ChangedAt: now,
				Source:    "api",
			},
			want: ports.HistoryEntry{
				Revision:  domain.Revision(2),
				Key:       "timeout_ms",
				Scope:     domain.ScopeGlobal,
				SubjectID: "",
				OldValue:  1000,
				NewValue:  2000,
				ActorID:   "admin",
				ChangedAt: now,
			},
		},
		{
			name: "history normalizes bson values",
			doc: historyDoc{
				Kind:      "config",
				Scope:     "global",
				Subject:   "",
				Key:       "normalized",
				OldValue:  bson.D{{Key: "attempts", Value: int64(3)}},
				NewValue:  bson.A{int32(1), bson.D{{Key: "ok", Value: true}}},
				Revision:  6,
				ActorID:   "admin",
				ChangedAt: now,
				Source:    "api",
			},
			want: ports.HistoryEntry{
				Revision:  domain.Revision(6),
				Key:       "normalized",
				Scope:     domain.ScopeGlobal,
				SubjectID: "",
				OldValue:  map[string]any{"attempts": 3},
				NewValue:  []any{1, map[string]any{"ok": true}},
				ActorID:   "admin",
				ChangedAt: now,
			},
		},
		{
			name: "history entry for new key (nil old value)",
			doc: historyDoc{
				Kind:      "setting",
				Scope:     "tenant",
				Subject:   "tenant-abc",
				Key:       "feature_flag",
				OldValue:  nil,
				NewValue:  true,
				Revision:  1,
				ActorID:   "system",
				ChangedAt: now,
				Source:    "bootstrap",
			},
			want: ports.HistoryEntry{
				Revision:  domain.Revision(1),
				Key:       "feature_flag",
				Scope:     domain.ScopeTenant,
				SubjectID: "tenant-abc",
				OldValue:  nil,
				NewValue:  true,
				ActorID:   "system",
				ChangedAt: now,
			},
		},
		{
			name: "history entry for reset (nil new value)",
			doc: historyDoc{
				Kind:      "config",
				Scope:     "global",
				Subject:   "",
				Key:       "cache_ttl",
				OldValue:  60,
				NewValue:  nil,
				Revision:  5,
				ActorID:   "admin",
				ChangedAt: now,
				Source:    "api",
			},
			want: ports.HistoryEntry{
				Revision:  domain.Revision(5),
				Key:       "cache_ttl",
				Scope:     domain.ScopeGlobal,
				SubjectID: "",
				OldValue:  60,
				NewValue:  nil,
				ActorID:   "admin",
				ChangedAt: now,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := tt.doc.toHistoryEntry()
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestNewEntryDoc(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	target := domain.Target{
		Kind:      domain.KindConfig,
		Scope:     domain.ScopeGlobal,
		SubjectID: "",
	}
	actor := domain.Actor{ID: "test-user"}

	doc := newEntryDoc(target, "timeout_ms", 3000, domain.Revision(2), actor, "api", now)

	assert.Equal(t, "config", doc.Kind)
	assert.Equal(t, "global", doc.Scope)
	assert.Equal(t, "", doc.Subject)
	assert.Equal(t, "timeout_ms", doc.Key)
	assert.Equal(t, 3000, doc.Value)
	assert.Equal(t, uint64(2), doc.Revision)
	assert.Equal(t, now, doc.UpdatedAt)
	assert.Equal(t, "test-user", doc.UpdatedBy)
	assert.Equal(t, "api", doc.Source)
}

func TestNewEntryDoc_TenantScope(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	target := domain.Target{
		Kind:      domain.KindSetting,
		Scope:     domain.ScopeTenant,
		SubjectID: "tenant-xyz",
	}
	actor := domain.Actor{ID: "user-42"}

	doc := newEntryDoc(target, "theme", "dark", domain.Revision(1), actor, "ui", now)

	assert.Equal(t, "setting", doc.Kind)
	assert.Equal(t, "tenant", doc.Scope)
	assert.Equal(t, "tenant-xyz", doc.Subject)
	assert.Equal(t, "theme", doc.Key)
	assert.Equal(t, "dark", doc.Value)
	assert.Equal(t, uint64(1), doc.Revision)
	assert.Equal(t, "user-42", doc.UpdatedBy)
	assert.Equal(t, "ui", doc.Source)
}

func TestNewHistoryDoc(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	target := domain.Target{
		Kind:      domain.KindConfig,
		Scope:     domain.ScopeGlobal,
		SubjectID: "",
	}
	actor := domain.Actor{ID: "admin"}

	doc := newHistoryDoc(target, "max_retries", 3, 5, domain.Revision(2), actor, "api", now)

	assert.Equal(t, "config", doc.Kind)
	assert.Equal(t, "global", doc.Scope)
	assert.Equal(t, "", doc.Subject)
	assert.Equal(t, "max_retries", doc.Key)
	assert.Equal(t, 3, doc.OldValue)
	assert.Equal(t, 5, doc.NewValue)
	assert.Equal(t, uint64(2), doc.Revision)
	assert.Equal(t, "admin", doc.ActorID)
	assert.Equal(t, now, doc.ChangedAt)
	assert.Equal(t, "api", doc.Source)
}

func TestNewHistoryDoc_NilValues(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	target := domain.Target{
		Kind:      domain.KindConfig,
		Scope:     domain.ScopeGlobal,
		SubjectID: "",
	}
	actor := domain.Actor{ID: "system"}

	doc := newHistoryDoc(target, "cache_ttl", nil, 60, domain.Revision(1), actor, "bootstrap", now)

	assert.Nil(t, doc.OldValue)
	assert.Equal(t, 60, doc.NewValue)

	docReset := newHistoryDoc(target, "cache_ttl", 60, nil, domain.Revision(2), actor, "api", now)

	assert.Equal(t, 60, docReset.OldValue)
	assert.Nil(t, docReset.NewValue)
}

func TestRevisionMetaKey(t *testing.T) {
	t.Parallel()

	// Verify the sentinel key is stable and non-empty. Changing this value
	// would break existing databases.
	assert.Equal(t, "__revision_meta__", revisionMetaKey)
	assert.NotEmpty(t, revisionMetaKey)
}
