//go:build unit

// Copyright 2025 Lerian Studio.

package testutil

import (
	"context"
	"testing"
	"time"

	"github.com/LerianStudio/matcher/pkg/systemplane/domain"
	"github.com/LerianStudio/matcher/pkg/systemplane/ports"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFakeHistoryStore_ListHistory_FiltersByExplicitKind(t *testing.T) {
	t.Parallel()

	store := NewFakeHistoryStore()
	now := time.Now().UTC()

	store.AppendForKind(domain.KindConfig, ports.HistoryEntry{
		Revision:  1,
		Key:       "config.key",
		Scope:     domain.ScopeGlobal,
		ActorID:   "admin",
		ChangedAt: now,
	})
	store.AppendForKind(domain.KindSetting, ports.HistoryEntry{
		Revision:  2,
		Key:       "setting.key",
		Scope:     domain.ScopeGlobal,
		ActorID:   "admin",
		ChangedAt: now.Add(time.Second),
	})

	configHistory, err := store.ListHistory(context.Background(), ports.HistoryFilter{Kind: domain.KindConfig})
	require.NoError(t, err)
	require.Len(t, configHistory, 1)
	assert.Equal(t, "config.key", configHistory[0].Key)

	settingHistory, err := store.ListHistory(context.Background(), ports.HistoryFilter{Kind: domain.KindSetting, Scope: domain.ScopeGlobal})
	require.NoError(t, err)
	require.Len(t, settingHistory, 1)
	assert.Equal(t, "setting.key", settingHistory[0].Key)
}

func TestFakeHistoryStore_Append_InferHistoryKind(t *testing.T) {
	t.Parallel()

	store := NewFakeHistoryStore()
	now := time.Now().UTC()

	store.Append(ports.HistoryEntry{
		Revision:  1,
		Key:       "tenant.setting",
		Scope:     domain.ScopeTenant,
		SubjectID: "tenant-1",
		ActorID:   "admin",
		ChangedAt: now,
	})

	entries, err := store.ListHistory(context.Background(), ports.HistoryFilter{Kind: domain.KindSetting})
	require.NoError(t, err)
	require.Len(t, entries, 1)
	assert.Equal(t, "tenant.setting", entries[0].Key)
}
