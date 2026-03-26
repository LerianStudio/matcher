//go:build unit

package bootstrap

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	spdomain "github.com/LerianStudio/lib-commons/v4/commons/systemplane/domain"
	spports "github.com/LerianStudio/lib-commons/v4/commons/systemplane/ports"
	spservice "github.com/LerianStudio/lib-commons/v4/commons/systemplane/service"
)

func TestAliasAwareSystemplaneManager_GetConfigsAddsLegacyAliases(t *testing.T) {
	t.Parallel()

	manager := newAliasAwareSystemplaneManager(&mockManagerForMount{
		getConfigsFn: func(context.Context) (spservice.ResolvedSet, error) {
			return spservice.ResolvedSet{
				Values: map[string]spdomain.EffectiveValue{
					"cors.allowed_origins": {Key: "cors.allowed_origins", Value: "https://app.example.com"},
				},
			}, nil
		},
	})

	resolved, err := manager.GetConfigs(context.Background())
	require.NoError(t, err)
	assert.Contains(t, resolved.Values, "cors.allowed_origins")
	assert.Contains(t, resolved.Values, "server.cors_allowed_origins")
	assert.Equal(t, "https://app.example.com", resolved.Values["server.cors_allowed_origins"].Value)
}

func TestAliasAwareSystemplaneManager_PatchConfigsKeepsLegacyStoreKeyUntilMigrated(t *testing.T) {
	t.Parallel()

	var capturedReq spservice.PatchRequest
	manager := newAliasAwareSystemplaneManager(&mockManagerForMount{
		getConfigsFn: func(context.Context) (spservice.ResolvedSet, error) {
			return spservice.ResolvedSet{
				Values: map[string]spdomain.EffectiveValue{
					"server.cors_allowed_origins": {Key: "server.cors_allowed_origins", Value: "https://legacy.example.com"},
				},
			}, nil
		},
		patchConfigsFn: func(_ context.Context, req spservice.PatchRequest) (spservice.WriteResult, error) {
			capturedReq = req
			return spservice.WriteResult{Revision: 2}, nil
		},
	})

	_, err := manager.PatchConfigs(context.Background(), spservice.PatchRequest{
		Ops: []spports.WriteOp{{Key: "cors.allowed_origins", Value: "https://new.example.com"}},
	})
	require.NoError(t, err)
	require.Len(t, capturedReq.Ops, 1)
	assert.Equal(t, "server.cors_allowed_origins", capturedReq.Ops[0].Key)
}

func TestAliasAwareSystemplaneManager_GetConfigSchemaAddsLegacyAliases(t *testing.T) {
	t.Parallel()

	manager := newAliasAwareSystemplaneManager(&mockManagerForMount{
		getConfigSchemaFn: func(context.Context) ([]spservice.SchemaEntry, error) {
			return []spservice.SchemaEntry{{Key: "cors.allowed_origins", Group: "cors"}}, nil
		},
	})

	entries, err := manager.GetConfigSchema(context.Background())
	require.NoError(t, err)
	assert.Contains(t, entries, spservice.SchemaEntry{Key: "server.cors_allowed_origins", Group: "cors"})
}

func TestAliasAwareSystemplaneManager_GetConfigHistorySupportsLegacyFilter(t *testing.T) {
	t.Parallel()

	manager := newAliasAwareSystemplaneManager(&mockManagerForMount{
		getHistoryFn: func(_ context.Context, filter spports.HistoryFilter) ([]spports.HistoryEntry, error) {
			assert.Equal(t, "cors.allowed_origins", filter.Key)
			return []spports.HistoryEntry{{Key: "cors.allowed_origins", ChangedAt: time.Now().UTC()}}, nil
		},
	})

	history, err := manager.GetConfigHistory(context.Background(), spports.HistoryFilter{Key: "server.cors_allowed_origins"})
	require.NoError(t, err)
	require.Len(t, history, 1)
	assert.Equal(t, "server.cors_allowed_origins", history[0].Key)
}
