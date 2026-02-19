//go:build e2e

package client

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewConfigurationClient(t *testing.T) {
	t.Parallel()

	baseClient := NewClient("http://localhost:4018", "tenant-123", 30*time.Second)
	configClient := NewConfigurationClient(baseClient)

	assert.NotNil(t, configClient)
	assert.Equal(t, baseClient, configClient.client)
}

func TestConfigurationClient_CreateContext(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/v1/config/contexts", r.URL.Path)
		assert.Equal(t, http.MethodPost, r.Method)

		var req CreateContextRequest
		err := json.NewDecoder(r.Body).Decode(&req)
		require.NoError(t, err)
		assert.Equal(t, "Test Context", req.Name)

		resp := Context{
			ID:       "ctx-123",
			Name:     req.Name,
			Type:     req.Type,
			Interval: req.Interval,
		}
		json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	client := NewConfigurationClient(NewClient(server.URL, "tenant-123", 5*time.Second))
	result, err := client.CreateContext(context.Background(), CreateContextRequest{
		Name:     "Test Context",
		Type:     "BANK_RECONCILIATION",
		Interval: "DAILY",
	})

	require.NoError(t, err)
	assert.Equal(t, "ctx-123", result.ID)
	assert.Equal(t, "Test Context", result.Name)
}

func TestConfigurationClient_GetContext(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/v1/config/contexts/ctx-123", r.URL.Path)

		resp := Context{ID: "ctx-123", Name: "My Context"}
		json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	client := NewConfigurationClient(NewClient(server.URL, "tenant-123", 5*time.Second))
	result, err := client.GetContext(context.Background(), "ctx-123")

	require.NoError(t, err)
	assert.Equal(t, "ctx-123", result.ID)
}

func TestConfigurationClient_ListContexts(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		resp := struct {
			Items []Context `json:"items"`
		}{
			Items: []Context{
				{ID: "ctx-1", Name: "Context 1"},
				{ID: "ctx-2", Name: "Context 2"},
			},
		}
		json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	client := NewConfigurationClient(NewClient(server.URL, "tenant-123", 5*time.Second))
	result, err := client.ListContexts(context.Background())

	require.NoError(t, err)
	assert.Len(t, result, 2)
}

func TestConfigurationClient_UpdateContext(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodPatch, r.Method)

		var req UpdateContextRequest
		err := json.NewDecoder(r.Body).Decode(&req)
		require.NoError(t, err)
		assert.NotNil(t, req.Name)

		resp := Context{ID: "ctx-123", Name: *req.Name}
		json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	client := NewConfigurationClient(NewClient(server.URL, "tenant-123", 5*time.Second))
	name := "Updated Name"
	result, err := client.UpdateContext(context.Background(), "ctx-123", UpdateContextRequest{
		Name: &name,
	})

	require.NoError(t, err)
	assert.Equal(t, "Updated Name", result.Name)
}

func TestConfigurationClient_DeleteContext(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodDelete, r.Method)
		assert.Equal(t, "/v1/config/contexts/ctx-123", r.URL.Path)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	client := NewConfigurationClient(NewClient(server.URL, "tenant-123", 5*time.Second))
	err := client.DeleteContext(context.Background(), "ctx-123")

	require.NoError(t, err)
}

func TestConfigurationClient_CreateSource(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/v1/config/contexts/ctx-123/sources", r.URL.Path)
		assert.Equal(t, http.MethodPost, r.Method)

		resp := Source{ID: "src-123", Name: "Bank Source"}
		json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	client := NewConfigurationClient(NewClient(server.URL, "tenant-123", 5*time.Second))
	result, err := client.CreateSource(context.Background(), "ctx-123", CreateSourceRequest{
		Name: "Bank Source",
		Type: "CSV",
	})

	require.NoError(t, err)
	assert.Equal(t, "src-123", result.ID)
}

func TestConfigurationClient_GetSource(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/v1/config/contexts/ctx-123/sources/src-456", r.URL.Path)

		resp := Source{ID: "src-456", Name: "My Source"}
		json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	client := NewConfigurationClient(NewClient(server.URL, "tenant-123", 5*time.Second))
	result, err := client.GetSource(context.Background(), "ctx-123", "src-456")

	require.NoError(t, err)
	assert.Equal(t, "src-456", result.ID)
}

func TestConfigurationClient_ListSources(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		resp := struct {
			Items []Source `json:"items"`
		}{
			Items: []Source{
				{ID: "src-1"},
				{ID: "src-2"},
			},
		}
		json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	client := NewConfigurationClient(NewClient(server.URL, "tenant-123", 5*time.Second))
	result, err := client.ListSources(context.Background(), "ctx-123")

	require.NoError(t, err)
	assert.Len(t, result, 2)
}

func TestConfigurationClient_UpdateSource(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodPatch, r.Method)

		resp := Source{ID: "src-123", Name: "Updated Source"}
		json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	client := NewConfigurationClient(NewClient(server.URL, "tenant-123", 5*time.Second))
	name := "Updated Source"
	result, err := client.UpdateSource(
		context.Background(),
		"ctx-123",
		"src-123",
		UpdateSourceRequest{
			Name: &name,
		},
	)

	require.NoError(t, err)
	assert.Equal(t, "Updated Source", result.Name)
}

func TestConfigurationClient_DeleteSource(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodDelete, r.Method)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	client := NewConfigurationClient(NewClient(server.URL, "tenant-123", 5*time.Second))
	err := client.DeleteSource(context.Background(), "ctx-123", "src-123")

	require.NoError(t, err)
}

func TestConfigurationClient_CreateFieldMap(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/v1/config/contexts/ctx-123/sources/src-456/field-maps", r.URL.Path)

		resp := FieldMap{ID: "fm-123"}
		json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	client := NewConfigurationClient(NewClient(server.URL, "tenant-123", 5*time.Second))
	result, err := client.CreateFieldMap(
		context.Background(),
		"ctx-123",
		"src-456",
		CreateFieldMapRequest{
			Mapping: map[string]any{"amount": "col_a"},
		},
	)

	require.NoError(t, err)
	assert.Equal(t, "fm-123", result.ID)
}

func TestConfigurationClient_GetFieldMapBySource(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		resp := FieldMap{
			ID:      "fm-123",
			Mapping: map[string]any{"amount": "col_a"},
		}
		json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	client := NewConfigurationClient(NewClient(server.URL, "tenant-123", 5*time.Second))
	result, err := client.GetFieldMapBySource(context.Background(), "ctx-123", "src-456")

	require.NoError(t, err)
	assert.Equal(t, "fm-123", result.ID)
}

func TestConfigurationClient_UpdateFieldMap(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/v1/config/field-maps/fm-123", r.URL.Path)
		assert.Equal(t, http.MethodPatch, r.Method)

		resp := FieldMap{ID: "fm-123"}
		json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	client := NewConfigurationClient(NewClient(server.URL, "tenant-123", 5*time.Second))
	result, err := client.UpdateFieldMap(context.Background(), "fm-123", UpdateFieldMapRequest{
		Mapping: map[string]any{"amount": "col_b"},
	})

	require.NoError(t, err)
	assert.Equal(t, "fm-123", result.ID)
}

func TestConfigurationClient_DeleteFieldMap(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/v1/config/field-maps/fm-123", r.URL.Path)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	client := NewConfigurationClient(NewClient(server.URL, "tenant-123", 5*time.Second))
	err := client.DeleteFieldMap(context.Background(), "fm-123")

	require.NoError(t, err)
}

func TestConfigurationClient_CreateMatchRule(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/v1/config/contexts/ctx-123/rules", r.URL.Path)

		resp := MatchRule{ID: "rule-123", Priority: 1}
		json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	client := NewConfigurationClient(NewClient(server.URL, "tenant-123", 5*time.Second))
	result, err := client.CreateMatchRule(context.Background(), "ctx-123", CreateMatchRuleRequest{
		Priority: 1,
		Type:     "EXACT",
	})

	require.NoError(t, err)
	assert.Equal(t, "rule-123", result.ID)
}

func TestConfigurationClient_GetMatchRule(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/v1/config/contexts/ctx-123/rules/rule-456", r.URL.Path)

		resp := MatchRule{ID: "rule-456", Priority: 2}
		json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	client := NewConfigurationClient(NewClient(server.URL, "tenant-123", 5*time.Second))
	result, err := client.GetMatchRule(context.Background(), "ctx-123", "rule-456")

	require.NoError(t, err)
	assert.Equal(t, "rule-456", result.ID)
}

func TestConfigurationClient_ListMatchRules(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		resp := struct {
			Items []MatchRule `json:"items"`
		}{
			Items: []MatchRule{
				{ID: "rule-1", Priority: 1},
				{ID: "rule-2", Priority: 2},
			},
		}
		json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	client := NewConfigurationClient(NewClient(server.URL, "tenant-123", 5*time.Second))
	result, err := client.ListMatchRules(context.Background(), "ctx-123")

	require.NoError(t, err)
	assert.Len(t, result, 2)
}

func TestConfigurationClient_UpdateMatchRule(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodPatch, r.Method)

		resp := MatchRule{ID: "rule-123", Priority: 5}
		json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	client := NewConfigurationClient(NewClient(server.URL, "tenant-123", 5*time.Second))
	priority := 5
	result, err := client.UpdateMatchRule(
		context.Background(),
		"ctx-123",
		"rule-123",
		UpdateMatchRuleRequest{
			Priority: &priority,
		},
	)

	require.NoError(t, err)
	assert.Equal(t, 5, result.Priority)
}

func TestConfigurationClient_DeleteMatchRule(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodDelete, r.Method)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	client := NewConfigurationClient(NewClient(server.URL, "tenant-123", 5*time.Second))
	err := client.DeleteMatchRule(context.Background(), "ctx-123", "rule-123")

	require.NoError(t, err)
}

func TestConfigurationClient_ReorderMatchRules(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/v1/config/contexts/ctx-123/rules/reorder", r.URL.Path)
		assert.Equal(t, http.MethodPost, r.Method)

		var req ReorderMatchRulesRequest
		err := json.NewDecoder(r.Body).Decode(&req)
		require.NoError(t, err)
		assert.Equal(t, []string{"rule-2", "rule-1", "rule-3"}, req.RuleIDs)

		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client := NewConfigurationClient(NewClient(server.URL, "tenant-123", 5*time.Second))
	err := client.ReorderMatchRules(context.Background(), "ctx-123", ReorderMatchRulesRequest{
		RuleIDs: []string{"rule-2", "rule-1", "rule-3"},
	})

	require.NoError(t, err)
}

func TestConfigurationClient_ErrorHandling(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		handler        http.HandlerFunc
		timeout        time.Duration
		expectContains []string
		expectNetErr   bool
	}{
		{
			name: "timeout",
			handler: func(w http.ResponseWriter, r *http.Request) {
				time.Sleep(50 * time.Millisecond)
				w.WriteHeader(http.StatusOK)
			},
			timeout:        10 * time.Millisecond,
			expectContains: []string{"create context", "Client.Timeout"},
		},
		{
			name: "server error",
			handler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusInternalServerError)
				w.Write([]byte(`{"error":"server error"}`))
			},
			timeout:        5 * time.Second,
			expectContains: []string{"create context", "API error 500"},
		},
		{
			name: "invalid json",
			handler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
				w.Write([]byte("{invalid-json"))
			},
			timeout:        5 * time.Second,
			expectContains: []string{"create context", "failed to unmarshal response"},
		},
		{
			name: "network failure",
			handler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
			},
			timeout:        5 * time.Second,
			expectContains: []string{"create context"},
			expectNetErr:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(tt.handler)
			if tt.name == "network failure" {
				server.Close()
			} else {
				defer server.Close()
			}

			client := NewConfigurationClient(NewClient(server.URL, "tenant-123", tt.timeout))
			_, err := client.CreateContext(context.Background(), CreateContextRequest{})
			require.Error(t, err)
			if tt.expectNetErr {
				var netErr net.Error
				var urlErr *url.Error
				assert.True(t, errors.As(err, &netErr) || errors.As(err, &urlErr))
			}
			for _, fragment := range tt.expectContains {
				assert.Contains(t, err.Error(), fragment)
			}
		})
	}
}
