//go:build unit

//nolint:varnamelen,wsl_v5 // Configuration fee-rule client tests use compact handler fixtures.
package client

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConfigurationClient_CreateFeeRule(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/v1/config/contexts/ctx-abc/fee-rules", r.URL.Path)
		assert.Equal(t, http.MethodPost, r.Method)

		var req CreateFeeRuleRequest
		err := json.NewDecoder(r.Body).Decode(&req)
		require.NoError(t, err)
		assert.Equal(t, "Wire Transfer Fee", req.Name)
		assert.Equal(t, "LEFT", req.Side)
		assert.Equal(t, "sched-001", req.FeeScheduleID)
		assert.Equal(t, 1, req.Priority)
		assert.Len(t, req.Predicates, 1)
		assert.Equal(t, "currency", req.Predicates[0].Field)

		w.WriteHeader(http.StatusCreated)
		resp := FeeRuleResponse{
			ID:            "rule-xyz",
			ContextID:     "ctx-abc",
			Side:          req.Side,
			FeeScheduleID: req.FeeScheduleID,
			Name:          req.Name,
			Priority:      req.Priority,
		}
		json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	client := NewConfigurationClient(NewClient(server.URL, "tenant-1", 5*time.Second))
	result, err := client.CreateFeeRule(context.Background(), "ctx-abc", CreateFeeRuleRequest{
		Side:          "LEFT",
		FeeScheduleID: "sched-001",
		Name:          "Wire Transfer Fee",
		Priority:      1,
		Predicates: []CreateFeeRulePredicateRequest{
			{Field: "currency", Operator: "EQUALS", Value: "USD"},
		},
	})

	require.NoError(t, err)
	assert.Equal(t, "rule-xyz", result.ID)
	assert.Equal(t, "ctx-abc", result.ContextID)
	assert.Equal(t, "LEFT", result.Side)
	assert.Equal(t, "Wire Transfer Fee", result.Name)
}

func TestConfigurationClient_GetFeeRule(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/v1/config/fee-rules/rule-xyz", r.URL.Path)
		assert.Equal(t, http.MethodGet, r.Method)

		resp := FeeRuleResponse{
			ID:            "rule-xyz",
			ContextID:     "ctx-abc",
			Side:          "RIGHT",
			FeeScheduleID: "sched-002",
			Name:          "ACH Fee",
			Priority:      2,
		}
		json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	client := NewConfigurationClient(NewClient(server.URL, "tenant-1", 5*time.Second))
	result, err := client.GetFeeRule(context.Background(), "rule-xyz")

	require.NoError(t, err)
	assert.Equal(t, "rule-xyz", result.ID)
	assert.Equal(t, "RIGHT", result.Side)
	assert.Equal(t, "ACH Fee", result.Name)
}

func TestConfigurationClient_ListFeeRules(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/v1/config/contexts/ctx-abc/fee-rules", r.URL.Path)
		assert.Equal(t, http.MethodGet, r.Method)

		resp := []FeeRuleResponse{
			{ID: "rule-1", Priority: 1, Name: "Rule A"},
			{ID: "rule-2", Priority: 2, Name: "Rule B"},
		}
		json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	client := NewConfigurationClient(NewClient(server.URL, "tenant-1", 5*time.Second))
	result, err := client.ListFeeRules(context.Background(), "ctx-abc")

	require.NoError(t, err)
	assert.Len(t, result, 2)
	assert.Equal(t, "rule-1", result[0].ID)
	assert.Equal(t, "rule-2", result[1].ID)
}

func TestConfigurationClient_UpdateFeeRule(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/v1/config/fee-rules/rule-xyz", r.URL.Path)
		assert.Equal(t, http.MethodPatch, r.Method)

		var req UpdateFeeRuleRequest
		err := json.NewDecoder(r.Body).Decode(&req)
		require.NoError(t, err)
		assert.NotNil(t, req.Name)
		assert.Equal(t, "Updated Rule", *req.Name)

		resp := FeeRuleResponse{
			ID:       "rule-xyz",
			Name:     *req.Name,
			Priority: 1,
		}
		json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	client := NewConfigurationClient(NewClient(server.URL, "tenant-1", 5*time.Second))
	name := "Updated Rule"
	result, err := client.UpdateFeeRule(context.Background(), "rule-xyz", UpdateFeeRuleRequest{
		Name: &name,
	})

	require.NoError(t, err)
	assert.Equal(t, "Updated Rule", result.Name)
}

func TestConfigurationClient_DeleteFeeRule(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/v1/config/fee-rules/rule-xyz", r.URL.Path)
		assert.Equal(t, http.MethodDelete, r.Method)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	client := NewConfigurationClient(NewClient(server.URL, "tenant-1", 5*time.Second))
	err := client.DeleteFeeRule(context.Background(), "rule-xyz")

	require.NoError(t, err)
}

func TestConfigurationClient_CreateFeeRule_ServerError(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusConflict)
		w.Write([]byte(`{"code":"MTCH-0107","title":"Conflict","message":"duplicate priority"}`))
	}))
	defer server.Close()

	client := NewConfigurationClient(NewClient(server.URL, "tenant-1", 5*time.Second))
	_, err := client.CreateFeeRule(context.Background(), "ctx-abc", CreateFeeRuleRequest{
		Name:     "Dupe Rule",
		Priority: 1,
	})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "create fee rule")
	assert.Contains(t, err.Error(), "409")
}

func TestConfigurationClient_GetFeeRule_NotFound(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte(`{"code":"MTCH-0114","title":"Not Found","message":"fee rule not found"}`))
	}))
	defer server.Close()

	client := NewConfigurationClient(NewClient(server.URL, "tenant-1", 5*time.Second))
	_, err := client.GetFeeRule(context.Background(), "nonexistent")

	require.Error(t, err)
	assert.Contains(t, err.Error(), "get fee rule")
	assert.Contains(t, err.Error(), "404")
}

func TestConfigurationClient_DeleteFeeRule_ServerError(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(`{"code":"MTCH-0002","title":"Internal Server Error","message":"internal error"}`))
	}))
	defer server.Close()

	client := NewConfigurationClient(NewClient(server.URL, "tenant-1", 5*time.Second))
	err := client.DeleteFeeRule(context.Background(), "rule-xyz")

	require.Error(t, err)
	assert.Contains(t, err.Error(), "delete fee rule")
	assert.Contains(t, err.Error(), "500")
}
