//go:build e2e

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

func TestNewFeeScheduleClient(t *testing.T) {
	t.Parallel()

	base := NewClient("http://localhost:4018", "tenant-1", 0)
	fc := NewFeeScheduleClient(base)
	assert.NotNil(t, fc, "fee schedule client should not be nil")
}

func TestFeeScheduleClient_CreateFeeSchedule(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/v1/fee-schedules", r.URL.Path)
		assert.Equal(t, http.MethodPost, r.Method)

		var req CreateFeeScheduleRequest
		err := json.NewDecoder(r.Body).Decode(&req)
		require.NoError(t, err)
		assert.Equal(t, "Standard", req.Name)

		resp := FeeScheduleResponse{ID: "fs-123", Name: req.Name}
		require.NoError(t, json.NewEncoder(w).Encode(resp))
	}))
	defer server.Close()

	client := NewFeeScheduleClient(NewClient(server.URL, "tenant-1", 5*time.Second))
	result, err := client.CreateFeeSchedule(context.Background(), CreateFeeScheduleRequest{Name: "Standard"})

	require.NoError(t, err)
	assert.Equal(t, "fs-123", result.ID)
	assert.Equal(t, "Standard", result.Name)
}

func TestFeeScheduleClient_ListFeeSchedules(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/v1/fee-schedules", r.URL.Path)
		assert.Equal(t, http.MethodGet, r.Method)

		resp := []FeeScheduleResponse{{ID: "fs-1"}, {ID: "fs-2"}}
		require.NoError(t, json.NewEncoder(w).Encode(resp))
	}))
	defer server.Close()

	client := NewFeeScheduleClient(NewClient(server.URL, "tenant-1", 5*time.Second))
	result, err := client.ListFeeSchedules(context.Background())

	require.NoError(t, err)
	assert.Len(t, result, 2)
}

func TestFeeScheduleClient_GetFeeSchedule(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/v1/fee-schedules/fs-123", r.URL.Path)
		assert.Equal(t, http.MethodGet, r.Method)

		resp := FeeScheduleResponse{ID: "fs-123", Name: "Standard"}
		require.NoError(t, json.NewEncoder(w).Encode(resp))
	}))
	defer server.Close()

	client := NewFeeScheduleClient(NewClient(server.URL, "tenant-1", 5*time.Second))
	result, err := client.GetFeeSchedule(context.Background(), "fs-123")

	require.NoError(t, err)
	assert.Equal(t, "fs-123", result.ID)
}

func TestFeeScheduleClient_UpdateFeeSchedule(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/v1/fee-schedules/fs-123", r.URL.Path)
		assert.Equal(t, http.MethodPatch, r.Method)

		resp := FeeScheduleResponse{ID: "fs-123", Name: "Updated"}
		require.NoError(t, json.NewEncoder(w).Encode(resp))
	}))
	defer server.Close()

	client := NewFeeScheduleClient(NewClient(server.URL, "tenant-1", 5*time.Second))
	name := "Updated"
	result, err := client.UpdateFeeSchedule(context.Background(), "fs-123", UpdateFeeScheduleRequest{Name: &name})

	require.NoError(t, err)
	assert.Equal(t, "Updated", result.Name)
}

func TestFeeScheduleClient_DeleteFeeSchedule(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/v1/fee-schedules/fs-123", r.URL.Path)
		assert.Equal(t, http.MethodDelete, r.Method)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	client := NewFeeScheduleClient(NewClient(server.URL, "tenant-1", 5*time.Second))
	err := client.DeleteFeeSchedule(context.Background(), "fs-123")

	require.NoError(t, err)
}

func TestFeeScheduleClient_SimulateFeeSchedule(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/v1/fee-schedules/fs-123/simulate", r.URL.Path)
		assert.Equal(t, http.MethodPost, r.Method)

		var req SimulateFeeRequest
		err := json.NewDecoder(r.Body).Decode(&req)
		require.NoError(t, err)
		assert.Equal(t, "100.00", req.GrossAmount)

		resp := SimulateFeeResponse{GrossAmount: "100.00", NetAmount: "97.00", TotalFee: "3.00", Currency: "USD"}
		require.NoError(t, json.NewEncoder(w).Encode(resp))
	}))
	defer server.Close()

	client := NewFeeScheduleClient(NewClient(server.URL, "tenant-1", 5*time.Second))
	result, err := client.SimulateFeeSchedule(context.Background(), "fs-123", SimulateFeeRequest{GrossAmount: "100.00", Currency: "USD"})

	require.NoError(t, err)
	assert.Equal(t, "97.00", result.NetAmount)
}
