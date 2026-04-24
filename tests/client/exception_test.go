//go:build unit

//nolint:varnamelen,wsl_v5 // Exception client tests use compact handler fixtures.
package client

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewExceptionClient(t *testing.T) {
	t.Parallel()

	baseClient := NewClient("http://localhost:4018", "tenant-123", 30*time.Second)
	exceptionClient := NewExceptionClient(baseClient)

	assert.NotNil(t, exceptionClient)
	assert.Equal(t, baseClient, exceptionClient.client)
}

func TestExceptionClient_ForceMatch(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/v1/exceptions/exc-123/force-match", r.URL.Path)
		assert.Equal(t, http.MethodPost, r.Method)

		var req ForceMatchRequest
		err := json.NewDecoder(r.Body).Decode(&req)
		require.NoError(t, err)
		assert.Equal(t, "BUSINESS_DECISION", req.OverrideReason)
		assert.Equal(t, "Approved by finance", req.Notes)

		resolutionType := "FORCE_MATCH"
		json.NewEncoder(w).Encode(Exception{
			ID:             "exc-123",
			Status:         "RESOLVED",
			ResolutionType: &resolutionType,
		})
	}))
	defer server.Close()

	client := NewExceptionClient(NewClient(server.URL, "tenant-123", 5*time.Second))
	result, err := client.ForceMatch(context.Background(), "exc-123", ForceMatchRequest{
		OverrideReason: "BUSINESS_DECISION",
		Notes:          "Approved by finance",
	})

	require.NoError(t, err)
	assert.Equal(t, "exc-123", result.ID)
	assert.Equal(t, "RESOLVED", result.Status)
	assert.Equal(t, "FORCE_MATCH", *result.ResolutionType)
}

func TestExceptionClient_AdjustEntry(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC().Truncate(time.Second)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/v1/exceptions/exc-123/adjust-entry", r.URL.Path)

		var req AdjustEntryRequest
		err := json.NewDecoder(r.Body).Decode(&req)
		require.NoError(t, err)
		assert.Equal(t, "FEE_ADJUSTMENT", req.ReasonCode)
		assert.True(t, req.Amount.Equal(decimal.NewFromFloat(150.50)))
		assert.Equal(t, "USD", req.Currency)
		assert.True(t, req.EffectiveAt.Equal(now))

		resolutionType := "ADJUST_ENTRY"
		json.NewEncoder(w).Encode(Exception{
			ID:             "exc-123",
			Status:         "RESOLVED",
			ResolutionType: &resolutionType,
		})
	}))
	defer server.Close()

	client := NewExceptionClient(NewClient(server.URL, "tenant-123", 5*time.Second))
	result, err := client.AdjustEntry(context.Background(), "exc-123", AdjustEntryRequest{
		ReasonCode:  "FEE_ADJUSTMENT",
		Notes:       "Correcting fee",
		Amount:      decimal.NewFromFloat(150.50),
		Currency:    "USD",
		EffectiveAt: now,
	})

	require.NoError(t, err)
	assert.Equal(t, "RESOLVED", result.Status)
}

func TestExceptionClient_GetException(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC().Truncate(time.Second)
	reason := "Amount mismatch"

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/v1/exceptions/exc-123", r.URL.Path)
		assert.Equal(t, http.MethodGet, r.Method)

		json.NewEncoder(w).Encode(Exception{
			ID:            "exc-123",
			TransactionID: "tx-456",
			Severity:      "HIGH",
			Status:        "OPEN",
			Reason:        &reason,
			CreatedAt:     now,
			UpdatedAt:     now,
		})
	}))
	defer server.Close()

	client := NewExceptionClient(NewClient(server.URL, "tenant-123", 5*time.Second))
	result, err := client.GetException(context.Background(), "exc-123")

	require.NoError(t, err)
	assert.Equal(t, "exc-123", result.ID)
	assert.Equal(t, "HIGH", result.Severity)
	assert.Equal(t, "OPEN", result.Status)
	assert.Equal(t, &reason, result.Reason)
}

func TestExceptionClient_ListExceptions_NoFilter(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/v1/exceptions", r.URL.Path)

		json.NewEncoder(w).Encode(ListResponse[Exception]{
			Items: []Exception{
				{ID: "exc-1", Status: "OPEN"},
				{ID: "exc-2", Status: "RESOLVED"},
			},
			HasMore: false,
		})
	}))
	defer server.Close()

	client := NewExceptionClient(NewClient(server.URL, "tenant-123", 5*time.Second))
	result, err := client.ListExceptions(context.Background(), ExceptionListFilter{})

	require.NoError(t, err)
	assert.Len(t, result.Items, 2)
	assert.False(t, result.HasMore)
}

func TestExceptionClient_ListExceptions_WithFilters(t *testing.T) {
	t.Parallel()

	dateFrom := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	dateTo := time.Date(2024, 12, 31, 23, 59, 59, 0, time.UTC)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		query := r.URL.Query()
		assert.Equal(t, "OPEN", query.Get("status"))
		assert.Equal(t, "HIGH", query.Get("severity"))
		assert.Equal(t, "user@example.com", query.Get("assigned_to"))
		assert.Equal(t, "JIRA", query.Get("external_system"))
		assert.NotEmpty(t, query.Get("date_from"))
		assert.NotEmpty(t, query.Get("date_to"))
		assert.Equal(t, "cursor-abc", query.Get("cursor"))
		assert.Equal(t, "50", query.Get("limit"))
		assert.Equal(t, "created_at", query.Get("sort_by"))
		assert.Equal(t, "desc", query.Get("sort_order"))

		json.NewEncoder(w).Encode(ListResponse[Exception]{
			Items:      []Exception{{ID: "exc-1"}},
			NextCursor: "cursor-xyz",
			HasMore:    true,
		})
	}))
	defer server.Close()

	client := NewExceptionClient(NewClient(server.URL, "tenant-123", 5*time.Second))
	result, err := client.ListExceptions(context.Background(), ExceptionListFilter{
		Status:         "OPEN",
		Severity:       "HIGH",
		AssignedTo:     "user@example.com",
		ExternalSystem: "JIRA",
		DateFrom:       &dateFrom,
		DateTo:         &dateTo,
		Cursor:         "cursor-abc",
		Limit:          50,
		SortBy:         "created_at",
		SortOrder:      "desc",
	})

	require.NoError(t, err)
	assert.Len(t, result.Items, 1)
	assert.Equal(t, "cursor-xyz", result.NextCursor)
	assert.True(t, result.HasMore)
}

func TestExceptionClient_ListExceptionsByStatus(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "ASSIGNED", r.URL.Query().Get("status"))
		assert.Equal(t, "100", r.URL.Query().Get("limit"))

		json.NewEncoder(w).Encode(ListResponse[Exception]{
			Items: []Exception{{ID: "exc-1", Status: "ASSIGNED"}},
		})
	}))
	defer server.Close()

	client := NewExceptionClient(NewClient(server.URL, "tenant-123", 5*time.Second))
	result, err := client.ListExceptionsByStatus(context.Background(), "ASSIGNED")

	require.NoError(t, err)
	assert.Len(t, result.Items, 1)
}

func TestExceptionClient_ListOpenExceptions(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "OPEN", r.URL.Query().Get("status"))

		json.NewEncoder(w).Encode(ListResponse[Exception]{
			Items: []Exception{
				{ID: "exc-1", Status: "OPEN"},
				{ID: "exc-2", Status: "OPEN"},
			},
		})
	}))
	defer server.Close()

	client := NewExceptionClient(NewClient(server.URL, "tenant-123", 5*time.Second))
	result, err := client.ListOpenExceptions(context.Background())

	require.NoError(t, err)
	assert.Len(t, result.Items, 2)
}

func TestExceptionClient_GetExceptionHistory(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC().Truncate(time.Second)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/v1/exceptions/exc-123/history", r.URL.Path)
		assert.Equal(t, "cursor-abc", r.URL.Query().Get("cursor"))
		assert.Equal(t, "20", r.URL.Query().Get("limit"))

		json.NewEncoder(w).Encode(ListResponse[ExceptionHistory]{
			Items: []ExceptionHistory{
				{
					ID:          "hist-1",
					ExceptionID: "exc-123",
					Action:      "CREATE",
					ActorID:     "system",
					CreatedAt:   now,
				},
				{
					ID:          "hist-2",
					ExceptionID: "exc-123",
					Action:      "FORCE_MATCH",
					ActorID:     "user@example.com",
					Notes:       "Manual resolution",
					CreatedAt:   now.Add(time.Hour),
				},
			},
			NextCursor: "cursor-xyz",
			HasMore:    true,
		})
	}))
	defer server.Close()

	client := NewExceptionClient(NewClient(server.URL, "tenant-123", 5*time.Second))
	result, err := client.GetExceptionHistory(context.Background(), "exc-123", "cursor-abc", 20)

	require.NoError(t, err)
	assert.Len(t, result.Items, 2)
	assert.Equal(t, "CREATE", result.Items[0].Action)
	assert.Equal(t, "FORCE_MATCH", result.Items[1].Action)
	assert.Equal(t, "cursor-xyz", result.NextCursor)
}

func TestExceptionClient_DispatchToExternal(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC().Truncate(time.Second)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/v1/exceptions/exc-123/dispatch", r.URL.Path)
		assert.Equal(t, http.MethodPost, r.Method)

		var req DispatchRequest
		err := json.NewDecoder(r.Body).Decode(&req)
		require.NoError(t, err)
		assert.Equal(t, "JIRA", req.TargetSystem)
		assert.Equal(t, "RECON-TEAM", req.Queue)

		json.NewEncoder(w).Encode(DispatchResponse{
			ExceptionID:       "exc-123",
			Target:            "JIRA",
			ExternalReference: "RECON-1234",
			Acknowledged:      true,
			DispatchedAt:      now,
		})
	}))
	defer server.Close()

	client := NewExceptionClient(NewClient(server.URL, "tenant-123", 5*time.Second))
	result, err := client.DispatchToExternal(context.Background(), "exc-123", DispatchRequest{
		TargetSystem: "JIRA",
		Queue:        "RECON-TEAM",
	})

	require.NoError(t, err)
	assert.Equal(t, "exc-123", result.ExceptionID)
	assert.Equal(t, "JIRA", result.Target)
	assert.Equal(t, "RECON-1234", result.ExternalReference)
	assert.True(t, result.Acknowledged)
}

func TestExceptionClient_ErrorHandling(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte(`{"code":"MTCH-0501","title":"Not Found","message":"exception not found"}`))
	}))
	defer server.Close()

	client := NewExceptionClient(NewClient(server.URL, "tenant-123", 5*time.Second))

	_, err := client.GetException(context.Background(), "nonexistent")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "get exception")
}

func TestExceptionClient_ProcessCallbackWithOptions(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/v1/exceptions/exc-123/callback", r.URL.Path)
		assert.Equal(t, http.MethodPost, r.Method)
		assert.Equal(t, "callback-key", r.Header.Get("X-Idempotency-Key"))

		var req ProcessCallbackRequest
		err := json.NewDecoder(r.Body).Decode(&req)
		require.NoError(t, err)
		assert.Equal(t, "MANUAL", req.ExternalSystem)

		require.NoError(t, json.NewEncoder(w).Encode(ProcessCallbackResponse{Status: "accepted"}))
	}))
	defer server.Close()

	client := NewExceptionClient(NewClient(server.URL, "tenant-123", 5*time.Second))
	result, err := client.ProcessCallbackWithOptions(context.Background(), "exc-123", ProcessCallbackRequest{
		CallbackType:    "STATUS_UPDATE",
		ExternalSystem:  "MANUAL",
		ExternalIssueID: "MANUAL-1",
		Status:          "RESOLVED",
	}, RequestOptions{IdempotencyKey: "callback-key"})

	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, "accepted", result.Status)
}

func TestExceptionClient_UnprocessableEntity(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnprocessableEntity)
		w.Write([]byte(`{"code":"MTCH-0503","title":"Unprocessable Entity","message":"exception already resolved"}`))
	}))
	defer server.Close()

	client := NewExceptionClient(NewClient(server.URL, "tenant-123", 5*time.Second))

	_, err := client.ForceMatch(context.Background(), "exc-123", ForceMatchRequest{
		OverrideReason: "TEST",
		Notes:          "Test",
	})
	require.Error(t, err)
}

func TestExceptionListFilter_EmptyValues(t *testing.T) {
	t.Parallel()

	filter := ExceptionListFilter{}

	assert.Empty(t, filter.Status)
	assert.Empty(t, filter.Severity)
	assert.Empty(t, filter.AssignedTo)
	assert.Nil(t, filter.DateFrom)
	assert.Nil(t, filter.DateTo)
	assert.Empty(t, filter.Cursor)
	assert.Zero(t, filter.Limit)
}

func TestException_OptionalFields(t *testing.T) {
	t.Parallel()

	now := time.Now()
	exc := Exception{
		ID:            "exc-123",
		TransactionID: "tx-456",
		Severity:      "LOW",
		Status:        "OPEN",
		CreatedAt:     now,
		UpdatedAt:     now,
	}

	// Verify required fields are set
	assert.Equal(t, "exc-123", exc.ID)
	assert.Equal(t, "tx-456", exc.TransactionID)
	assert.Equal(t, "LOW", exc.Severity)
	assert.Equal(t, "OPEN", exc.Status)
	assert.Equal(t, now, exc.CreatedAt)
	assert.Equal(t, now, exc.UpdatedAt)

	// Verify optional fields are nil by default
	assert.Nil(t, exc.Reason)
	assert.Nil(t, exc.ExternalSystem)
	assert.Nil(t, exc.ExternalIssueID)
	assert.Nil(t, exc.AssignedTo)
	assert.Nil(t, exc.DueAt)
	assert.Nil(t, exc.ResolutionNotes)
	assert.Nil(t, exc.ResolutionType)
	assert.Nil(t, exc.ResolutionReason)
}
