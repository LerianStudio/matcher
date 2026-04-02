//go:build unit

//nolint:wsl_v5 // DTO round-trip tests prioritize compact fixtures.
package client

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestContext_JSONSerialization(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC().Truncate(time.Second)
	ctx := Context{
		ID:          "ctx-123",
		TenantID:    "tenant-456",
		Name:        "Test Context",
		Type:        "BANK_RECONCILIATION",
		Interval:    "DAILY",
		Description: "A test context",
		CreatedAt:   now,
		UpdatedAt:   now,
	}

	data, err := json.Marshal(ctx)
	require.NoError(t, err)

	var decoded Context
	err = json.Unmarshal(data, &decoded)
	require.NoError(t, err)

	assert.Equal(t, ctx.ID, decoded.ID)
	assert.Equal(t, ctx.TenantID, decoded.TenantID)
	assert.Equal(t, ctx.Name, decoded.Name)
	assert.Equal(t, ctx.Type, decoded.Type)
	assert.Equal(t, ctx.Interval, decoded.Interval)
	assert.Equal(t, ctx.Description, decoded.Description)
	assert.True(t, ctx.CreatedAt.Equal(decoded.CreatedAt))
	assert.True(t, ctx.UpdatedAt.Equal(decoded.UpdatedAt))
}

func TestCreateContextRequest_JSONSerialization(t *testing.T) {
	t.Parallel()

	req := CreateContextRequest{
		Name:        "New Context",
		Type:        "CARD_RECONCILIATION",
		Interval:    "WEEKLY",
		Description: "Description",
	}

	data, err := json.Marshal(req)
	require.NoError(t, err)

	var decoded CreateContextRequest
	err = json.Unmarshal(data, &decoded)
	require.NoError(t, err)

	assert.Equal(t, req, decoded)
}

func TestUpdateContextRequest_PartialUpdate(t *testing.T) {
	t.Parallel()

	name := "Updated Name"
	req := UpdateContextRequest{
		Name: &name,
	}

	data, err := json.Marshal(req)
	require.NoError(t, err)

	assert.Contains(t, string(data), "name")
	assert.NotContains(t, string(data), "interval")
	assert.NotContains(t, string(data), "description")
}

func TestSource_JSONSerialization(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC().Truncate(time.Second)
	source := Source{
		ID:        "src-123",
		ContextID: "ctx-456",
		Name:      "Bank Source",
		Type:      "CSV",
		Config:    map[string]any{"delimiter": ","},
		CreatedAt: now,
		UpdatedAt: now,
	}

	data, err := json.Marshal(source)
	require.NoError(t, err)

	var decoded Source
	err = json.Unmarshal(data, &decoded)
	require.NoError(t, err)

	assert.Equal(t, source.ID, decoded.ID)
	assert.Equal(t, source.Name, decoded.Name)
	assert.Equal(t, ",", decoded.Config["delimiter"])
}

func TestFieldMap_JSONSerialization(t *testing.T) {
	t.Parallel()

	fm := FieldMap{
		ID:        "fm-123",
		ContextID: "ctx-456",
		SourceID:  "src-789",
		Mapping: map[string]any{
			"amount":   "col_a",
			"currency": "col_b",
		},
	}

	data, err := json.Marshal(fm)
	require.NoError(t, err)

	var decoded FieldMap
	err = json.Unmarshal(data, &decoded)
	require.NoError(t, err)

	assert.Equal(t, fm.ID, decoded.ID)
	assert.Equal(t, "col_a", decoded.Mapping["amount"])
}

func TestMatchRule_JSONSerialization(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC().Truncate(time.Second)
	rule := MatchRule{
		ID:        "rule-123",
		ContextID: "ctx-456",
		Priority:  1,
		Type:      "EXACT",
		Config:    map[string]any{"fields": []string{"amount", "date"}},
		CreatedAt: now,
		UpdatedAt: now,
	}

	data, err := json.Marshal(rule)
	require.NoError(t, err)

	var decoded MatchRule
	err = json.Unmarshal(data, &decoded)
	require.NoError(t, err)

	assert.Equal(t, rule.ID, decoded.ID)
	assert.Equal(t, 1, decoded.Priority)
}

func TestIngestionJob_AllStatuses(t *testing.T) {
	t.Parallel()

	statuses := []string{"PENDING", "PROCESSING", "COMPLETED", "FAILED"}

	for _, status := range statuses {
		job := IngestionJob{
			ID:     "job-123",
			Status: status,
		}

		data, err := json.Marshal(job)
		require.NoError(t, err)

		var decoded IngestionJob
		err = json.Unmarshal(data, &decoded)
		require.NoError(t, err)

		assert.Equal(t, "job-123", decoded.ID)
		assert.Equal(t, status, decoded.Status)
	}
}

func TestTransaction_JSONSerialization(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC().Truncate(time.Second)
	tx := Transaction{
		ID:               "tx-123",
		JobID:            "job-456",
		SourceID:         "src-789",
		ExternalID:       "ext-001",
		Amount:           "1000.50",
		Currency:         "USD",
		Date:             now,
		Description:      "Payment",
		Status:           "PENDING",
		ExtractionStatus: "EXTRACTED",
		Metadata:         map[string]any{"ref": "ABC123"},
		CreatedAt:        now,
		UpdatedAt:        now,
	}

	data, err := json.Marshal(tx)
	require.NoError(t, err)

	var decoded Transaction
	err = json.Unmarshal(data, &decoded)
	require.NoError(t, err)

	assert.Equal(t, tx.ID, decoded.ID)
	assert.Equal(t, tx.Amount, decoded.Amount)
	assert.Equal(t, tx.Currency, decoded.Currency)
}

func TestMatchRun_JSONSerialization(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC().Truncate(time.Second)
	run := MatchRun{
		ID:        "run-123",
		ContextID: "ctx-456",
		Mode:      "COMMIT",
		Status:    "COMPLETED",
		Stats: map[string]int{
			"matched":   100,
			"unmatched": 10,
		},
		StartedAt:   now,
		CompletedAt: now.Add(5 * time.Minute),
		CreatedAt:   now,
		UpdatedAt:   now,
	}

	data, err := json.Marshal(run)
	require.NoError(t, err)

	var decoded MatchRun
	err = json.Unmarshal(data, &decoded)
	require.NoError(t, err)

	assert.Equal(t, "COMMIT", decoded.Mode)
	assert.Equal(t, 100, decoded.Stats["matched"])
}

func TestMatchGroup_WithItems(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC().Truncate(time.Second)
	group := MatchGroup{
		ID:         "grp-123",
		ContextID:  "ctx-456",
		RunID:      "run-789",
		RuleID:     "rule-001",
		Confidence: 0.95,
		Items: []MatchItem{
			{
				ID:            "item-1",
				MatchGroupID:  "grp-123",
				TransactionID: "tx-001",
				Amount:        "100.00",
				Currency:      "USD",
				Contribution:  "50.00",
				CreatedAt:     now,
			},
		},
		CreatedAt: now,
	}

	data, err := json.Marshal(group)
	require.NoError(t, err)

	var decoded MatchGroup
	err = json.Unmarshal(data, &decoded)
	require.NoError(t, err)

	assert.InEpsilon(t, 0.95, decoded.Confidence, 0.0001)
	require.Len(t, decoded.Items, 1)
	assert.Equal(t, "tx-001", decoded.Items[0].TransactionID)
}

func TestDashboardAggregates_JSONSerialization(t *testing.T) {
	t.Parallel()

	agg := DashboardAggregates{
		Volume: &VolumeStatsResponse{
			TotalTransactions:   1000,
			MatchedTransactions: 900,
			UnmatchedCount:      100,
			TotalAmount:         "1000000.00",
			MatchedAmount:       "900000.00",
			UnmatchedAmount:     "100000.00",
		},
		MatchRate: &MatchRateStatsResponse{
			MatchRate:      0.90,
			TotalCount:     1000,
			MatchedCount:   900,
			UnmatchedCount: 100,
		},
		SLA: &SLAStatsResponse{
			TotalExceptions:   10,
			SLAComplianceRate: 0.95,
		},
		UpdatedAt: "2024-01-01T00:00:00Z",
	}

	data, err := json.Marshal(agg)
	require.NoError(t, err)

	var decoded DashboardAggregates
	err = json.Unmarshal(data, &decoded)
	require.NoError(t, err)

	require.NotNil(t, decoded.Volume)
	assert.Equal(t, 1000, decoded.Volume.TotalTransactions)
	require.NotNil(t, decoded.MatchRate)
	assert.InEpsilon(t, 0.90, decoded.MatchRate.MatchRate, 0.0001)
}

func TestExportJob_AllStatuses(t *testing.T) {
	t.Parallel()

	statuses := []string{"QUEUED", "PROCESSING", "COMPLETED", "FAILED", "EXPIRED", "CANCELLED"}

	for _, status := range statuses {
		job := ExportJob{
			ID:     "job-123",
			Status: status,
		}
		assert.Equal(t, "job-123", job.ID)
		assert.Equal(t, status, job.Status)
	}
}

func TestListResponse_Generic(t *testing.T) {
	t.Parallel()

	resp := ListResponse[Context]{
		Items: []Context{
			{ID: "ctx-1", Name: "Context 1"},
			{ID: "ctx-2", Name: "Context 2"},
		},
		NextCursor: "cursor-abc",
		HasMore:    true,
	}

	data, err := json.Marshal(resp)
	require.NoError(t, err)

	var decoded ListResponse[Context]
	err = json.Unmarshal(data, &decoded)
	require.NoError(t, err)

	assert.Len(t, decoded.Items, 2)
	assert.Equal(t, "cursor-abc", decoded.NextCursor)
	assert.True(t, decoded.HasMore)
}

func TestErrorResponse_JSONSerialization(t *testing.T) {
	t.Parallel()

	resp := ErrorResponse{
		Code:    "MTCH-0001",
		Title:   "Bad Request",
		Message: "Field 'name' is required",
		Details: map[string]any{"field": "name"},
	}

	data, err := json.Marshal(resp)
	require.NoError(t, err)

	var decoded ErrorResponse
	err = json.Unmarshal(data, &decoded)
	require.NoError(t, err)

	assert.Equal(t, "MTCH-0001", decoded.Code)
	assert.Equal(t, "Bad Request", decoded.Title)
	assert.Equal(t, "Field 'name' is required", decoded.Message)
	assert.NotNil(t, decoded.Details)
}

func TestErrorResponse_OmitsEmptyOptionalFields(t *testing.T) {
	t.Parallel()

	resp := ErrorResponse{
		Code:    "MTCH-0005",
		Message: "Not found",
	}

	data, err := json.Marshal(resp)
	require.NoError(t, err)

	assert.NotContains(t, string(data), "title")
	assert.NotContains(t, string(data), "error")
	assert.NotContains(t, string(data), "details")
	assert.Contains(t, string(data), `"code":"MTCH-0005"`)
	assert.Contains(t, string(data), `"message":"Not found"`)
}

func TestReorderMatchRulesRequest_JSONSerialization(t *testing.T) {
	t.Parallel()

	req := ReorderMatchRulesRequest{
		RuleIDs: []string{"rule-1", "rule-2", "rule-3"},
	}

	data, err := json.Marshal(req)
	require.NoError(t, err)

	var decoded ReorderMatchRulesRequest
	err = json.Unmarshal(data, &decoded)
	require.NoError(t, err)

	assert.Equal(t, []string{"rule-1", "rule-2", "rule-3"}, decoded.RuleIDs)
}

func TestArchiveMetadata_JSONSerialization(t *testing.T) {
	t.Parallel()

	archive := ArchiveMetadata{
		ID:        "arch-123",
		StartDate: "2026-01-01",
		EndDate:   "2026-01-31",
		Status:    "READY",
		CreatedAt: "2026-02-01T00:00:00Z",
	}

	data, err := json.Marshal(archive)
	require.NoError(t, err)

	var decoded ArchiveMetadata
	err = json.Unmarshal(data, &decoded)
	require.NoError(t, err)

	assert.Equal(t, archive.ID, decoded.ID)
	assert.Equal(t, archive.StartDate, decoded.StartDate)
	assert.Equal(t, archive.EndDate, decoded.EndDate)
	assert.Equal(t, archive.Status, decoded.Status)
}

func TestArchiveDownloadResponse_JSONSerialization(t *testing.T) {
	t.Parallel()

	resp := ArchiveDownloadResponse{
		DownloadURL: "https://storage.example.com/archive.zip",
		ExpiresAt:   "2026-01-31T00:00:00Z",
		Checksum:    "sha256:abc123",
	}

	data, err := json.Marshal(resp)
	require.NoError(t, err)

	var decoded ArchiveDownloadResponse
	err = json.Unmarshal(data, &decoded)
	require.NoError(t, err)

	assert.Equal(t, resp.DownloadURL, decoded.DownloadURL)
	assert.Equal(t, resp.ExpiresAt, decoded.ExpiresAt)
	assert.Equal(t, resp.Checksum, decoded.Checksum)
}

func TestAuditLog_JSONSerialization(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC().Truncate(time.Second)
	log := AuditLog{
		ID:         "log-123",
		EntityType: "CONTEXT",
		EntityID:   "ctx-456",
		Action:     "CREATE",
		ActorID:    "user@example.com",
		Changes:    map[string]any{"name": "New Name"},
		CreatedAt:  now,
	}

	data, err := json.Marshal(log)
	require.NoError(t, err)

	var decoded AuditLog
	err = json.Unmarshal(data, &decoded)
	require.NoError(t, err)

	assert.Equal(t, "CONTEXT", decoded.EntityType)
	assert.Equal(t, "CREATE", decoded.Action)
}
