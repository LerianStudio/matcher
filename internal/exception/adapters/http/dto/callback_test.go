// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package dto

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestProcessCallbackRequest_JSONRoundTrip(t *testing.T) {
	t.Parallel()

	dueAt := "2025-06-15T10:00:00Z"
	updatedAt := "2025-06-15T09:30:00Z"

	req := ProcessCallbackRequest{
		CallbackType:    "status_update",
		ExternalSystem:  "JIRA",
		ExternalIssueID: "RECON-1234",
		Status:          "ASSIGNED",
		ResolutionNotes: "assigned to ops team",
		Assignee:        "analyst@example.com",
		DueAt:           &dueAt,
		UpdatedAt:       &updatedAt,
		Payload: map[string]any{
			"priority": "high",
		},
	}

	data, err := json.Marshal(req)
	require.NoError(t, err)

	var decoded ProcessCallbackRequest
	require.NoError(t, json.Unmarshal(data, &decoded))

	assert.Equal(t, req.CallbackType, decoded.CallbackType)
	assert.Equal(t, req.ExternalSystem, decoded.ExternalSystem)
	assert.Equal(t, req.ExternalIssueID, decoded.ExternalIssueID)
	assert.Equal(t, req.Status, decoded.Status)
	assert.Equal(t, req.ResolutionNotes, decoded.ResolutionNotes)
	assert.Equal(t, req.Assignee, decoded.Assignee)
	require.NotNil(t, decoded.DueAt)
	assert.Equal(t, dueAt, *decoded.DueAt)
	require.NotNil(t, decoded.UpdatedAt)
	assert.Equal(t, updatedAt, *decoded.UpdatedAt)
	assert.Equal(t, "high", decoded.Payload["priority"])
}

func TestProcessCallbackRequest_MinimalJSON(t *testing.T) {
	t.Parallel()

	jsonStr := `{
		"externalSystem": "WEBHOOK",
		"externalIssueId": "EXT-001",
		"status": "RESOLVED"
	}`

	var req ProcessCallbackRequest
	require.NoError(t, json.Unmarshal([]byte(jsonStr), &req))

	assert.Equal(t, "WEBHOOK", req.ExternalSystem)
	assert.Equal(t, "EXT-001", req.ExternalIssueID)
	assert.Equal(t, "RESOLVED", req.Status)
	assert.Empty(t, req.CallbackType)
	assert.Empty(t, req.ResolutionNotes)
	assert.Empty(t, req.Assignee)
	assert.Nil(t, req.DueAt)
	assert.Nil(t, req.UpdatedAt)
	assert.Nil(t, req.Payload)
}

func TestProcessCallbackResponse_JSONRoundTrip(t *testing.T) {
	t.Parallel()

	resp := ProcessCallbackResponse{Status: "accepted"}

	data, err := json.Marshal(resp)
	require.NoError(t, err)

	var decoded ProcessCallbackResponse
	require.NoError(t, json.Unmarshal(data, &decoded))

	assert.Equal(t, "accepted", decoded.Status)
}
