//go:build unit

package dto

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	sharedhttp "github.com/LerianStudio/lib-commons/v5/commons/net/http"
)

func TestBulkResolveRequest_JSONTags(t *testing.T) {
	t.Parallel()

	original := BulkResolveRequest{
		ExceptionIDs: []string{"550e8400-e29b-41d4-a716-446655440000"},
		Resolution:   "ACCEPTED",
		Reason:       "Duplicate entry",
	}

	data, err := json.Marshal(original)
	require.NoError(t, err)

	var raw map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(data, &raw))

	assert.Contains(t, raw, "exception_ids")
	assert.Contains(t, raw, "resolution")
	assert.Contains(t, raw, "reason")

	var decoded BulkResolveRequest
	require.NoError(t, json.Unmarshal(data, &decoded))

	assert.Equal(t, original, decoded)
}

func TestBulkAssignRequest_JSONTags(t *testing.T) {
	t.Parallel()

	original := BulkAssignRequest{
		ExceptionIDs: []string{"550e8400-e29b-41d4-a716-446655440000", "660e8400-e29b-41d4-a716-446655440001"},
		Assignee:     "analyst-1",
	}

	data, err := json.Marshal(original)
	require.NoError(t, err)

	var raw map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(data, &raw))

	assert.Contains(t, raw, "exception_ids")
	assert.Contains(t, raw, "assignee")

	var decoded BulkAssignRequest
	require.NoError(t, json.Unmarshal(data, &decoded))

	assert.Equal(t, original, decoded)
}

func TestBulkDispatchRequest_JSONTags(t *testing.T) {
	t.Parallel()

	original := BulkDispatchRequest{
		ExceptionIDs: []string{"550e8400-e29b-41d4-a716-446655440000"},
		TargetSystem: "jira",
		Queue:        "finance-team",
	}

	data, err := json.Marshal(original)
	require.NoError(t, err)

	var raw map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(data, &raw))

	assert.Contains(t, raw, "exception_ids")
	assert.Contains(t, raw, "target_system")
	assert.Contains(t, raw, "queue")

	var decoded BulkDispatchRequest
	require.NoError(t, json.Unmarshal(data, &decoded))

	assert.Equal(t, original, decoded)
}

func TestBulkActionResponse_JSONTags(t *testing.T) {
	t.Parallel()

	original := BulkActionResponse{
		Succeeded: []string{"id-1", "id-2"},
		Failed: []BulkFailure{
			{ExceptionID: "id-3", Error: "not found"},
		},
		Total: 3,
	}

	data, err := json.Marshal(original)
	require.NoError(t, err)

	var raw map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(data, &raw))

	assert.Contains(t, raw, "succeeded")
	assert.Contains(t, raw, "failed")
	assert.Contains(t, raw, "total")

	var decoded BulkActionResponse
	require.NoError(t, json.Unmarshal(data, &decoded))

	assert.Equal(t, original, decoded)
}

func TestBulkFailure_JSONTags(t *testing.T) {
	t.Parallel()

	original := BulkFailure{
		ExceptionID: "550e8400-e29b-41d4-a716-446655440000",
		Error:       "exception already resolved",
	}

	data, err := json.Marshal(original)
	require.NoError(t, err)

	var raw map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(data, &raw))

	assert.Contains(t, raw, "exception_id")
	assert.Contains(t, raw, "error")

	var decoded BulkFailure
	require.NoError(t, json.Unmarshal(data, &decoded))

	assert.Equal(t, original, decoded)
}

func TestBulkResolveRequest_EmptyExceptionIDs(t *testing.T) {
	t.Parallel()

	req := BulkResolveRequest{
		ExceptionIDs: []string{},
		Resolution:   "ACCEPTED",
	}

	err := sharedhttp.ValidateStruct(req)
	assert.Error(t, err, "empty exception_ids should fail validation (min=1)")
}

func TestBulkResolveRequest_TooManyExceptionIDs(t *testing.T) {
	t.Parallel()

	ids := make([]string, 101)
	for i := range ids {
		ids[i] = "550e8400-e29b-41d4-a716-44665544" + fmt.Sprintf("%04d", i)
	}

	req := BulkResolveRequest{
		ExceptionIDs: ids,
		Resolution:   "ACCEPTED",
	}

	err := sharedhttp.ValidateStruct(req)
	assert.Error(t, err, "101 exception_ids should fail validation (max=100)")
}

func TestBulkResolveRequest_MissingResolution(t *testing.T) {
	t.Parallel()

	req := BulkResolveRequest{
		ExceptionIDs: []string{"550e8400-e29b-41d4-a716-446655440000"},
		Resolution:   "",
	}

	err := sharedhttp.ValidateStruct(req)
	assert.Error(t, err, "empty resolution should fail validation (required)")
}

func TestBulkResolveRequest_InvalidUUID(t *testing.T) {
	t.Parallel()

	req := BulkResolveRequest{
		ExceptionIDs: []string{"not-a-uuid"},
		Resolution:   "ACCEPTED",
	}

	err := sharedhttp.ValidateStruct(req)
	assert.Error(t, err, "non-uuid exception_id should fail validation (dive,uuid)")
}

func TestBulkResolveRequest_ValidRequest(t *testing.T) {
	t.Parallel()

	req := BulkResolveRequest{
		ExceptionIDs: []string{"550e8400-e29b-41d4-a716-446655440000"},
		Resolution:   "ACCEPTED",
		Reason:       "Verified manually",
	}

	err := sharedhttp.ValidateStruct(req)
	assert.NoError(t, err, "valid request should pass validation")
}
