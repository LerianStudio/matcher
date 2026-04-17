//go:build unit

package fetcher

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestValidateFetcherResourceID_Match_Success(t *testing.T) {
	t.Parallel()

	err := validateFetcherResourceID("connection", "conn-abc", "conn-abc")

	assert.NoError(t, err)
}

func TestValidateFetcherResourceID_Mismatch_ReturnsError(t *testing.T) {
	t.Parallel()

	err := validateFetcherResourceID("connection", "conn-abc", "conn-other")

	require.Error(t, err)
	assert.ErrorIs(t, err, ErrFetcherBadResponse)
	assert.Contains(t, err.Error(), "connection id mismatch")
	assert.Contains(t, err.Error(), "conn-abc")
	assert.Contains(t, err.Error(), "conn-other")
}

func TestValidateFetcherResourceID_EmptyExpected_ReturnsError(t *testing.T) {
	t.Parallel()

	err := validateFetcherResourceID("job", "", "job-1")

	require.Error(t, err)
	assert.ErrorIs(t, err, ErrFetcherBadResponse)
	assert.Contains(t, err.Error(), "job id is required")
}

func TestValidateFetcherResourceID_EmptyActual_ReturnsError(t *testing.T) {
	t.Parallel()

	err := validateFetcherResourceID("job", "job-1", "")

	require.Error(t, err)
	assert.ErrorIs(t, err, ErrFetcherBadResponse)
	assert.Contains(t, err.Error(), "job id is required")
}

func TestValidateFetcherResourceID_BothEmpty_ReturnsError(t *testing.T) {
	t.Parallel()

	err := validateFetcherResourceID("connection", "", "")

	require.Error(t, err)
	assert.ErrorIs(t, err, ErrFetcherBadResponse)
}

func TestValidateFetcherResourceID_WhitespaceOnly_ReturnsError(t *testing.T) {
	t.Parallel()

	err := validateFetcherResourceID("job", "   ", "job-1")

	require.Error(t, err)
	assert.ErrorIs(t, err, ErrFetcherBadResponse)
	assert.Contains(t, err.Error(), "job id is required")
}

func TestValidateFetcherResourceID_TrimsWhitespace(t *testing.T) {
	t.Parallel()

	err := validateFetcherResourceID("connection", " conn-abc ", " conn-abc ")

	assert.NoError(t, err)
}

func TestValidateFetcherResourceID_WhitespaceMismatch_ReturnsError(t *testing.T) {
	t.Parallel()

	err := validateFetcherResourceID("connection", " conn-abc ", " conn-xyz ")

	require.Error(t, err)
	assert.ErrorIs(t, err, ErrFetcherBadResponse)
	assert.Contains(t, err.Error(), "connection id mismatch")
}

func TestNormalizeExtractionStatus_PassthroughStatuses(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{"pending lowercase", "pending", "PENDING"},
		{"PENDING uppercase", "PENDING", "PENDING"},
		{"running uppercase", "RUNNING", "RUNNING"},
		{"failed lowercase", "failed", "FAILED"},
		{"FAILED uppercase", "FAILED", "FAILED"},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			resp := fetcherExtractionStatusResponse{Status: tt.input}

			status, err := normalizeExtractionStatus(resp)

			require.NoError(t, err)
			assert.Equal(t, tt.expected, status)
		})
	}
}

func TestNormalizeExtractionStatus_Submitted_Passthrough(t *testing.T) {
	t.Parallel()

	resp := fetcherExtractionStatusResponse{Status: "submitted"}

	status, err := normalizeExtractionStatus(resp)

	require.NoError(t, err)
	assert.Equal(t, "SUBMITTED", status)
}

func TestNormalizeExtractionStatus_Processing_MapsToRunning(t *testing.T) {
	t.Parallel()

	resp := fetcherExtractionStatusResponse{Status: "processing"}

	status, err := normalizeExtractionStatus(resp)

	require.NoError(t, err)
	assert.Equal(t, "RUNNING", status)
}

func TestNormalizeExtractionStatus_Completed_MapsToComplete(t *testing.T) {
	t.Parallel()

	resp := fetcherExtractionStatusResponse{
		Status:     "completed",
		ResultPath: "/data/results/job-1.json",
	}

	status, err := normalizeExtractionStatus(resp)

	require.NoError(t, err)
	assert.Equal(t, "COMPLETE", status)
}

func TestNormalizeExtractionStatus_Extracting_Passthrough(t *testing.T) {
	t.Parallel()

	resp := fetcherExtractionStatusResponse{Status: "extracting"}

	status, err := normalizeExtractionStatus(resp)

	require.NoError(t, err)
	assert.Equal(t, "EXTRACTING", status)
}

func TestNormalizeExtractionStatus_Canceled_MapsToCancelled(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		input string
	}{
		{"lowercase", "canceled"},
		{"uppercase", "CANCELED"},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			resp := fetcherExtractionStatusResponse{Status: tt.input}

			status, err := normalizeExtractionStatus(resp)

			require.NoError(t, err)
			assert.Equal(t, "CANCELLED", status)
		})
	}
}

func TestNormalizeExtractionStatus_Complete_MissingResultPath_ReturnsError(t *testing.T) {
	t.Parallel()

	resp := fetcherExtractionStatusResponse{Status: "COMPLETE", ResultPath: ""}

	status, err := normalizeExtractionStatus(resp)

	require.Error(t, err)
	assert.Empty(t, status)
	assert.ErrorIs(t, err, ErrFetcherBadResponse)
	assert.Contains(t, err.Error(), "missing result path")
}

func TestNormalizeExtractionStatus_Complete_WhitespaceResultPath_ReturnsError(t *testing.T) {
	t.Parallel()

	resp := fetcherExtractionStatusResponse{Status: "COMPLETE", ResultPath: "   "}

	status, err := normalizeExtractionStatus(resp)

	require.Error(t, err)
	assert.Empty(t, status)
	assert.ErrorIs(t, err, ErrFetcherBadResponse)
}

func TestNormalizeExtractionStatus_Complete_InvalidResultPath_ReturnsError(t *testing.T) {
	t.Parallel()

	resp := fetcherExtractionStatusResponse{
		Status:     "COMPLETE",
		ResultPath: "s3://bucket/output.csv",
	}

	status, err := normalizeExtractionStatus(resp)

	require.Error(t, err)
	assert.Empty(t, status)
	assert.ErrorIs(t, err, ErrFetcherBadResponse)
}

func TestNormalizeExtractionStatus_UnknownStatus_ReturnsError(t *testing.T) {
	t.Parallel()

	resp := fetcherExtractionStatusResponse{Status: "UNKNOWN_STATUS"}

	status, err := normalizeExtractionStatus(resp)

	require.Error(t, err)
	assert.Empty(t, status)
	assert.ErrorIs(t, err, ErrFetcherBadResponse)
	assert.Contains(t, err.Error(), "unknown extraction status")
}

func TestNormalizeExtractionStatus_EmptyStatus_ReturnsError(t *testing.T) {
	t.Parallel()

	resp := fetcherExtractionStatusResponse{Status: ""}

	status, err := normalizeExtractionStatus(resp)

	require.Error(t, err)
	assert.Empty(t, status)
	assert.ErrorIs(t, err, ErrFetcherBadResponse)
}

func TestNormalizeExtractionStatus_WhitespaceStatus_ReturnsError(t *testing.T) {
	t.Parallel()

	resp := fetcherExtractionStatusResponse{Status: "   "}

	status, err := normalizeExtractionStatus(resp)

	require.Error(t, err)
	assert.Empty(t, status)
	assert.ErrorIs(t, err, ErrFetcherBadResponse)
}

func TestNormalizeExtractionStatus_TrimsWhitespace(t *testing.T) {
	t.Parallel()

	resp := fetcherExtractionStatusResponse{Status: "  pending  "}

	status, err := normalizeExtractionStatus(resp)

	require.NoError(t, err)
	assert.Equal(t, "PENDING", status)
}

func TestNormalizeExtractionStatus_MixedCase(t *testing.T) {
	t.Parallel()

	resp := fetcherExtractionStatusResponse{Status: "ProCeSsInG"}

	status, err := normalizeExtractionStatus(resp)

	require.NoError(t, err)
	assert.Equal(t, "RUNNING", status)
}
