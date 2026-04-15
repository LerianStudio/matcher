//go:build unit

package fetcher

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- validateFetcherResourceID tests ---

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

	// After trimming, "conn-abc" != "conn-xyz" — still a mismatch.
	err := validateFetcherResourceID("connection", " conn-abc ", " conn-xyz ")

	require.Error(t, err)
	assert.ErrorIs(t, err, ErrFetcherBadResponse)
	assert.Contains(t, err.Error(), "connection id mismatch")
}

// --- normalizeExtractionStatus tests ---

func TestNormalizeExtractionStatus_PassthroughStatuses(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{"pending lowercase", "pending", "PENDING"},
		{"PENDING uppercase", "PENDING", "PENDING"},
		{"submitted lowercase", "submitted", "SUBMITTED"},
		{"SUBMITTED uppercase", "SUBMITTED", "SUBMITTED"},
		{"running uppercase", "RUNNING", "RUNNING"},
		{"extracting lowercase", "extracting", "EXTRACTING"},
		{"EXTRACTING uppercase", "EXTRACTING", "EXTRACTING"},
		{"failed lowercase", "failed", "FAILED"},
		{"FAILED uppercase", "FAILED", "FAILED"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			resp := fetcherExtractionStatusResponse{Status: tt.input}

			status, err := normalizeExtractionStatus(resp)

			require.NoError(t, err)
			assert.Equal(t, tt.expected, status)
		})
	}
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

func TestNormalizeExtractionStatus_Canceled_MapsToCancelled(t *testing.T) {
	t.Parallel()

	resp := fetcherExtractionStatusResponse{Status: "canceled"}

	status, err := normalizeExtractionStatus(resp)

	require.NoError(t, err)
	assert.Equal(t, "CANCELLED", status)
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

// --- validateFetcherResultPath tests ---

func TestValidateFetcherResultPath_ValidAbsolutePath_Success(t *testing.T) {
	t.Parallel()

	err := validateFetcherResultPath("/data/results/job-1.json")

	assert.NoError(t, err)
}

func TestValidateFetcherResultPath_EmptyPath_ReturnsError(t *testing.T) {
	t.Parallel()

	err := validateFetcherResultPath("")

	require.Error(t, err)
	assert.ErrorIs(t, err, ErrFetcherResultPathRequired)
}

func TestValidateFetcherResultPath_WhitespacePath_ReturnsError(t *testing.T) {
	t.Parallel()

	err := validateFetcherResultPath("   ")

	require.Error(t, err)
	assert.ErrorIs(t, err, ErrFetcherResultPathRequired)
}

func TestValidateFetcherResultPath_RelativePath_ReturnsError(t *testing.T) {
	t.Parallel()

	err := validateFetcherResultPath("data/results/output.csv")

	require.Error(t, err)
	assert.ErrorIs(t, err, ErrFetcherResultPathNotAbsolute)
}

func TestValidateFetcherResultPath_URLScheme_NonAbsolute_ReturnsNotAbsolute(t *testing.T) {
	t.Parallel()

	// Paths like "s3://..." and "ftp://..." don't start with "/" so they
	// are rejected as non-absolute before the "://" check is reached.
	tests := []struct {
		name string
		path string
	}{
		{"s3 scheme", "s3://bucket/output.csv"},
		{"ftp scheme", "ftp://server/file.csv"},
		{"https scheme", "https://example.com/output.csv"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := validateFetcherResultPath(tt.path)

			require.Error(t, err)
			assert.ErrorIs(t, err, ErrFetcherResultPathNotAbsolute)
		})
	}
}

func TestValidateFetcherResultPath_AbsolutePathWithScheme_ReturnsInvalidFormat(t *testing.T) {
	t.Parallel()

	// An absolute path that contains "://" is caught by the scheme check.
	err := validateFetcherResultPath("/data://bucket/output.csv")

	require.Error(t, err)
	assert.ErrorIs(t, err, ErrFetcherResultPathInvalidFormat)
}

func TestValidateFetcherResultPath_QueryString_ReturnsError(t *testing.T) {
	t.Parallel()

	err := validateFetcherResultPath("/data/output.csv?version=2")

	require.Error(t, err)
	assert.ErrorIs(t, err, ErrFetcherResultPathInvalidFormat)
}

func TestValidateFetcherResultPath_Fragment_ReturnsError(t *testing.T) {
	t.Parallel()

	err := validateFetcherResultPath("/data/output.csv#section")

	require.Error(t, err)
	assert.ErrorIs(t, err, ErrFetcherResultPathInvalidFormat)
}

func TestValidateFetcherResultPath_TraversalSegment_ReturnsError(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		path string
	}{
		{"double dot in middle", "/data/../etc/passwd"},
		{"double dot at start", "/../etc/shadow"},
		{"double dot only", "/.."},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := validateFetcherResultPath(tt.path)

			require.Error(t, err)
			assert.ErrorIs(t, err, ErrFetcherResultPathTraversal)
		})
	}
}

func TestValidateFetcherResultPath_UncleanPath_ReturnsError(t *testing.T) {
	t.Parallel()

	// path.Clean("/data//output.csv") returns "/data/output.csv" != "/data//output.csv"
	err := validateFetcherResultPath("/data//output.csv")

	require.Error(t, err)
	assert.ErrorIs(t, err, ErrFetcherResultPathTraversal)
}

func TestValidateFetcherResultPath_TrailingSlash_ReturnsError(t *testing.T) {
	t.Parallel()

	// path.Clean("/data/results/") returns "/data/results" != "/data/results/"
	err := validateFetcherResultPath("/data/results/")

	require.Error(t, err)
	assert.ErrorIs(t, err, ErrFetcherResultPathTraversal)
}

func TestValidateFetcherResultPath_RootPath_Success(t *testing.T) {
	t.Parallel()

	// path.Clean("/") returns "/" which matches.
	err := validateFetcherResultPath("/")

	assert.NoError(t, err)
}

func TestValidateFetcherResultPath_DeepNestedPath_Success(t *testing.T) {
	t.Parallel()

	err := validateFetcherResultPath("/data/extractions/2026/01/15/job-abc123/output.json")

	assert.NoError(t, err)
}

// --- classifyResponse tests ---

func TestClassifyResponse_Success_ReturnsBody(t *testing.T) {
	t.Parallel()

	body := []byte(`{"status":"ok"}`)

	result, err := classifyResponse(http.StatusOK, body)

	require.NoError(t, err)
	assert.Equal(t, body, result)
}

func TestClassifyResponse_201Created_ReturnsBody(t *testing.T) {
	t.Parallel()

	body := []byte(`{"id":"new-resource"}`)

	result, err := classifyResponse(http.StatusCreated, body)

	require.NoError(t, err)
	assert.Equal(t, body, result)
}

func TestClassifyResponse_204NoContent_ReturnsEmptyBody(t *testing.T) {
	t.Parallel()

	result, err := classifyResponse(http.StatusNoContent, nil)

	require.NoError(t, err)
	assert.Nil(t, result)
}

func TestClassifyResponse_404NotFound_ReturnsSpecificError(t *testing.T) {
	t.Parallel()

	result, err := classifyResponse(http.StatusNotFound, nil)

	require.Error(t, err)
	assert.Nil(t, result)
	assert.ErrorIs(t, err, ErrFetcherNotFound)
}

func TestClassifyResponse_RedirectStatuses_ReturnsError(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		statusCode int
	}{
		{"301 Moved Permanently", http.StatusMovedPermanently},
		{"302 Found", http.StatusFound},
		{"307 Temporary Redirect", http.StatusTemporaryRedirect},
		{"308 Permanent Redirect", http.StatusPermanentRedirect},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result, err := classifyResponse(tt.statusCode, nil)

			require.Error(t, err)
			assert.Nil(t, result)
			assert.ErrorIs(t, err, ErrFetcherBadResponse)
			assert.Contains(t, err.Error(), "redirects are not allowed")
		})
	}
}

func TestClassifyResponse_ClientErrors_ReturnsBadResponse(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		statusCode int
	}{
		{"400 Bad Request", http.StatusBadRequest},
		{"401 Unauthorized", http.StatusUnauthorized},
		{"403 Forbidden", http.StatusForbidden},
		{"409 Conflict", http.StatusConflict},
		{"422 Unprocessable", http.StatusUnprocessableEntity},
		{"429 Too Many Requests", http.StatusTooManyRequests},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result, err := classifyResponse(tt.statusCode, nil)

			require.Error(t, err)
			assert.Nil(t, result)
			assert.ErrorIs(t, err, ErrFetcherBadResponse)
		})
	}
}

func TestClassifyResponse_ServerErrors_ReturnsBadResponse(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		statusCode int
	}{
		{"500 Internal Server Error", http.StatusInternalServerError},
		{"502 Bad Gateway", http.StatusBadGateway},
		{"503 Service Unavailable", http.StatusServiceUnavailable},
		{"504 Gateway Timeout", http.StatusGatewayTimeout},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result, err := classifyResponse(tt.statusCode, nil)

			require.Error(t, err)
			assert.Nil(t, result)
			assert.ErrorIs(t, err, ErrFetcherBadResponse)
		})
	}
}

func TestClassifyResponse_300MultipleChoices_IsRedirectRange(t *testing.T) {
	t.Parallel()

	// 300 is the start of the redirect range (>= 300 && < 400).
	result, err := classifyResponse(http.StatusMultipleChoices, nil)

	require.Error(t, err)
	assert.Nil(t, result)
	assert.ErrorIs(t, err, ErrFetcherBadResponse)
	assert.Contains(t, err.Error(), "redirects are not allowed")
}

func TestClassifyResponse_399_IsRedirectRange(t *testing.T) {
	t.Parallel()

	// 399 is still in the redirect range (>= 300 && < 400).
	result, err := classifyResponse(399, nil)

	require.Error(t, err)
	assert.Nil(t, result)
	assert.ErrorIs(t, err, ErrFetcherBadResponse)
	assert.Contains(t, err.Error(), "redirects are not allowed")
}

func TestClassifyResponse_404_TakesPriorityOverGenericClientError(t *testing.T) {
	t.Parallel()

	// 404 is checked BEFORE the >= 400 range, so it returns ErrFetcherNotFound
	// rather than ErrFetcherBadResponse.
	result, err := classifyResponse(http.StatusNotFound, []byte(`{"error":"not found"}`))

	require.Error(t, err)
	assert.Nil(t, result)
	assert.ErrorIs(t, err, ErrFetcherNotFound)
	// The body is discarded on 404.
}
