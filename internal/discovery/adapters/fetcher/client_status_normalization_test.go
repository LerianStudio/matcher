//go:build unit

package fetcher

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNormalizeExtractionStatus_Submitted_Passthrough_Uppercase(t *testing.T) {
	t.Parallel()

	resp := fetcherExtractionStatusResponse{Status: "submitted"}

	status, err := normalizeExtractionStatus(resp)

	require.NoError(t, err)
	assert.Equal(t, "SUBMITTED", status)
}

func TestNormalizeExtractionStatus_Extracting_Passthrough_Uppercase(t *testing.T) {
	t.Parallel()

	resp := fetcherExtractionStatusResponse{Status: "extracting"}

	status, err := normalizeExtractionStatus(resp)

	require.NoError(t, err)
	assert.Equal(t, "EXTRACTING", status)
}

func TestNormalizeExtractionStatus_Canceled_MapsToCancelled_Uppercase(t *testing.T) {
	t.Parallel()

	resp := fetcherExtractionStatusResponse{Status: "CANCELED"}

	status, err := normalizeExtractionStatus(resp)

	require.NoError(t, err)
	assert.Equal(t, "CANCELLED", status)
}
