//go:build unit

package dto

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUploadRequest_JSON(t *testing.T) {
	t.Parallel()

	req := UploadRequest{
		FileName: "transactions.csv",
		Format:   "csv",
	}

	data, err := json.Marshal(req)
	require.NoError(t, err)

	var decoded UploadRequest

	err = json.Unmarshal(data, &decoded)
	require.NoError(t, err)

	assert.Equal(t, req.FileName, decoded.FileName)
	assert.Equal(t, req.Format, decoded.Format)
}

func TestIgnoreTransactionRequest_JSON(t *testing.T) {
	t.Parallel()

	req := IgnoreTransactionRequest{
		Reason: "Duplicate entry",
	}

	data, err := json.Marshal(req)
	require.NoError(t, err)

	var decoded IgnoreTransactionRequest

	err = json.Unmarshal(data, &decoded)
	require.NoError(t, err)

	assert.Equal(t, req.Reason, decoded.Reason)
}
