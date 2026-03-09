//go:build unit

package dto

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	sharedhttp "github.com/LerianStudio/lib-commons/v4/commons/net/http"
)

func TestForceMatchRequest_JSON(t *testing.T) {
	t.Parallel()

	req := ForceMatchRequest{
		OverrideReason: "BUSINESS_DECISION",
		Notes:          "Approved by finance team",
	}

	data, err := json.Marshal(req)
	require.NoError(t, err)

	var decoded ForceMatchRequest

	err = json.Unmarshal(data, &decoded)
	require.NoError(t, err)

	assert.Equal(t, req.OverrideReason, decoded.OverrideReason)
	assert.Equal(t, req.Notes, decoded.Notes)
}

func TestAdjustEntryRequest_JSON(t *testing.T) {
	t.Parallel()

	req := AdjustEntryRequest{
		ReasonCode:  "FEE_ADJUSTMENT",
		Notes:       "Correcting fee discrepancy",
		Amount:      decimal.NewFromFloat(150.50),
		Currency:    "USD",
		EffectiveAt: time.Now().UTC(),
	}

	data, err := json.Marshal(req)
	require.NoError(t, err)

	var decoded AdjustEntryRequest

	err = json.Unmarshal(data, &decoded)
	require.NoError(t, err)

	assert.Equal(t, req.ReasonCode, decoded.ReasonCode)
	assert.True(t, req.Amount.Equal(decoded.Amount))
}

func TestOpenDisputeRequest_JSON(t *testing.T) {
	t.Parallel()

	req := OpenDisputeRequest{
		Category:    "AMOUNT_MISMATCH",
		Description: "Transaction amount differs from invoice",
	}

	data, err := json.Marshal(req)
	require.NoError(t, err)

	var decoded OpenDisputeRequest

	err = json.Unmarshal(data, &decoded)
	require.NoError(t, err)

	assert.Equal(t, req.Category, decoded.Category)
	assert.Equal(t, req.Description, decoded.Description)
}

func TestCloseDisputeRequest_JSON(t *testing.T) {
	t.Parallel()

	req := CloseDisputeRequest{
		Won:        true,
		Resolution: "Counterparty acknowledged the error",
	}

	data, err := json.Marshal(req)
	require.NoError(t, err)

	var decoded CloseDisputeRequest

	err = json.Unmarshal(data, &decoded)
	require.NoError(t, err)

	assert.Equal(t, req.Won, decoded.Won)
	assert.Equal(t, req.Resolution, decoded.Resolution)
}

func TestSubmitEvidenceRequest_JSON(t *testing.T) {
	t.Parallel()

	fileURL := "https://storage.example.com/evidence/doc123.pdf"
	req := SubmitEvidenceRequest{
		Comment: "Attached bank statement",
		FileURL: &fileURL,
	}

	data, err := json.Marshal(req)
	require.NoError(t, err)

	var decoded SubmitEvidenceRequest

	err = json.Unmarshal(data, &decoded)
	require.NoError(t, err)

	assert.Equal(t, req.Comment, decoded.Comment)
	require.NotNil(t, decoded.FileURL)
	assert.Equal(t, *req.FileURL, *decoded.FileURL)
}

func TestDispatchRequest_JSON(t *testing.T) {
	t.Parallel()

	req := DispatchRequest{
		TargetSystem: "JIRA",
		Queue:        "RECON-TEAM",
	}

	data, err := json.Marshal(req)
	require.NoError(t, err)

	var decoded DispatchRequest

	err = json.Unmarshal(data, &decoded)
	require.NoError(t, err)

	assert.Equal(t, req.TargetSystem, decoded.TargetSystem)
	assert.Equal(t, req.Queue, decoded.Queue)
}

func TestForceMatchRequest_Validation(t *testing.T) {
	t.Parallel()

	t.Run("missing override reason fails", func(t *testing.T) {
		t.Parallel()

		req := ForceMatchRequest{Notes: "some notes"}
		err := sharedhttp.ValidateStruct(req)
		assert.Error(t, err, "missing overrideReason should fail validation")
	})

	t.Run("missing notes fails", func(t *testing.T) {
		t.Parallel()

		req := ForceMatchRequest{OverrideReason: "BUSINESS_DECISION"}
		err := sharedhttp.ValidateStruct(req)
		assert.Error(t, err, "missing notes should fail validation")
	})

	t.Run("valid request passes", func(t *testing.T) {
		t.Parallel()

		req := ForceMatchRequest{OverrideReason: "BUSINESS_DECISION", Notes: "Approved"}
		err := sharedhttp.ValidateStruct(req)
		assert.NoError(t, err)
	})
}

func TestOpenDisputeRequest_Validation(t *testing.T) {
	t.Parallel()

	t.Run("missing category fails", func(t *testing.T) {
		t.Parallel()

		req := OpenDisputeRequest{Description: "some desc"}
		err := sharedhttp.ValidateStruct(req)
		assert.Error(t, err, "missing category should fail validation")
	})

	t.Run("missing description fails", func(t *testing.T) {
		t.Parallel()

		req := OpenDisputeRequest{Category: "AMOUNT_MISMATCH"}
		err := sharedhttp.ValidateStruct(req)
		assert.Error(t, err, "missing description should fail validation")
	})

	t.Run("valid request passes", func(t *testing.T) {
		t.Parallel()

		req := OpenDisputeRequest{Category: "AMOUNT_MISMATCH", Description: "Amount differs"}
		err := sharedhttp.ValidateStruct(req)
		assert.NoError(t, err)
	})
}

func TestDispatchRequest_Validation(t *testing.T) {
	t.Parallel()

	t.Run("missing target system fails", func(t *testing.T) {
		t.Parallel()

		req := DispatchRequest{Queue: "RECON-TEAM"}
		err := sharedhttp.ValidateStruct(req)
		assert.Error(t, err, "missing targetSystem should fail validation")
	})

	t.Run("valid request passes", func(t *testing.T) {
		t.Parallel()

		req := DispatchRequest{TargetSystem: "JIRA"}
		err := sharedhttp.ValidateStruct(req)
		assert.NoError(t, err)
	})
}
