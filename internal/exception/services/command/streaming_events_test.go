// Copyright 2026 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package command

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/exception/domain/entities"
	vo "github.com/LerianStudio/matcher/internal/exception/domain/value_objects"
	"github.com/LerianStudio/matcher/internal/streaming/emission"
)

func TestExceptionPayloadIncludesOptionalFieldsAndExtraData(t *testing.T) {
	exceptionID := uuid.New()
	transactionID := uuid.New()
	assignee := "analyst@example.com"
	resolutionType := "manual_match"
	exception := &entities.Exception{
		ID:             exceptionID,
		TransactionID:  transactionID,
		Status:         vo.ExceptionStatusAssigned,
		AssignedTo:     &assignee,
		ResolutionType: &resolutionType,
		Version:        3,
	}

	payload := exceptionPayload(exception, map[string]any{"resolved_at": "2026-05-04T00:00:00Z"})

	assert.Equal(t, exceptionID.String(), payload["exception_id"])
	assert.Equal(t, transactionID.String(), payload["transaction_id"])
	assert.Equal(t, "ASSIGNED", payload["status"])
	assert.Equal(t, int64(3), payload["version"])
	assert.NotContains(t, payload, "assignee")
	assert.Equal(t, resolutionType, payload["resolution_type"])
	assert.Equal(t, "2026-05-04T00:00:00Z", payload["resolved_at"])
}

func TestFormatExceptionTimeUsesUTCAndRFC3339Nano(t *testing.T) {
	input := time.Date(2026, time.May, 4, 10, 11, 12, 13, time.FixedZone("BRT", -3*60*60))

	formatted := formatExceptionTime(input)

	parsed, err := time.Parse(time.RFC3339Nano, formatted)
	require.NoError(t, err)
	assert.Equal(t, input.UTC(), parsed)
}

func TestValidateCriticalTxLeaseRejectsNilLease(t *testing.T) {
	err := validateCriticalTxLease(nil, "begin critical transaction")

	require.ErrorIs(t, err, emission.ErrCriticalOutboxTxRequired)
}
