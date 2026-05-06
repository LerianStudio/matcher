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

	"github.com/LerianStudio/matcher/internal/discovery/domain/entities"
	vo "github.com/LerianStudio/matcher/internal/discovery/domain/value_objects"
)

func TestExtractionPayloadMergesBaseAndExtraFields(t *testing.T) {
	extractionID := uuid.New()
	connectionID := uuid.New()
	req := &entities.ExtractionRequest{
		ID:           extractionID,
		ConnectionID: connectionID,
		Status:       vo.ExtractionStatusComplete,
	}

	payload := extractionPayload(req, map[string]any{"has_result": true})

	assert.Equal(t, extractionID.String(), payload["extraction_request_id"])
	assert.Equal(t, connectionID.String(), payload["connection_id"])
	assert.Equal(t, "COMPLETE", payload["status"])
	// payload_version is intentionally absent: SchemaVersion lives on the
	// CloudEvents envelope, not the inner payload — duplicating it here would
	// fight the catalog as the source of truth.
	assert.NotContains(t, payload, "payload_version")
	assert.Equal(t, true, payload["has_result"])
}

func TestFormatDiscoveryTimeUsesUTCAndRFC3339Nano(t *testing.T) {
	input := time.Date(2026, time.May, 4, 10, 11, 12, 13, time.FixedZone("BRT", -3*60*60))

	formatted := formatDiscoveryTime(input)

	parsed, err := time.Parse(time.RFC3339Nano, formatted)
	require.NoError(t, err)
	assert.Equal(t, input.UTC(), parsed)
}
