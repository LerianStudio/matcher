// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package http

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"

	matchererrors "github.com/LerianStudio/matcher/pkg"
)

func TestDefinitionFromLegacySlug_UsesSpecificMappings(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		slug     string
		status   int
		expected matchererrors.Definition
	}{
		{slug: "duplicate_name", status: http.StatusConflict, expected: defConfigurationDuplicateName},
		{slug: "rate_limit_exceeded", status: http.StatusTooManyRequests, expected: defCallbackRateLimitExceeded},
		{slug: "configuration_field_map_not_found", status: http.StatusNotFound, expected: defConfigurationFieldMapNotFound},
		{slug: "discovery_invalid_extraction", status: http.StatusBadRequest, expected: defDiscoveryInvalidExtraction},
		{slug: "ingestion_job_not_found", status: http.StatusNotFound, expected: defIngestionJobNotFound},
		{slug: "comment_not_found", status: http.StatusNotFound, expected: defCommentNotFound},
		{slug: "reporting_export_job_not_found", status: http.StatusNotFound, expected: defReportingExportJobNotFound},
	}

	for _, testCase := range testCases {
		t.Run(testCase.slug, func(t *testing.T) {
			definition, ok := definitionFromLegacySlug(testCase.slug)
			require.True(t, ok)
			require.Equal(t, testCase.expected, definition)
		})
	}
}

func TestDefinitionFromLegacySlug_FallsBackByStatus(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name     string
		status   int
		expected matchererrors.Definition
	}{
		{name: "bad request", status: http.StatusBadRequest, expected: defInvalidRequest},
		{name: "unauthorized", status: http.StatusUnauthorized, expected: defUnauthorized},
		{name: "forbidden", status: http.StatusForbidden, expected: defForbidden},
		{name: "not found", status: http.StatusNotFound, expected: defNotFound},
		{name: "conflict", status: http.StatusConflict, expected: defConflict},
		{name: "unprocessable", status: http.StatusUnprocessableEntity, expected: defUnprocessableEntity},
		{name: "rate limit", status: http.StatusTooManyRequests, expected: defRateLimitExceeded},
		{name: "payload too large", status: http.StatusRequestEntityTooLarge, expected: defRequestEntityTooLarge},
		{name: "service unavailable", status: http.StatusServiceUnavailable, expected: defServiceUnavailable},
		{name: "generic client", status: http.StatusMethodNotAllowed, expected: matchererrors.Definition{Code: defRequestFailed.Code, Title: http.StatusText(http.StatusMethodNotAllowed), HTTPStatus: http.StatusMethodNotAllowed}},
		{name: "default", status: 599, expected: defInternalServerError},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			definition := definitionForStatus(testCase.status)
			require.Equal(t, testCase.expected, definition)
		})
	}
}
