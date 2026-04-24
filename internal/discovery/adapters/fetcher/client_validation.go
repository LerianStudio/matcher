// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package fetcher

import (
	"fmt"
	"net/http"
	"path"
	"strings"
)

// errFmtStructured is the format string used when Fetcher returns a structured error body.
// Propagate the stable upstream code, but avoid echoing the full upstream message into
// Matcher errors because it can leak operational details into logs and traces.
const errFmtStructured = "%w: status %d [%s]"

// errFmtStatusOnly is the format string used when the error body is absent or unparseable.
const errFmtStatusOnly = "%w: status %d"

const (
	statusSubmitted  = "SUBMITTED"
	statusExtracting = "EXTRACTING"
)

func validateFetcherResourceID(resource, expected, actual string) error {
	trimmedExpected := strings.TrimSpace(expected)
	trimmedActual := strings.TrimSpace(actual)

	if trimmedExpected == "" || trimmedActual == "" {
		return fmt.Errorf("%w: %s id is required", ErrFetcherBadResponse, resource)
	}

	if trimmedExpected != trimmedActual {
		return fmt.Errorf("%w: %s id mismatch (expected %q, got %q)", ErrFetcherBadResponse, resource, trimmedExpected, trimmedActual)
	}

	return nil
}

// normalizeExtractionStatus maps Fetcher extraction status values to Matcher's
// canonical uppercase representation. Keep compatibility with older or mixed
// upstream deployments because downstream pollers still model these states.
//
//	Fetcher              -> Matcher
//	-------------------------------
//	pending              -> PENDING
//	submitted            -> SUBMITTED
//	processing           -> RUNNING
//	extracting           -> EXTRACTING
//	completed            -> COMPLETE
//	failed               -> FAILED
//	canceled/cancelled   -> CANCELLED
//
// Returns an error if the status is unrecognized or if a COMPLETE status lacks a result path.
func normalizeExtractionStatus(resp fetcherExtractionStatusResponse) (string, error) {
	normalizedStatus := strings.ToUpper(strings.TrimSpace(resp.Status))

	// Map Fetcher lowercase conventions to canonical uppercase.
	switch normalizedStatus {
	case "CANCELED":
		normalizedStatus = "CANCELLED"
	case "COMPLETED":
		normalizedStatus = "COMPLETE"
	case statusExtracting:
		normalizedStatus = statusExtracting
	case "PROCESSING":
		normalizedStatus = "RUNNING"
	case statusSubmitted:
		normalizedStatus = statusSubmitted
	}

	switch normalizedStatus {
	case "PENDING", statusSubmitted, "RUNNING", statusExtracting:
		return normalizedStatus, nil
	case "COMPLETE":
		if strings.TrimSpace(resp.ResultPath) == "" {
			return "", fmt.Errorf("%w: complete extraction missing result path", ErrFetcherBadResponse)
		}

		if err := validateFetcherResultPath(resp.ResultPath); err != nil {
			return "", fmt.Errorf("%w: %w", ErrFetcherBadResponse, err)
		}

		return normalizedStatus, nil
	case "FAILED", "CANCELLED":
		return normalizedStatus, nil
	default:
		return "", fmt.Errorf("%w: unknown extraction status %q", ErrFetcherBadResponse, resp.Status)
	}
}

func validateFetcherResultPath(resultPath string) error {
	trimmed := strings.TrimSpace(resultPath)

	if trimmed == "" {
		return ErrFetcherResultPathRequired
	}

	if !strings.HasPrefix(trimmed, "/") {
		return ErrFetcherResultPathNotAbsolute
	}

	if strings.Contains(trimmed, "://") || strings.ContainsAny(trimmed, "?#") {
		return ErrFetcherResultPathInvalidFormat
	}

	cleaned := path.Clean(trimmed)
	if cleaned != trimmed || strings.Contains(trimmed, "..") {
		return ErrFetcherResultPathTraversal
	}

	return nil
}

// classifyResponse maps HTTP status codes to domain errors or returns the body on success.
// The returned statusCode lets callers distinguish semantically identical bodies (e.g., 200 vs 202).
func classifyResponse(statusCode int, body []byte) ([]byte, int, error) {
	// 1xx informational responses should never surface here — Go's HTTP
	// transport transparently handles them and delivers the final status
	// to callers. Treat any 1xx that slips through as an unexpected
	// upstream condition rather than silently falling through to the
	// success branch below.
	if statusCode >= http.StatusContinue && statusCode < http.StatusOK {
		return nil, statusCode, fmt.Errorf("%w: unexpected 1xx response %d", ErrFetcherBadResponse, statusCode)
	}

	if statusCode == http.StatusNotFound {
		return nil, statusCode, ErrFetcherNotFound
	}

	if statusCode >= http.StatusMultipleChoices && statusCode < http.StatusBadRequest {
		return nil, statusCode, fmt.Errorf("%w: redirects are not allowed (status %d)", ErrFetcherBadResponse, statusCode)
	}

	if statusCode >= http.StatusBadRequest {
		if parsed := tryParseFetcherError(body); parsed != nil {
			if parsed.Code != "" {
				return nil, statusCode, fmt.Errorf(errFmtStructured, ErrFetcherBadResponse, statusCode, parsed.Code)
			}
		}

		return nil, statusCode, fmt.Errorf(errFmtStatusOnly, ErrFetcherBadResponse, statusCode)
	}

	return body, statusCode, nil
}
