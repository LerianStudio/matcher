package fetcher

import (
	"fmt"
	"net/http"
	"path"
	"strings"
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

// normalizeExtractionStatus maps Fetcher's lowercase extraction status values to
// Matcher's canonical uppercase representation. The full mapping is:
//
//	Fetcher (lowercase)  -> Matcher (uppercase)
//	--------------------------------------------
//	pending              -> PENDING
//	submitted            -> SUBMITTED
//	processing           -> RUNNING       (renamed)
//	extracting           -> EXTRACTING
//	completed            -> COMPLETE      (renamed)
//	failed               -> FAILED
//	canceled             -> CANCELLED     (British spelling)
//
// Returns an error if the status is unrecognized or if a COMPLETE status lacks a result path.
func normalizeExtractionStatus(resp fetcherExtractionStatusResponse) (string, error) {
	normalizedStatus := strings.ToUpper(strings.TrimSpace(resp.Status))

	// Map Fetcher lowercase conventions to canonical uppercase.
	switch normalizedStatus {
	case "PROCESSING":
		normalizedStatus = "RUNNING"
	case "COMPLETED":
		normalizedStatus = "COMPLETE"
	case "CANCELED":
		normalizedStatus = "CANCELLED"
	}

	switch normalizedStatus {
	case "PENDING", "SUBMITTED", "RUNNING", "EXTRACTING":
		return normalizedStatus, nil
	case "COMPLETE":
		if strings.TrimSpace(resp.ResultPath) == "" {
			return "", fmt.Errorf("%w: complete extraction missing result path", ErrFetcherBadResponse)
		}

		if err := validateFetcherResultPath(resp.ResultPath); err != nil {
			return "", fmt.Errorf("%w: %w", ErrFetcherBadResponse, err)
		}

		return normalizedStatus, nil
	case "FAILED":
		return normalizedStatus, nil
	case "CANCELLED":
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
func classifyResponse(statusCode int, body []byte) ([]byte, error) {
	if statusCode == http.StatusNotFound {
		return nil, ErrFetcherNotFound
	}

	if statusCode >= http.StatusMultipleChoices && statusCode < http.StatusBadRequest {
		return nil, fmt.Errorf("%w: redirects are not allowed (status %d)", ErrFetcherBadResponse, statusCode)
	}

	if statusCode >= http.StatusBadRequest {
		return nil, fmt.Errorf("%w: status %d", ErrFetcherBadResponse, statusCode)
	}

	return body, nil
}
