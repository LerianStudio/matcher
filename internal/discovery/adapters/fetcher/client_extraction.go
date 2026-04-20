package fetcher

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	libCommons "github.com/LerianStudio/lib-commons/v5/commons"
	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"

	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

// SubmitExtractionJob submits an async data extraction job to Fetcher.
// Returns the Fetcher-assigned job ID. Fetcher returns 202 for new jobs and
// 200 when an identical request was already submitted (dedup hit). The port
// contract is unchanged; the dedup signal is logged for observability only.
func (client *HTTPFetcherClient) SubmitExtractionJob(ctx context.Context, input sharedPorts.ExtractionJobInput) (string, error) {
	if err := client.ensureReady(); err != nil {
		return "", err
	}

	logger, _, _, _ := libCommons.NewTrackingFromContext(ctx) //nolint:dogsled // only logger needed here

	reqBody := fetcherExtractionSubmitRequest{
		DataRequest: fetcherDataRequest{
			MappedFields: input.MappedFields,
			Filters:      convertPortFiltersToTypedFilters(input.Filters),
		},
		Metadata: input.Metadata,
	}

	jsonBody, err := json.Marshal(reqBody)
	if err != nil {
		return "", fmt.Errorf("marshal extraction request: %w", err)
	}

	body, statusCode, err := client.doPostWithStatus(ctx, client.baseURL+"/v1/fetcher", jsonBody)
	if err != nil {
		return "", fmt.Errorf("submit extraction: %w", err)
	}

	var resp fetcherExtractionSubmitResponse

	if err := rejectEmptyOrNullBody(body); err != nil {
		return "", err
	}

	if err := json.Unmarshal(body, &resp); err != nil {
		return "", fmt.Errorf("decode extraction response: %w", err)
	}

	if strings.TrimSpace(resp.JobID) == "" {
		return "", fmt.Errorf("%w: %w", ErrFetcherBadResponse, ErrFetcherJobIDEmpty)
	}

	// Log dedup vs new-job distinction for observability.
	// 200 = Fetcher recognized an identical prior request (deduplicated).
	// 202 = Fetcher accepted a new extraction job.
	if statusCode == http.StatusOK {
		logger.Log(ctx, libLog.LevelInfo, "fetcher returned 200 (deduplicated submit)", libLog.String("jobID", resp.JobID))
	} else {
		logger.Log(ctx, libLog.LevelDebug, "fetcher accepted new extraction job", libLog.String("jobID", resp.JobID), libLog.Int("status", statusCode))
	}

	return resp.JobID, nil
}

// GetExtractionJobStatus polls the status of a running extraction job.
func (client *HTTPFetcherClient) GetExtractionJobStatus(ctx context.Context, jobID string) (*sharedPorts.ExtractionJobStatus, error) {
	if err := client.ensureReady(); err != nil {
		return nil, err
	}

	reqURL := client.baseURL + "/v1/fetcher/" + url.PathEscape(jobID)

	body, err := client.doGet(ctx, reqURL)
	if err != nil {
		return nil, fmt.Errorf("get extraction status: %w", err)
	}

	var resp fetcherExtractionStatusResponse

	if err := rejectEmptyOrNullBody(body); err != nil {
		return nil, err
	}

	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, fmt.Errorf("decode extraction status: %w", err)
	}

	if err := validateFetcherResourceID("job", jobID, resp.ID); err != nil {
		return nil, err
	}

	normalizedStatus, err := normalizeExtractionStatus(resp)
	if err != nil {
		return nil, err
	}

	return &sharedPorts.ExtractionJobStatus{
		ID:           resp.ID,
		Status:       normalizedStatus,
		MappedFields: resp.MappedFields,
		ResultPath:   resp.ResultPath,
		ResultHmac:   resp.ResultHmac,
		RequestHash:  resp.RequestHash,
		Metadata:     resp.Metadata,
		CreatedAt:    parseOptionalRFC3339(resp.CreatedAt),
		CompletedAt:  parseOptionalRFC3339Ptr(resp.CompletedAt),
	}, nil
}

// parseOptionalRFC3339 parses an RFC3339 timestamp string, returning zero time on failure.
func parseOptionalRFC3339(raw string) time.Time {
	if raw == "" {
		return time.Time{}
	}

	t, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		return time.Time{}
	}

	return t
}

// parseOptionalRFC3339Ptr parses an RFC3339 timestamp string, returning nil on empty/failure.
func parseOptionalRFC3339Ptr(raw string) *time.Time {
	if raw == "" {
		return nil
	}

	t, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		return nil
	}

	return &t
}

// convertPortFiltersToTypedFilters converts the port-level untyped filter representation
// (map[string]map[string]map[string]any) to the adapter-level typed request form
// (fetcherFilterCondition) for symmetric wire format on the request side.
func convertPortFiltersToTypedFilters(
	untyped map[string]map[string]map[string]any,
) map[string]map[string]map[string]fetcherFilterCondition {
	if len(untyped) == 0 {
		return nil
	}

	result := make(map[string]map[string]map[string]fetcherFilterCondition, len(untyped))

	for configName, tables := range untyped {
		tableMap := make(map[string]map[string]fetcherFilterCondition, len(tables))

		for tableName, fields := range tables {
			fieldMap := make(map[string]fetcherFilterCondition, len(fields))

			for fieldName, condition := range fields {
				condMap, ok := condition.(map[string]any)
				if !ok {
					// Scalar value (e.g. "USD") -- treat as an equality filter
					// for backward compat with existing callers that use plain values.
					fieldMap[fieldName] = fetcherFilterCondition{Eq: []any{condition}}

					continue
				}

				fieldMap[fieldName] = filterConditionFromMap(condMap)
			}

			tableMap[tableName] = fieldMap
		}

		result[configName] = tableMap
	}

	return result
}
