package fetcher

import "encoding/json"

// fetcherErrorBody maps the error response body returned by Fetcher on non-2xx responses.
// Fetcher's HTTPError shape: {"code":"INVALID_PATH_PARAMETER","title":"...","message":"...","entityType":"..."}.
type fetcherErrorBody struct {
	Code       string `json:"code,omitempty"`
	Title      string `json:"title,omitempty"`
	Message    string `json:"message,omitempty"`
	EntityType string `json:"entityType,omitempty"`
}

// tryParseFetcherError attempts to extract structured error detail from a Fetcher error response body.
// Returns the parsed error body if successful and at least one of code/message is non-empty.
// Returns nil when the body is not valid JSON or contains no useful detail.
func tryParseFetcherError(body []byte) *fetcherErrorBody {
	if len(body) == 0 {
		return nil
	}

	var errBody fetcherErrorBody
	if err := json.Unmarshal(body, &errBody); err != nil {
		return nil
	}

	if errBody.Code == "" && errBody.Message == "" {
		return nil
	}

	return &errBody
}
