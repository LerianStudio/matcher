//nolint:varnamelen,wsl_v5 // Test HTTP client helpers favor concise receiver names and direct path assembly.
package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"time"

	"github.com/google/uuid"
)

// Client is the base HTTP client for e2e tests.
type Client struct {
	baseURL    string
	httpClient *http.Client
	tenantID   string
	userID     string
}

const httpErrorStatusThreshold = 400

// RequestOptions customize outgoing test requests.
type RequestOptions struct {
	Headers        map[string]string
	IdempotencyKey string
}

// DefaultUserID is the default user ID for e2e tests when auth is disabled.
const DefaultUserID = "e2e-test-user@example.com"

// NewClient creates a new base client.
func NewClient(baseURL, tenantID string, timeout time.Duration) *Client {
	return &Client{
		baseURL:    baseURL,
		tenantID:   tenantID,
		userID:     DefaultUserID,
		httpClient: &http.Client{Timeout: timeout},
	}
}

// SetTenantID updates the tenant ID for subsequent requests.
func (c *Client) SetTenantID(tenantID string) {
	c.tenantID = tenantID
}

// TenantID returns the configured tenant ID.
func (c *Client) TenantID() string {
	return c.tenantID
}

// Do performs an HTTP request with tenant headers.
// For POST/PUT/PATCH requests, it automatically adds a unique idempotency key.
func (c *Client) Do(
	ctx context.Context,
	method, path string,
	body io.Reader,
	contentType string,
) (*http.Response, error) {
	return c.DoWithOptions(ctx, method, path, body, contentType, RequestOptions{})
}

// DoWithOptions performs an HTTP request with tenant headers and optional overrides.
func (c *Client) DoWithOptions(
	ctx context.Context,
	method, path string,
	body io.Reader,
	contentType string,
	opts RequestOptions,
) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, body)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}

	req.Header.Set("X-Tenant-ID", c.tenantID)
	req.Header.Set("X-User-ID", c.userID)
	req.Header.Set("Accept", "application/json")

	// Add unique idempotency key for write operations to prevent caching between tests
	if method == http.MethodPost || method == http.MethodPut || method == http.MethodPatch {
		idempotencyKey := opts.IdempotencyKey
		if idempotencyKey == "" {
			idempotencyKey = uuid.New().String()
		}

		req.Header.Set("X-Idempotency-Key", idempotencyKey)
	}

	for key, value := range opts.Headers {
		req.Header.Set(key, value)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("execute request: %w", err)
	}

	return resp, nil
}

// DoJSON performs a JSON request and decodes the response.
// The response body is read and closed internally.
func (c *Client) DoJSON(ctx context.Context, method, path string, reqBody, respBody any) error {
	return c.DoJSONWithOptions(ctx, method, path, reqBody, respBody, RequestOptions{})
}

// DoJSONWithOptions performs a JSON request and decodes the response with request options.
func (c *Client) DoJSONWithOptions(
	ctx context.Context,
	method, path string,
	reqBody, respBody any,
	opts RequestOptions,
) error {
	var body io.Reader

	if reqBody != nil {
		data, err := json.Marshal(reqBody)
		if err != nil {
			return fmt.Errorf("failed to marshal request: %w", err)
		}

		body = bytes.NewReader(data)
	}

	resp, err := c.DoWithOptions(ctx, method, path, body, "application/json", opts)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	respData, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("failed to read response: %w", err)
	}

	if resp.StatusCode >= httpErrorStatusThreshold {
		return newAPIError(resp.StatusCode, respData)
	}

	if respBody != nil && len(respData) > 0 {
		if err := json.Unmarshal(respData, respBody); err != nil {
			return fmt.Errorf("failed to unmarshal response: %w", err)
		}
	}

	return nil
}

// DoMultipart performs a multipart file upload.
func (c *Client) DoMultipart(
	ctx context.Context,
	path, fieldName, fileName string,
	fileContent []byte,
	formFields map[string]string,
) (*http.Response, []byte, error) {
	var buf bytes.Buffer
	writer := multipart.NewWriter(&buf)

	part, err := writer.CreateFormFile(fieldName, fileName)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create form file: %w", err)
	}

	if _, err := part.Write(fileContent); err != nil {
		return nil, nil, fmt.Errorf("failed to write file content: %w", err)
	}

	for key, val := range formFields {
		if err := writer.WriteField(key, val); err != nil {
			return nil, nil, fmt.Errorf("failed to write field %s: %w", key, err)
		}
	}

	if err := writer.Close(); err != nil {
		return nil, nil, fmt.Errorf("failed to close multipart writer: %w", err)
	}

	resp, err := c.Do(ctx, http.MethodPost, path, &buf, writer.FormDataContentType())
	if err != nil {
		return nil, nil, err
	}
	defer resp.Body.Close()

	respData, err := io.ReadAll(resp.Body)
	if err != nil {
		return resp, nil, fmt.Errorf("failed to read response: %w", err)
	}

	return resp, respData, nil
}

// DoRaw performs a request and returns raw response data.
func (c *Client) DoRaw(
	ctx context.Context,
	method, path string,
	body io.Reader,
	contentType string,
) (*http.Response, []byte, error) {
	resp, err := c.Do(ctx, method, path, body, contentType)
	if err != nil {
		return nil, nil, err
	}
	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return resp, nil, fmt.Errorf("failed to read response: %w", err)
	}

	if resp.StatusCode >= httpErrorStatusThreshold {
		return resp, data, newAPIError(resp.StatusCode, data)
	}

	return resp, data, nil
}

// APIError represents an HTTP error response.
type APIError struct {
	StatusCode int
	Body       []byte
	Parsed     *ErrorResponse
}

func (e *APIError) Error() string {
	if e == nil {
		return "API error <nil>"
	}

	if e.Parsed != nil {
		if e.Parsed.Code != "" {
			return fmt.Sprintf("API error %d (%s): %s", e.StatusCode, e.Parsed.Code, e.Parsed.Message)
		}

		return fmt.Sprintf("API error %d: %s", e.StatusCode, e.Parsed.Message)
	}

	return fmt.Sprintf("API error %d: %s", e.StatusCode, string(e.Body))
}

// ProductCode returns the parsed Matcher product code when available.
func (e *APIError) ProductCode() string {
	if e == nil || e.Parsed == nil {
		return ""
	}

	return e.Parsed.Code
}

// ProductTitle returns the parsed Matcher product title when available.
func (e *APIError) ProductTitle() string {
	if e == nil || e.Parsed == nil {
		return ""
	}

	return e.Parsed.Title
}

// ProductMessage returns the parsed Matcher product message when available.
func (e *APIError) ProductMessage() string {
	if e == nil || e.Parsed == nil {
		return ""
	}

	return e.Parsed.Message
}

// IsNotFound returns true if the error is a 404.
func (e *APIError) IsNotFound() bool {
	return e.StatusCode == http.StatusNotFound
}

// IsBadRequest returns true if the error is a 400.
func (e *APIError) IsBadRequest() bool {
	return e.StatusCode == http.StatusBadRequest
}

// IsConflict returns true if the error is a 409.
func (e *APIError) IsConflict() bool {
	return e.StatusCode == http.StatusConflict
}

func newAPIError(statusCode int, body []byte) *APIError {
	apiError := &APIError{
		StatusCode: statusCode,
		Body:       body,
	}

	var parsed ErrorResponse
	if err := json.Unmarshal(body, &parsed); err == nil && parsed.Code != "" {
		apiError.Parsed = &parsed
	}

	return apiError
}
