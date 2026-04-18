package fetcher

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"go.opentelemetry.io/otel/attribute"

	libCommons "github.com/LerianStudio/lib-commons/v5/commons"
	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"

	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

// IsHealthy checks if the Fetcher service is reachable and healthy.
//
// Deliberately bypasses auth injection, retry, and the circuit breaker —
// intended for liveness probes that must not mask real connectivity issues
// (an auth outage or an open breaker would otherwise make a healthy upstream
// look down). Use doRequest / doRequestWithHeaders for authenticated calls
// that need the full retry + breaker pipeline.
func (client *HTTPFetcherClient) IsHealthy(ctx context.Context) bool {
	if err := client.ensureReady(); err != nil {
		return false
	}

	healthCtx, cancel := context.WithTimeout(ctx, client.cfg.HealthTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(healthCtx, http.MethodGet, client.baseURL+"/health", http.NoBody)
	if err != nil {
		return false
	}

	resp, err := client.httpClient.Do(req) // #nosec G704 -- URL comes from validated fetcher config, not user input
	if err != nil {
		return false
	}

	body, err := func() ([]byte, error) {
		defer func() {
			_ = resp.Body.Close()
		}()

		return readBoundedBody(resp.Body)
	}()

	if resp.StatusCode != http.StatusOK {
		return false
	}

	if err != nil {
		return false
	}

	if err := rejectEmptyOrNullBody(body); err != nil {
		return false
	}

	var health fetcherHealthResponse
	if err := json.Unmarshal(body, &health); err != nil {
		return false
	}

	return strings.EqualFold(health.Status, "ok") || strings.EqualFold(health.Status, "healthy")
}

// ListConnections retrieves all database connections managed by Fetcher, paginating
// through the full result set. Fetcher defaults to limit=50; we explicitly request
// listConnectionsPageSize (100) per page and keep fetching until the upstream returns
// a short or empty page.
//
// productName is sent as the X-Product-Name header — Fetcher's product-isolation
// mechanism (also referred to as "schema qualification" in related commit messages).
// It scopes the listing to connections belonging to the specified product.
func (client *HTTPFetcherClient) ListConnections(ctx context.Context, productName string) ([]*sharedPorts.FetcherConnection, error) {
	if err := client.ensureReady(); err != nil {
		return nil, err
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "fetcher.list_connections")
	defer span.End()

	var extraHeaders map[string]string
	if productName != "" {
		extraHeaders = map[string]string{"X-Product-Name": productName}
	}

	var result []*sharedPorts.FetcherConnection

	seenConnectionIDs := make(map[string]struct{})

	collected := false

	for page := 1; page <= maxPaginationPages; page++ {
		span.SetAttributes(attribute.Int("page.number", page))

		pageResp, err := client.fetchConnectionPage(ctx, page, extraHeaders)
		if err != nil {
			return nil, fmt.Errorf("list connections page %d: %w", page, err)
		}

		newConnectionsOnPage := 0

		for i := range pageResp.Items {
			if _, seen := seenConnectionIDs[pageResp.Items[i].ID]; seen {
				continue
			}

			seenConnectionIDs[pageResp.Items[i].ID] = struct{}{}
			newConnectionsOnPage++

			result = append(result, mapFetcherConnection(&pageResp.Items[i]))
		}

		if len(pageResp.Items) == listConnectionsPageSize && newConnectionsOnPage == 0 {
			return nil, fmt.Errorf("%w: page %d repeated without new connections", ErrFetcherPaginationOverflow, page)
		}

		// Treat the upstream total as advisory only. Stop when we observe a short
		// or empty page so inconsistent totals do not truncate discovery early.
		if len(pageResp.Items) < listConnectionsPageSize {
			collected = true

			break
		}
	}

	if !collected {
		return nil, fmt.Errorf("%w: fetched %d items over %d pages", ErrFetcherPaginationOverflow, len(result), maxPaginationPages)
	}

	logger.Log(ctx, libLog.LevelInfo, "listed fetcher connections across paginated pages", libLog.Int("count", len(result)))

	span.SetAttributes(attribute.Int("connections.total", len(result)))

	return result, nil
}

// fetchConnectionPage retrieves a single page of connections from Fetcher.
func (client *HTTPFetcherClient) fetchConnectionPage(ctx context.Context, page int, extraHeaders map[string]string) (*fetcherConnectionListResponse, error) {
	reqURL := client.baseURL + "/v1/management/connections?page=" + strconv.Itoa(page) + "&limit=" + strconv.Itoa(listConnectionsPageSize)

	body, err := client.doGetWithHeaders(ctx, reqURL, extraHeaders)
	if err != nil {
		return nil, err
	}

	if err := rejectEmptyOrNullBody(body); err != nil {
		return nil, err
	}

	var listResp fetcherConnectionListResponse
	if err := json.Unmarshal(body, &listResp); err != nil {
		return nil, fmt.Errorf("decode connections response: %w", err)
	}

	return &listResp, nil
}

// mapFetcherConnection converts a Fetcher API connection response to the shared port type.
func mapFetcherConnection(conn *fetcherConnectionResponse) *sharedPorts.FetcherConnection {
	fc := &sharedPorts.FetcherConnection{
		ID:           conn.ID,
		ConfigName:   conn.ConfigName,
		DatabaseType: conn.Type,
		Host:         conn.Host,
		Port:         conn.Port,
		Schema:       conn.Schema,
		DatabaseName: conn.DatabaseName,
		UserName:     conn.UserName,
		ProductName:  conn.ProductName,
		Metadata:     conn.Metadata,
		CreatedAt:    parseOptionalRFC3339(conn.CreatedAt),
		UpdatedAt:    parseOptionalRFC3339(conn.UpdatedAt),
	}

	return fc
}
