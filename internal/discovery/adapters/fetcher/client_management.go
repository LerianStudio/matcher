// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package fetcher

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"time"

	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

// GetSchema retrieves the schema (tables and columns) for a specific connection.
func (client *HTTPFetcherClient) GetSchema(ctx context.Context, connectionID string) (*sharedPorts.FetcherSchema, error) {
	if err := client.ensureReady(); err != nil {
		return nil, err
	}

	reqURL := client.baseURL + "/v1/management/connections/" + url.PathEscape(connectionID) + "/schema"

	body, err := client.doGet(ctx, reqURL)
	if err != nil {
		return nil, fmt.Errorf("get schema: %w", err)
	}

	var schemaResp fetcherSchemaResponse

	if err := rejectEmptyOrNullBody(body); err != nil {
		return nil, err
	}

	if err := json.Unmarshal(body, &schemaResp); err != nil {
		return nil, fmt.Errorf("decode schema response: %w", err)
	}

	if err := validateFetcherResourceID("connection", connectionID, schemaResp.ID); err != nil {
		return nil, err
	}

	tables := make([]sharedPorts.FetcherTableSchema, 0, len(schemaResp.Tables))
	for _, table := range schemaResp.Tables {
		tables = append(tables, sharedPorts.FetcherTableSchema{
			Name:   table.Name,
			Fields: table.Fields,
		})
	}

	return &sharedPorts.FetcherSchema{
		ID:           schemaResp.ID,
		ConfigName:   schemaResp.ConfigName,
		DatabaseName: schemaResp.DatabaseName,
		Type:         schemaResp.Type,
		Tables:       tables,
		DiscoveredAt: time.Now().UTC(),
	}, nil
}

// TestConnection tests connectivity for a specific Fetcher connection.
func (client *HTTPFetcherClient) TestConnection(ctx context.Context, connectionID string) (*sharedPorts.FetcherTestResult, error) {
	if err := client.ensureReady(); err != nil {
		return nil, err
	}

	reqURL := client.baseURL + "/v1/management/connections/" + url.PathEscape(connectionID) + "/test"

	body, err := client.doPost(ctx, reqURL, nil)
	if err != nil {
		return nil, fmt.Errorf("test connection: %w", err)
	}

	var testResp fetcherTestResponse

	if err := rejectEmptyOrNullBody(body); err != nil {
		return nil, err
	}

	if err := json.Unmarshal(body, &testResp); err != nil {
		return nil, fmt.Errorf("decode test response: %w", err)
	}

	return &sharedPorts.FetcherTestResult{
		Status:    testResp.Status,
		Message:   testResp.Message,
		LatencyMs: testResp.LatencyMs,
	}, nil
}
