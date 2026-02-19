//go:build e2e

package client

import (
	"context"
	"fmt"
	"net/http"
)

// FeeScheduleClient handles fee schedule API endpoints.
type FeeScheduleClient struct {
	client *Client
}

// NewFeeScheduleClient creates a new fee schedule client.
func NewFeeScheduleClient(client *Client) *FeeScheduleClient {
	return &FeeScheduleClient{client: client}
}

// CreateFeeSchedule creates a new fee schedule.
func (c *FeeScheduleClient) CreateFeeSchedule(
	ctx context.Context,
	req CreateFeeScheduleRequest,
) (*FeeScheduleResponse, error) {
	var resp FeeScheduleResponse
	err := c.client.DoJSON(ctx, http.MethodPost, "/v1/config/fee-schedules", req, &resp)
	if err != nil {
		return nil, fmt.Errorf("create fee schedule: %w", err)
	}
	return &resp, nil
}

// ListFeeSchedules retrieves all fee schedules.
func (c *FeeScheduleClient) ListFeeSchedules(ctx context.Context) ([]FeeScheduleResponse, error) {
	var resp []FeeScheduleResponse
	err := c.client.DoJSON(ctx, http.MethodGet, "/v1/config/fee-schedules", nil, &resp)
	if err != nil {
		return nil, fmt.Errorf("list fee schedules: %w", err)
	}
	return resp, nil
}

// GetFeeSchedule retrieves a fee schedule by ID.
func (c *FeeScheduleClient) GetFeeSchedule(ctx context.Context, scheduleID string) (*FeeScheduleResponse, error) {
	var resp FeeScheduleResponse
	path := fmt.Sprintf("/v1/config/fee-schedules/%s", scheduleID)
	err := c.client.DoJSON(ctx, http.MethodGet, path, nil, &resp)
	if err != nil {
		return nil, fmt.Errorf("get fee schedule: %w", err)
	}
	return &resp, nil
}

// UpdateFeeSchedule updates an existing fee schedule.
func (c *FeeScheduleClient) UpdateFeeSchedule(
	ctx context.Context,
	scheduleID string,
	req UpdateFeeScheduleRequest,
) (*FeeScheduleResponse, error) {
	var resp FeeScheduleResponse
	path := fmt.Sprintf("/v1/config/fee-schedules/%s", scheduleID)
	err := c.client.DoJSON(ctx, http.MethodPatch, path, req, &resp)
	if err != nil {
		return nil, fmt.Errorf("update fee schedule: %w", err)
	}
	return &resp, nil
}

// DeleteFeeSchedule deletes a fee schedule.
func (c *FeeScheduleClient) DeleteFeeSchedule(ctx context.Context, scheduleID string) error {
	path := fmt.Sprintf("/v1/config/fee-schedules/%s", scheduleID)
	err := c.client.DoJSON(ctx, http.MethodDelete, path, nil, nil)
	if err != nil {
		return fmt.Errorf("delete fee schedule: %w", err)
	}
	return nil
}

// SimulateFeeSchedule simulates fee calculation for a fee schedule.
func (c *FeeScheduleClient) SimulateFeeSchedule(
	ctx context.Context,
	scheduleID string,
	req SimulateFeeRequest,
) (*SimulateFeeResponse, error) {
	var resp SimulateFeeResponse
	path := fmt.Sprintf("/v1/config/fee-schedules/%s/simulate", scheduleID)
	err := c.client.DoJSON(ctx, http.MethodPost, path, req, &resp)
	if err != nil {
		return nil, fmt.Errorf("simulate fee schedule: %w", err)
	}
	return &resp, nil
}
