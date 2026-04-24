//nolint:perfsprint,varnamelen,wsl_v5 // Test configuration client favors concise path composition.
package client

import (
	"context"
	"fmt"
	"net/http"
)

// ConfigurationClient handles configuration API endpoints.
type ConfigurationClient struct {
	client *Client
}

// NewConfigurationClient creates a new configuration client.
func NewConfigurationClient(client *Client) *ConfigurationClient {
	return &ConfigurationClient{client: client}
}

// CreateContext creates a new reconciliation context.
func (c *ConfigurationClient) CreateContext(
	ctx context.Context,
	req CreateContextRequest,
) (*Context, error) {
	var resp Context
	err := c.client.DoJSON(ctx, http.MethodPost, "/v1/contexts", req, &resp)
	if err != nil {
		return nil, fmt.Errorf("create context: %w", err)
	}
	return &resp, nil
}

// GetContext retrieves a context by ID.
func (c *ConfigurationClient) GetContext(ctx context.Context, contextID string) (*Context, error) {
	var resp Context
	err := c.client.DoJSON(ctx, http.MethodGet, "/v1/contexts/"+contextID, nil, &resp)
	if err != nil {
		return nil, fmt.Errorf("get context: %w", err)
	}
	return &resp, nil
}

// ListContexts retrieves all contexts.
func (c *ConfigurationClient) ListContexts(ctx context.Context) ([]Context, error) {
	var resp struct {
		Items []Context `json:"items"`
	}
	err := c.client.DoJSON(ctx, http.MethodGet, "/v1/contexts", nil, &resp)
	if err != nil {
		return nil, fmt.Errorf("list contexts: %w", err)
	}
	return resp.Items, nil
}

// UpdateContext updates an existing context.
func (c *ConfigurationClient) UpdateContext(
	ctx context.Context,
	contextID string,
	req UpdateContextRequest,
) (*Context, error) {
	var resp Context
	err := c.client.DoJSON(ctx, http.MethodPatch, "/v1/contexts/"+contextID, req, &resp)
	if err != nil {
		return nil, fmt.Errorf("update context: %w", err)
	}
	return &resp, nil
}

// DeleteContext deletes a context.
func (c *ConfigurationClient) DeleteContext(ctx context.Context, contextID string) error {
	err := c.client.DoJSON(ctx, http.MethodDelete, "/v1/contexts/"+contextID, nil, nil)
	if err != nil {
		return fmt.Errorf("delete context: %w", err)
	}
	return nil
}

// CreateSource creates a new source within a context.
func (c *ConfigurationClient) CreateSource(
	ctx context.Context,
	contextID string,
	req CreateSourceRequest,
) (*Source, error) {
	var resp Source
	path := fmt.Sprintf("/v1/contexts/%s/sources", contextID)
	err := c.client.DoJSON(ctx, http.MethodPost, path, req, &resp)
	if err != nil {
		return nil, fmt.Errorf("create source: %w", err)
	}
	return &resp, nil
}

// GetSource retrieves a source by ID.
func (c *ConfigurationClient) GetSource(
	ctx context.Context,
	contextID, sourceID string,
) (*Source, error) {
	var resp Source
	path := fmt.Sprintf("/v1/contexts/%s/sources/%s", contextID, sourceID)
	err := c.client.DoJSON(ctx, http.MethodGet, path, nil, &resp)
	if err != nil {
		return nil, fmt.Errorf("get source: %w", err)
	}
	return &resp, nil
}

// ListSources retrieves all sources for a context.
func (c *ConfigurationClient) ListSources(ctx context.Context, contextID string) ([]Source, error) {
	var resp struct {
		Items []Source `json:"items"`
	}
	path := fmt.Sprintf("/v1/contexts/%s/sources", contextID)
	err := c.client.DoJSON(ctx, http.MethodGet, path, nil, &resp)
	if err != nil {
		return nil, fmt.Errorf("list sources: %w", err)
	}
	return resp.Items, nil
}

// UpdateSource updates an existing source.
func (c *ConfigurationClient) UpdateSource(
	ctx context.Context,
	contextID, sourceID string,
	req UpdateSourceRequest,
) (*Source, error) {
	var resp Source
	path := fmt.Sprintf("/v1/contexts/%s/sources/%s", contextID, sourceID)
	err := c.client.DoJSON(ctx, http.MethodPatch, path, req, &resp)
	if err != nil {
		return nil, fmt.Errorf("update source: %w", err)
	}
	return &resp, nil
}

// DeleteSource deletes a source.
func (c *ConfigurationClient) DeleteSource(ctx context.Context, contextID, sourceID string) error {
	path := fmt.Sprintf("/v1/contexts/%s/sources/%s", contextID, sourceID)
	err := c.client.DoJSON(ctx, http.MethodDelete, path, nil, nil)
	if err != nil {
		return fmt.Errorf("delete source: %w", err)
	}
	return nil
}

// CreateFieldMap creates a new field map for a source.
func (c *ConfigurationClient) CreateFieldMap(
	ctx context.Context,
	contextID, sourceID string,
	req CreateFieldMapRequest,
) (*FieldMap, error) {
	var resp FieldMap
	path := fmt.Sprintf("/v1/contexts/%s/sources/%s/field-maps", contextID, sourceID)
	err := c.client.DoJSON(ctx, http.MethodPost, path, req, &resp)
	if err != nil {
		return nil, fmt.Errorf("create field map: %w", err)
	}
	return &resp, nil
}

// GetFieldMapBySource retrieves the field map for a source.
func (c *ConfigurationClient) GetFieldMapBySource(
	ctx context.Context,
	contextID, sourceID string,
) (*FieldMap, error) {
	var resp FieldMap
	path := fmt.Sprintf("/v1/contexts/%s/sources/%s/field-maps", contextID, sourceID)
	err := c.client.DoJSON(ctx, http.MethodGet, path, nil, &resp)
	if err != nil {
		return nil, fmt.Errorf("get field map: %w", err)
	}
	return &resp, nil
}

// UpdateFieldMap updates an existing field map.
func (c *ConfigurationClient) UpdateFieldMap(
	ctx context.Context,
	fieldMapID string,
	req UpdateFieldMapRequest,
) (*FieldMap, error) {
	var resp FieldMap
	path := fmt.Sprintf("/v1/field-maps/%s", fieldMapID)
	err := c.client.DoJSON(ctx, http.MethodPatch, path, req, &resp)
	if err != nil {
		return nil, fmt.Errorf("update field map: %w", err)
	}
	return &resp, nil
}

// DeleteFieldMap deletes a field map.
func (c *ConfigurationClient) DeleteFieldMap(ctx context.Context, fieldMapID string) error {
	path := fmt.Sprintf("/v1/field-maps/%s", fieldMapID)
	err := c.client.DoJSON(ctx, http.MethodDelete, path, nil, nil)
	if err != nil {
		return fmt.Errorf("delete field map: %w", err)
	}
	return nil
}

// CreateFeeRule creates a new fee rule within a context.
func (c *ConfigurationClient) CreateFeeRule(
	ctx context.Context,
	contextID string,
	req CreateFeeRuleRequest,
) (*FeeRuleResponse, error) {
	var resp FeeRuleResponse
	path := fmt.Sprintf("/v1/config/contexts/%s/fee-rules", contextID)
	err := c.client.DoJSON(ctx, http.MethodPost, path, req, &resp)
	if err != nil {
		return nil, fmt.Errorf("create fee rule: %w", err)
	}

	return &resp, nil
}

// GetFeeRule retrieves a fee rule by ID.
func (c *ConfigurationClient) GetFeeRule(ctx context.Context, feeRuleID string) (*FeeRuleResponse, error) {
	var resp FeeRuleResponse
	path := fmt.Sprintf("/v1/config/fee-rules/%s", feeRuleID)
	err := c.client.DoJSON(ctx, http.MethodGet, path, nil, &resp)
	if err != nil {
		return nil, fmt.Errorf("get fee rule: %w", err)
	}

	return &resp, nil
}

// ListFeeRules retrieves all fee rules for a context.
func (c *ConfigurationClient) ListFeeRules(ctx context.Context, contextID string) ([]FeeRuleResponse, error) {
	var resp []FeeRuleResponse
	path := fmt.Sprintf("/v1/config/contexts/%s/fee-rules", contextID)
	err := c.client.DoJSON(ctx, http.MethodGet, path, nil, &resp)
	if err != nil {
		return nil, fmt.Errorf("list fee rules: %w", err)
	}

	return resp, nil
}

// UpdateFeeRule updates an existing fee rule.
func (c *ConfigurationClient) UpdateFeeRule(
	ctx context.Context,
	feeRuleID string,
	req UpdateFeeRuleRequest,
) (*FeeRuleResponse, error) {
	var resp FeeRuleResponse
	path := fmt.Sprintf("/v1/config/fee-rules/%s", feeRuleID)
	err := c.client.DoJSON(ctx, http.MethodPatch, path, req, &resp)
	if err != nil {
		return nil, fmt.Errorf("update fee rule: %w", err)
	}

	return &resp, nil
}

// DeleteFeeRule deletes a fee rule.
func (c *ConfigurationClient) DeleteFeeRule(ctx context.Context, feeRuleID string) error {
	path := fmt.Sprintf("/v1/config/fee-rules/%s", feeRuleID)
	err := c.client.DoJSON(ctx, http.MethodDelete, path, nil, nil)
	if err != nil {
		return fmt.Errorf("delete fee rule: %w", err)
	}

	return nil
}

// CreateMatchRule creates a new match rule.
func (c *ConfigurationClient) CreateMatchRule(
	ctx context.Context,
	contextID string,
	req CreateMatchRuleRequest,
) (*MatchRule, error) {
	var resp MatchRule
	path := fmt.Sprintf("/v1/contexts/%s/rules", contextID)
	err := c.client.DoJSON(ctx, http.MethodPost, path, req, &resp)
	if err != nil {
		return nil, fmt.Errorf("create match rule: %w", err)
	}
	return &resp, nil
}

// GetMatchRule retrieves a match rule by ID.
func (c *ConfigurationClient) GetMatchRule(
	ctx context.Context,
	contextID, ruleID string,
) (*MatchRule, error) {
	var resp MatchRule
	path := fmt.Sprintf("/v1/contexts/%s/rules/%s", contextID, ruleID)
	err := c.client.DoJSON(ctx, http.MethodGet, path, nil, &resp)
	if err != nil {
		return nil, fmt.Errorf("get match rule: %w", err)
	}
	return &resp, nil
}

// ListMatchRules retrieves all match rules for a context.
func (c *ConfigurationClient) ListMatchRules(
	ctx context.Context,
	contextID string,
) ([]MatchRule, error) {
	var resp struct {
		Items []MatchRule `json:"items"`
	}
	path := fmt.Sprintf("/v1/contexts/%s/rules", contextID)
	err := c.client.DoJSON(ctx, http.MethodGet, path, nil, &resp)
	if err != nil {
		return nil, fmt.Errorf("list match rules: %w", err)
	}
	return resp.Items, nil
}

// UpdateMatchRule updates an existing match rule.
func (c *ConfigurationClient) UpdateMatchRule(
	ctx context.Context,
	contextID, ruleID string,
	req UpdateMatchRuleRequest,
) (*MatchRule, error) {
	var resp MatchRule
	path := fmt.Sprintf("/v1/contexts/%s/rules/%s", contextID, ruleID)
	err := c.client.DoJSON(ctx, http.MethodPatch, path, req, &resp)
	if err != nil {
		return nil, fmt.Errorf("update match rule: %w", err)
	}
	return &resp, nil
}

// DeleteMatchRule deletes a match rule.
func (c *ConfigurationClient) DeleteMatchRule(ctx context.Context, contextID, ruleID string) error {
	path := fmt.Sprintf("/v1/contexts/%s/rules/%s", contextID, ruleID)
	err := c.client.DoJSON(ctx, http.MethodDelete, path, nil, nil)
	if err != nil {
		return fmt.Errorf("delete match rule: %w", err)
	}
	return nil
}

// ReorderMatchRules reorders match rules by priority.
func (c *ConfigurationClient) ReorderMatchRules(
	ctx context.Context,
	contextID string,
	req ReorderMatchRulesRequest,
) error {
	path := fmt.Sprintf("/v1/contexts/%s/rules/reorder", contextID)
	err := c.client.DoJSON(ctx, http.MethodPost, path, req, nil)
	if err != nil {
		return fmt.Errorf("reorder match rules: %w", err)
	}
	return nil
}

// CloneContext clones a context with its configuration.
func (c *ConfigurationClient) CloneContext(
	ctx context.Context,
	contextID string,
	req CloneContextRequest,
) (*CloneContextResponse, error) {
	var resp CloneContextResponse
	path := fmt.Sprintf("/v1/contexts/%s/clone", contextID)
	err := c.client.DoJSON(ctx, http.MethodPost, path, req, &resp)
	if err != nil {
		return nil, fmt.Errorf("clone context: %w", err)
	}
	return &resp, nil
}

// CreateSchedule creates a reconciliation schedule for a context.
func (c *ConfigurationClient) CreateSchedule(
	ctx context.Context,
	contextID string,
	req CreateScheduleRequest,
) (*ScheduleResponse, error) {
	var resp ScheduleResponse
	path := fmt.Sprintf("/v1/contexts/%s/schedules", contextID)
	err := c.client.DoJSON(ctx, http.MethodPost, path, req, &resp)
	if err != nil {
		return nil, fmt.Errorf("create schedule: %w", err)
	}
	return &resp, nil
}

// GetSchedule retrieves a schedule by ID.
func (c *ConfigurationClient) GetSchedule(
	ctx context.Context,
	contextID, scheduleID string,
) (*ScheduleResponse, error) {
	var resp ScheduleResponse
	path := fmt.Sprintf("/v1/contexts/%s/schedules/%s", contextID, scheduleID)
	err := c.client.DoJSON(ctx, http.MethodGet, path, nil, &resp)
	if err != nil {
		return nil, fmt.Errorf("get schedule: %w", err)
	}
	return &resp, nil
}

// ListSchedules retrieves all schedules for a context.
func (c *ConfigurationClient) ListSchedules(
	ctx context.Context,
	contextID string,
) ([]ScheduleResponse, error) {
	var resp []ScheduleResponse
	path := fmt.Sprintf("/v1/contexts/%s/schedules", contextID)
	err := c.client.DoJSON(ctx, http.MethodGet, path, nil, &resp)
	if err != nil {
		return nil, fmt.Errorf("list schedules: %w", err)
	}
	return resp, nil
}

// UpdateSchedule updates an existing schedule.
func (c *ConfigurationClient) UpdateSchedule(
	ctx context.Context,
	contextID, scheduleID string,
	req UpdateScheduleRequest,
) (*ScheduleResponse, error) {
	var resp ScheduleResponse
	path := fmt.Sprintf("/v1/contexts/%s/schedules/%s", contextID, scheduleID)
	err := c.client.DoJSON(ctx, http.MethodPatch, path, req, &resp)
	if err != nil {
		return nil, fmt.Errorf("update schedule: %w", err)
	}
	return &resp, nil
}

// DeleteSchedule deletes a schedule.
func (c *ConfigurationClient) DeleteSchedule(
	ctx context.Context,
	contextID, scheduleID string,
) error {
	path := fmt.Sprintf("/v1/contexts/%s/schedules/%s", contextID, scheduleID)
	err := c.client.DoJSON(ctx, http.MethodDelete, path, nil, nil)
	if err != nil {
		return fmt.Errorf("delete schedule: %w", err)
	}
	return nil
}
