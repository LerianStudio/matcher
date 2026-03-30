//go:build e2e

package e2e

import (
	"fmt"
	"strings"

	"github.com/LerianStudio/matcher/tests/e2e/client"
)

// Client wraps all API clients for e2e tests.
type Client struct {
	base *client.Client

	Configuration *client.ConfigurationClient
	FeeSchedule   *client.FeeScheduleClient
	Ingestion     *client.IngestionClient
	Matching      *client.MatchingClient
	Reporting     *client.ReportingClient
	Governance    *client.GovernanceClient
	Exception     *client.ExceptionClient
	Discovery     *client.DiscoveryClient
}

// NewClient creates a new unified API client.
func NewClient(cfg *E2EConfig) (*Client, error) {
	if cfg == nil {
		return nil, fmt.Errorf("config is required")
	}
	if strings.TrimSpace(cfg.AppBaseURL) == "" {
		return nil, fmt.Errorf("app base url is required")
	}
	if strings.TrimSpace(cfg.DefaultTenantID) == "" {
		return nil, fmt.Errorf("default tenant id is required")
	}
	if cfg.RequestTimeout <= 0 {
		return nil, fmt.Errorf("request timeout must be positive")
	}

	base := client.NewClient(cfg.AppBaseURL, cfg.DefaultTenantID, cfg.RequestTimeout)

	return &Client{
		base:          base,
		Configuration: client.NewConfigurationClient(base),
		FeeSchedule:   client.NewFeeScheduleClient(base),
		Ingestion:     client.NewIngestionClient(base),
		Matching:      client.NewMatchingClient(base),
		Reporting:     client.NewReportingClient(base),
		Governance:    client.NewGovernanceClient(base),
		Exception:     client.NewExceptionClient(base),
		Discovery:     client.NewDiscoveryClient(base),
	}, nil
}

// SetTenantID updates the tenant ID for all subsequent requests.
func (c *Client) SetTenantID(tenantID string) {
	c.base.SetTenantID(tenantID)
}

// TenantID exposes the current tenant ID.
func (c *Client) TenantID() string {
	return c.base.TenantID()
}
