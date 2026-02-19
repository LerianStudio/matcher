// Package adapters provides infrastructure adapter implementations.
package adapters

import (
	"context"
	"errors"

	"github.com/LerianStudio/matcher/internal/shared/ports"
)

var errNilStaticConfigAdapter = errors.New("static configuration adapter is nil")

// StaticConfigurationAdapter returns the same configuration for all tenants.
// This is the default mode for single-tenant or environment-driven setups.
type StaticConfigurationAdapter struct {
	config ports.TenantConfig
}

// NewStaticConfigurationAdapter creates a new adapter with the given static config.
// A zero-value TenantConfig is valid and will be returned for all tenants.
func NewStaticConfigurationAdapter(cfg ports.TenantConfig) *StaticConfigurationAdapter {
	return &StaticConfigurationAdapter{config: cfg}
}

// GetTenantConfig returns the static configuration regardless of tenant ID.
func (sca *StaticConfigurationAdapter) GetTenantConfig(
	_ context.Context,
	_ string,
) (*ports.TenantConfig, error) {
	if sca == nil {
		return nil, errNilStaticConfigAdapter
	}

	cfg := sca.config

	return &cfg, nil
}
