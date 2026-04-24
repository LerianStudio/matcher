// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package fetcher

import (
	"context"
	"fmt"
	"net/http"

	libCommons "github.com/LerianStudio/lib-commons/v5/commons"
	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"

	"github.com/LerianStudio/matcher/internal/auth"
)

// injectAuth adds per-tenant authentication to the request.
// When a tokenExchanger is available, M2M credentials are exchanged for a Bearer token.
// Otherwise, credentials are injected as BasicAuth (backward compat during migration).
// In single-tenant mode (m2mProvider is nil), this is a no-op.
func (client *HTTPFetcherClient) injectAuth(ctx context.Context, req *http.Request) error {
	if client.m2mProvider == nil {
		return nil
	}

	tenantOrgID := auth.GetTenantID(ctx)
	if tenantOrgID == "" {
		// No tenant in context — skip credential injection (may be health check or single-tenant request).
		return nil
	}

	creds, err := client.m2mProvider.GetCredentials(ctx, tenantOrgID)
	if err != nil {
		return fmt.Errorf("fetching M2M credentials for tenant %s: %w", tenantOrgID, err)
	}

	if creds == nil {
		return fmt.Errorf("tenant %s: %w", tenantOrgID, ErrFetcherNilCredentials)
	}

	// Prefer Bearer token via token exchange.
	if client.tokenExchanger != nil {
		token, tokenErr := client.tokenExchanger.GetToken(ctx, creds.ClientID, creds.ClientSecret)
		if tokenErr != nil {
			return fmt.Errorf("exchanging credentials for bearer token: %w", tokenErr)
		}

		// Register the tenant→clientID mapping for 401 recovery so that
		// InvalidateTokenByTenant can evict the right token without calling
		// GetCredentials again (which may itself be failing).
		client.tokenExchanger.RegisterTenantClient(tenantOrgID, creds.ClientID)

		req.Header.Set("Authorization", "Bearer "+token)

		return nil
	}

	// Fallback: BasicAuth (backward compat during migration).
	req.SetBasicAuth(creds.ClientID, creds.ClientSecret)

	return nil
}

// invalidateM2MOnUnauthorized invalidates cached credentials and Bearer tokens
// when a 401 response is received, forcing re-fetch on the next request.
// Redis eviction errors are logged but not propagated — the 401 itself is the
// primary error returned to the caller via classifyResponse.
func (client *HTTPFetcherClient) invalidateM2MOnUnauthorized(ctx context.Context, statusCode int) {
	if statusCode != http.StatusUnauthorized {
		return
	}

	tenantOrgID := auth.GetTenantID(ctx)
	if tenantOrgID == "" {
		return
	}

	// Invalidate Bearer token cache using the reverse tenant→clientID mapping.
	// This avoids calling GetCredentials during 401 recovery — if credentials
	// themselves are stale or the provider is failing, that call would either
	// return cached stale creds (useless) or error (silently swallowing the
	// token invalidation).
	if client.tokenExchanger != nil {
		client.tokenExchanger.InvalidateTokenByTenant(tenantOrgID)
	}

	// Invalidate M2M credential cache (L1 + L2).
	if client.m2mProvider == nil {
		return
	}

	if err := client.m2mProvider.InvalidateCredentials(ctx, tenantOrgID); err != nil {
		logger, _, _, _ := libCommons.NewTrackingFromContext(ctx)
		logger.With(
			libLog.String("tenant_org_id", tenantOrgID),
			libLog.Err(err),
		).Log(ctx, libLog.LevelWarn, "m2m credential invalidation failed on 401 recovery")
	}
}
