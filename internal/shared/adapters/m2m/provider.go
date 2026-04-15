// Package m2m provides a two-level cached M2M credential provider for
// per-tenant service-to-service authentication via AWS Secrets Manager.
//
// Cache Architecture:
//
//	L1 (in-memory sync.Map) → 30s fixed TTL, avoids Redis round-trip per request
//	L2 (Redis/Valkey)       → configurable TTL, shared across pods
//	Source (AWS Secrets Manager) → authoritative, called only on full cache miss
package m2m

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	libRedis "github.com/LerianStudio/lib-commons/v4/commons/redis"
	"github.com/LerianStudio/lib-commons/v4/commons/tenant-manager/valkey"

	"github.com/LerianStudio/matcher/internal/shared/ports"
)

// l1CacheTTL is the fixed in-memory cache TTL. Not configurable via env var —
// this is an internal implementation detail (fast path optimization).
const l1CacheTTL = 30 * time.Second

// defaultCredCacheTTL is the fallback L2 cache TTL when none is provided.
const defaultCredCacheTTL = 5 * time.Minute

// Sentinel errors for M2M credential retrieval.
var (
	ErrM2MClientNil         = errors.New("M2M secrets client is nil")
	ErrM2MTenantIDRequired  = errors.New("tenant org ID is required")
	ErrM2MSecretsClientNil  = errors.New("secrets client is required")
	ErrM2MEnvRequired       = errors.New("env is required")
	ErrM2MAppNameRequired   = errors.New("application name is required")
	ErrM2MTargetSvcRequired = errors.New("target service is required")
)

// SecretsClient abstracts the secret store backend (AWS Secrets Manager or mock).
// The method signature follows the lib-commons secretsmanager convention.
type SecretsClient interface {
	// GetM2MCredentials retrieves M2M credentials from the secret store.
	// Path: tenants/{env}/{tenantOrgID}/{applicationName}/m2m/{targetService}/credentials
	GetM2MCredentials(
		ctx context.Context,
		env, tenantOrgID, applicationName, targetService string,
	) (*ports.M2MCredentials, error)
}

// cachedCredentials wraps credentials with an expiration timestamp for L1.
type cachedCredentials struct {
	creds     *ports.M2MCredentials
	expiresAt time.Time
}

// redisCredentials is used for Redis cache serialization.
// M2MCredentials uses json:"-" to prevent API exposure, so we need
// a separate type for cache storage.
type redisCredentials struct {
	ClientID     string `json:"clientId"`
	ClientSecret string `json:"clientSecret"`
}

// Compile-time interface check.
var _ ports.M2MProvider = (*M2MCredentialProvider)(nil)

// M2MCredentialProvider handles per-tenant M2M credential retrieval with two-level caching.
// L1 (in-memory) provides fast path; L2 (Redis/Valkey via lib-commons) provides cross-pod consistency.
// Token acquisition is handled by the caller — this only provides credentials.
type M2MCredentialProvider struct {
	smClient        SecretsClient
	env             string
	applicationName string
	targetService   string
	credCacheTTL    time.Duration // L2 TTL (service-defined)

	credCache sync.Map // L1: map[tenantOrgID]*cachedCredentials

	// L2: lib-commons Redis client (nil = local-only mode)
	redisClient *libRedis.Client
}

// NewM2MCredentialProvider creates a credential provider with two-level cache.
// Pass nil for redisClient to use local-only mode (single-tenant or dev).
// Returns an error if required parameters are missing.
func NewM2MCredentialProvider(
	smClient SecretsClient,
	env, applicationName, targetService string,
	credCacheTTL time.Duration,
	redisClient *libRedis.Client,
) (*M2MCredentialProvider, error) {
	if smClient == nil {
		return nil, ErrM2MSecretsClientNil
	}

	if env == "" {
		return nil, ErrM2MEnvRequired
	}

	if applicationName == "" {
		return nil, ErrM2MAppNameRequired
	}

	if targetService == "" {
		return nil, ErrM2MTargetSvcRequired
	}

	if credCacheTTL <= 0 {
		credCacheTTL = defaultCredCacheTTL
	}

	return &M2MCredentialProvider{
		smClient:        smClient,
		env:             env,
		applicationName: applicationName,
		targetService:   targetService,
		credCacheTTL:    credCacheTTL,
		redisClient:     redisClient,
	}, nil
}

// m2mRedisKey returns the tenant-prefixed Redis key for M2M credentials.
// Uses valkey.GetKeyContext to apply tenant prefix consistently with all other
// Redis operations in the codebase. The tenantOrgID is included in the base key
// to prevent cross-tenant cache collisions at the L2 (Redis) layer.
func (provider *M2MCredentialProvider) m2mRedisKey(ctx context.Context, tenantOrgID string) (string, error) {
	baseKey := fmt.Sprintf("m2m:%s:%s:credentials", provider.targetService, tenantOrgID)

	key, err := valkey.GetKeyContext(ctx, baseKey)
	if err != nil {
		return "", fmt.Errorf("build tenant-prefixed M2M redis key: %w", err)
	}

	return key, nil
}

// GetCredentials returns M2M credentials for the given tenant using two-level cache.
// Lookup order: L1 (memory) → L2 (Redis via lib-commons) → AWS Secrets Manager.
// The caller (Fetcher client integration) handles credential injection.
func (provider *M2MCredentialProvider) GetCredentials(ctx context.Context, tenantOrgID string) (*ports.M2MCredentials, error) {
	if provider.smClient == nil {
		return nil, ErrM2MClientNil
	}

	if tenantOrgID == "" {
		return nil, ErrM2MTenantIDRequired
	}

	// L1: Check in-memory cache (fast path)
	if cached, ok := provider.credCache.Load(tenantOrgID); ok {
		cc, valid := cached.(*cachedCredentials)
		if valid && time.Now().UTC().Before(cc.expiresAt) {
			return cc.creds, nil
		}
	}

	// L2: Check distributed cache (Redis/Valkey via lib-commons)
	if provider.redisClient != nil {
		creds, found := provider.getFromRedis(ctx, tenantOrgID)
		if found {
			return creds, nil
		}
	}

	// Source: Fetch from AWS Secrets Manager (authoritative source)
	creds, err := provider.smClient.GetM2MCredentials(ctx, provider.env, tenantOrgID, provider.applicationName, provider.targetService)
	if err != nil {
		return nil, fmt.Errorf("fetching M2M credentials for tenant %s: %w", tenantOrgID, err)
	}

	// Store in L2 (distributed via lib-commons)
	provider.storeInRedis(ctx, tenantOrgID, creds)

	// Store in L1 (local)
	provider.storeInL1(tenantOrgID, creds)

	return creds, nil
}

// InvalidateCredentials removes cached credentials for a tenant from both cache levels.
// Call this when a 401 is received during token exchange (credential revocation).
func (provider *M2MCredentialProvider) InvalidateCredentials(ctx context.Context, tenantOrgID string) error {
	// Delete from L1 (local — immediate effect)
	provider.credCache.Delete(tenantOrgID)

	// Delete from L2 (distributed — propagates to all pods via lib-commons)
	if provider.redisClient == nil {
		return nil
	}

	rds, err := provider.redisClient.GetClient(ctx)
	if err != nil {
		return fmt.Errorf("get Redis client for credential invalidation: %w", err)
	}

	if rds == nil {
		return nil
	}

	key, keyErr := provider.m2mRedisKey(ctx, tenantOrgID)
	if keyErr != nil {
		return fmt.Errorf("build Redis key for credential invalidation: %w", keyErr)
	}

	if delErr := rds.Del(ctx, key).Err(); delErr != nil {
		return fmt.Errorf("delete M2M credentials from Redis for tenant %s: %w", tenantOrgID, delErr)
	}

	return nil
}

// storeInL1 stores credentials in the in-memory L1 cache with fixed TTL.
func (provider *M2MCredentialProvider) storeInL1(tenantOrgID string, creds *ports.M2MCredentials) {
	provider.credCache.Store(tenantOrgID, &cachedCredentials{
		creds:     creds,
		expiresAt: time.Now().UTC().Add(l1CacheTTL),
	})
}

// getFromRedis attempts to read credentials from the Redis L2 cache.
// Returns (creds, true) on hit, (nil, false) on miss or error.
func (provider *M2MCredentialProvider) getFromRedis(ctx context.Context, tenantOrgID string) (*ports.M2MCredentials, bool) {
	rds, err := provider.redisClient.GetClient(ctx)
	if err != nil || rds == nil {
		return nil, false
	}

	key, keyErr := provider.m2mRedisKey(ctx, tenantOrgID)
	if keyErr != nil {
		return nil, false
	}

	val, getErr := rds.Get(ctx, key).Bytes()
	if getErr != nil {
		return nil, false
	}

	var rc redisCredentials
	if json.Unmarshal(val, &rc) != nil {
		return nil, false
	}

	creds := &ports.M2MCredentials{
		ClientID:     rc.ClientID,
		ClientSecret: rc.ClientSecret,
	}

	// Populate L1 with short TTL
	provider.storeInL1(tenantOrgID, creds)

	return creds, true
}

// storeInRedis stores credentials in the Redis L2 cache.
// Errors are silently ignored (Redis is best-effort for caching).
func (provider *M2MCredentialProvider) storeInRedis(ctx context.Context, tenantOrgID string, creds *ports.M2MCredentials) {
	if creds == nil {
		return
	}

	if provider.redisClient == nil {
		return
	}

	rds, err := provider.redisClient.GetClient(ctx)
	if err != nil || rds == nil {
		return
	}

	key, keyErr := provider.m2mRedisKey(ctx, tenantOrgID)
	if keyErr != nil {
		return
	}

	rc := redisCredentials{
		ClientID:     creds.ClientID,
		ClientSecret: creds.ClientSecret,
	}

	data, marshalErr := json.Marshal(rc) // #nosec G117 -- intentional: serializing M2M credentials for Redis L2 cache storage //nolint:gosec
	if marshalErr != nil {
		return
	}

	// TODO(SECURITY: M2M-SEC-03): Redis L2 cache stores credentials as plaintext JSON.
	// Planned remediation: encrypt with AES-256-GCM using a key derived via HKDF
	// from SYSTEMPLANE_SECRET_MASTER_KEY. Current mitigations:
	//   - TTL expiry (5 min default) narrows exposure window
	//   - Network isolation (Redis on internal network only)
	//   - json:"-" tags on M2MCredentials prevent accidental API serialization
	// Must be addressed before multi-region deployment or any deployment with shared Redis.
	_ = rds.Set(ctx, key, data, provider.credCacheTTL).Err()
}
