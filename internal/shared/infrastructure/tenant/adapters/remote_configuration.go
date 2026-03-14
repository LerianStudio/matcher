// Package adapters provides tenant-aware infrastructure adapter implementations.
package adapters

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"reflect"
	"strings"
	"time"

	libCommons "github.com/LerianStudio/lib-commons/v4/commons"
	"github.com/LerianStudio/lib-commons/v4/commons/circuitbreaker"
	libLog "github.com/LerianStudio/lib-commons/v4/commons/log"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v4/commons/opentelemetry"

	"github.com/LerianStudio/matcher/internal/shared/ports"
)

const (
	tenantManagerCircuitBreakerName    = "tenant-manager"
	defaultRemoteConfigTimeout         = 10 * time.Second
	defaultRemoteConsecutiveFailures   = 5
	defaultRemoteCircuitBreakerTimeout = 30 * time.Second
)

var (
	errRemoteConfigAdapterNil       = errors.New("remote configuration adapter is nil")
	errTenantManagerURLRequired     = errors.New("tenant manager url is required")
	errTenantManagerServiceEmpty    = errors.New("tenant manager service name is required")
	errTenantManagerAPIKeyEmpty     = errors.New("tenant manager service api key is required")
	errTenantIDRequired             = errors.New("tenant id is required")
	errNilHTTPClient                = errors.New("http client is required")
	errUnsafeTenantManagerTransport = errors.New("tenant manager requires https for non-local production transport")
	errTenantSettingsRequestFailed  = errors.New("tenant settings request failed")
	errUnexpectedTenantConfigType   = errors.New("unexpected tenant config result type")
	errTenantConfigPayloadMissing   = errors.New("tenant settings response did not include a tenant config payload")
)

// RemoteConfigurationConfig configures the tenant-aware HTTP settings adapter.
type RemoteConfigurationConfig struct {
	BaseURL            string
	ServiceName        string
	ServiceAPIKey      string
	RequestTimeout     time.Duration
	BreakerConfig      circuitbreaker.Config
	Logger             libLog.Logger
	HTTPClient         *http.Client
	EnvironmentName    string
	RuntimeEnvironment string
}

// RemoteConfigurationAdapter resolves tenant infrastructure settings from the
// external tenant settings service.
type RemoteConfigurationAdapter struct {
	baseURL         string
	serviceName     string
	apiKey          string
	environmentName string
	httpClient      *http.Client
	breaker         circuitbreaker.Manager
}

type tenantConfigEnvelope struct {
	Config       *ports.TenantConfig `json:"config"`
	TenantConfig *ports.TenantConfig `json:"tenantConfig"`
	Settings     *ports.TenantConfig `json:"settings"`
}

// NewRemoteConfigurationAdapter creates a tenant settings adapter backed by
// stdlib HTTP and lib-commons circuit breaker primitives.
func NewRemoteConfigurationAdapter(cfg RemoteConfigurationConfig) (*RemoteConfigurationAdapter, error) {
	baseURL := strings.TrimSpace(cfg.BaseURL)
	if baseURL == "" {
		return nil, errTenantManagerURLRequired
	}

	serviceName := strings.TrimSpace(cfg.ServiceName)
	if serviceName == "" {
		return nil, errTenantManagerServiceEmpty
	}

	apiKey := strings.TrimSpace(cfg.ServiceAPIKey)
	if apiKey == "" {
		return nil, errTenantManagerAPIKeyEmpty
	}

	if err := validateTenantManagerTransport(baseURL, cfg.RuntimeEnvironment); err != nil {
		return nil, err
	}

	logger := cfg.Logger
	if logger == nil {
		logger = &libLog.NopLogger{}
	}

	httpClient := cfg.HTTPClient
	if httpClient == nil {
		timeout := cfg.RequestTimeout
		if timeout <= 0 {
			timeout = defaultRemoteConfigTimeout
		}

		httpClient = &http.Client{Timeout: timeout}
	} else {
		clonedClient := *httpClient
		httpClient = &clonedClient
	}

	httpClient.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }

	manager, err := circuitbreaker.NewManager(logger)
	if err != nil {
		return nil, fmt.Errorf("create circuit breaker manager: %w", err)
	}

	breakerConfig := cfg.BreakerConfig
	if breakerConfig == (circuitbreaker.Config{}) {
		breakerConfig = circuitbreaker.Config{
			ConsecutiveFailures: defaultRemoteConsecutiveFailures,
			Timeout:             defaultRemoteCircuitBreakerTimeout,
		}
	}

	if _, err := manager.GetOrCreate(tenantManagerCircuitBreakerName, breakerConfig); err != nil {
		return nil, fmt.Errorf("create tenant-manager circuit breaker: %w", err)
	}

	return &RemoteConfigurationAdapter{
		baseURL:         strings.TrimRight(baseURL, "/"),
		serviceName:     serviceName,
		apiKey:          apiKey,
		environmentName: strings.TrimSpace(cfg.EnvironmentName),
		httpClient:      httpClient,
		breaker:         manager,
	}, nil
}

// GetTenantConfig fetches the effective tenant infrastructure configuration for
// the provided tenant ID.
func (adapter *RemoteConfigurationAdapter) GetTenantConfig(ctx context.Context, tenantID string) (*ports.TenantConfig, error) {
	if adapter == nil {
		return nil, errRemoteConfigAdapterNil
	}

	if adapter.httpClient == nil {
		return nil, errNilHTTPClient
	}

	tenantID = strings.TrimSpace(tenantID)
	if tenantID == "" {
		return nil, errTenantIDRequired
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "infrastructure.tenant.fetch_config")
	defer span.End()

	result, err := adapter.breaker.Execute(tenantManagerCircuitBreakerName, func() (any, error) {
		req, err := adapter.newTenantSettingsRequest(ctx, tenantID)
		if err != nil {
			return nil, fmt.Errorf("build tenant settings request: %w", err)
		}

		resp, err := adapter.httpClient.Do(req)
		if err != nil {
			return nil, fmt.Errorf("call tenant settings endpoint: %w", err)
		}
		defer resp.Body.Close()

		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return nil, fmt.Errorf("read tenant settings response: %w", err)
		}

		if resp.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("%w: status=%d", errTenantSettingsRequestFailed, resp.StatusCode)
		}

		cfg, err := decodeTenantConfig(body)
		if err != nil {
			return nil, err
		}

		return cfg, nil
	})
	if err != nil {
		wrappedErr := fmt.Errorf("execute tenant settings request: %w", err)

		libOpentelemetry.HandleSpanError(span, "failed to resolve tenant configuration", wrappedErr)

		if logger != nil {
			logger.With(
				libLog.String("tenant_id", tenantID),
				libLog.String("service", adapter.serviceName),
				libLog.String("error", wrappedErr.Error()),
			).Log(ctx, libLog.LevelError, "failed to resolve tenant configuration")
		}

		return nil, wrappedErr
	}

	config, ok := result.(*ports.TenantConfig)
	if !ok {
		return nil, fmt.Errorf("%w: %T", errUnexpectedTenantConfigType, result)
	}

	return cloneTenantConfig(config), nil
}

func (adapter *RemoteConfigurationAdapter) newTenantSettingsRequest(ctx context.Context, tenantID string) (*http.Request, error) {
	requestURL := fmt.Sprintf(
		"%s/tenants/%s/services/%s/settings",
		adapter.baseURL,
		url.PathEscape(tenantID),
		url.PathEscape(adapter.serviceName),
	)

	if adapter.environmentName != "" {
		requestURL = requestURL + "?environment=" + url.QueryEscape(adapter.environmentName)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, requestURL, http.NoBody)
	if err != nil {
		return nil, fmt.Errorf("create tenant settings request: %w", err)
	}

	req.Header.Set("X-API-Key", adapter.apiKey)
	req.Header.Set("Accept", "application/json")

	if adapter.environmentName != "" {
		req.Header.Set("X-Tenant-Environment", adapter.environmentName)
	}

	libOpentelemetry.InjectHTTPContext(ctx, req.Header)

	return req, nil
}

func validateTenantManagerTransport(rawURL, runtimeEnvironment string) error {
	if !isProductionRuntime(runtimeEnvironment) {
		return nil
	}

	parsed, err := url.Parse(rawURL)
	if err != nil {
		return fmt.Errorf("parse tenant manager url: %w", err)
	}

	if strings.EqualFold(parsed.Scheme, "https") {
		return nil
	}

	return errUnsafeTenantManagerTransport
}

func isProductionRuntime(runtimeEnvironment string) bool {
	return strings.EqualFold(strings.TrimSpace(runtimeEnvironment), "production")
}

func decodeTenantConfig(body []byte) (*ports.TenantConfig, error) {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil, fmt.Errorf("decode tenant settings response: %w", err)
	}

	if payload, ok := raw["config"]; ok {
		return unmarshalTenantConfigPayload(payload)
	}

	if payload, ok := raw["tenantConfig"]; ok {
		return unmarshalTenantConfigPayload(payload)
	}

	if payload, ok := raw["settings"]; ok {
		return unmarshalTenantConfigPayload(payload)
	}

	var direct ports.TenantConfig
	if err := json.Unmarshal(body, &direct); err == nil && !isTenantConfigZero(direct) {
		return cloneTenantConfig(&direct), nil
	}

	var envelope tenantConfigEnvelope
	if err := json.Unmarshal(body, &envelope); err != nil {
		return nil, fmt.Errorf("decode tenant settings response: %w", err)
	}

	switch {
	case envelope.Config != nil:
		return cloneTenantConfig(envelope.Config), nil
	case envelope.TenantConfig != nil:
		return cloneTenantConfig(envelope.TenantConfig), nil
	case envelope.Settings != nil:
		return cloneTenantConfig(envelope.Settings), nil
	default:
		return nil, errTenantConfigPayloadMissing
	}
}

func unmarshalTenantConfigPayload(payload []byte) (*ports.TenantConfig, error) {
	var cfg *ports.TenantConfig
	if err := json.Unmarshal(payload, &cfg); err != nil {
		return nil, fmt.Errorf("decode tenant settings payload: %w", err)
	}

	if cfg == nil {
		return nil, errTenantConfigPayloadMissing
	}

	return cloneTenantConfig(cfg), nil
}

func isTenantConfigZero(cfg ports.TenantConfig) bool {
	return reflect.DeepEqual(cfg, ports.TenantConfig{})
}

func cloneTenantConfig(cfg *ports.TenantConfig) *ports.TenantConfig {
	if cfg == nil {
		return nil
	}

	clone := *cfg
	if len(cfg.RedisAddresses) > 0 {
		clone.RedisAddresses = append([]string(nil), cfg.RedisAddresses...)
	}

	return &clone
}
