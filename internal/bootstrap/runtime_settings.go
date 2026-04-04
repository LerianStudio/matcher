// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package bootstrap

import (
	"context"
	"sync"
	"time"

	libCommons "github.com/LerianStudio/lib-commons/v4/commons"
	libLog "github.com/LerianStudio/lib-commons/v4/commons/log"
	spdomain "github.com/LerianStudio/lib-commons/v4/commons/systemplane/domain"
	spservice "github.com/LerianStudio/lib-commons/v4/commons/systemplane/service"

	"github.com/LerianStudio/matcher/internal/auth"
)

type systemplaneManagerProvider func() spservice.Manager

// runtimeSettingsResolver resolves tenant/global settings from the current
// systemplane snapshot and overlays them onto fallback process-global values.
//
// Fallback values should come from the current global Config path so that:
//  1. global settings continue working even before every consumer is migrated,
//  2. env/bootstrap fallback remains available while startup sequencing evolves,
//  3. tenant-specific overrides can be layered on top without losing the current
//     global behavior semantics.
type runtimeSettingsResolver struct {
	managerProvider systemplaneManagerProvider
	logger          libLog.Logger
	mu              sync.RWMutex
	tenantCache     map[string]cachedRuntimeSettings
}

type cachedRuntimeSettings struct {
	values    map[string]spdomain.EffectiveValue
	expiresAt time.Time
}

const tenantRuntimeSettingsCacheTTL = time.Second

func newRuntimeSettingsResolver(provider systemplaneManagerProvider, logger ...libLog.Logger) *runtimeSettingsResolver {
	if provider == nil {
		return nil
	}

	var runtimeLogger libLog.Logger
	if len(logger) > 0 {
		runtimeLogger = logger[0]
	}

	return &runtimeSettingsResolver{managerProvider: provider, logger: runtimeLogger}
}

func (resolver *runtimeSettingsResolver) rateLimit(ctx context.Context, fallback RateLimitConfig) RateLimitConfig {
	values, ok := resolver.values(ctx)
	if !ok {
		return fallback
	}

	fallback.Enabled = boolSettingValue(values, "rate_limit.enabled", fallback.Enabled)
	fallback.Max = intSettingValue(values, "rate_limit.max", fallback.Max)
	fallback.ExpirySec = intSettingValue(values, "rate_limit.expiry_sec", fallback.ExpirySec)
	fallback.ExportMax = intSettingValue(values, "rate_limit.export_max", fallback.ExportMax)
	fallback.ExportExpirySec = intSettingValue(values, "rate_limit.export_expiry_sec", fallback.ExportExpirySec)
	fallback.DispatchMax = intSettingValue(values, "rate_limit.dispatch_max", fallback.DispatchMax)
	fallback.DispatchExpirySec = intSettingValue(values, "rate_limit.dispatch_expiry_sec", fallback.DispatchExpirySec)

	return fallback
}

func (resolver *runtimeSettingsResolver) idempotencyRetryWindow(ctx context.Context, fallback time.Duration) time.Duration {
	seconds := intSettingValueFromContext(ctx, resolver, "idempotency.retry_window_sec", int(fallback/time.Second))
	if seconds <= 0 {
		return fallback
	}

	return time.Duration(seconds) * time.Second
}

func (resolver *runtimeSettingsResolver) idempotencySuccessTTL(ctx context.Context, fallback time.Duration) time.Duration {
	hours := intSettingValueFromContext(ctx, resolver, "idempotency.success_ttl_hours", int(fallback/time.Hour))
	if hours <= 0 {
		return fallback
	}

	return time.Duration(hours) * time.Hour
}

func (resolver *runtimeSettingsResolver) callbackRateLimitPerMinute(ctx context.Context, fallback int) int {
	return intSettingValueFromContext(ctx, resolver, "callback_rate_limit.per_minute", fallback)
}

func (resolver *runtimeSettingsResolver) webhookTimeout(ctx context.Context, fallback time.Duration) time.Duration {
	seconds := intSettingValueFromContext(ctx, resolver, "webhook.timeout_sec", int(fallback/time.Second))
	if seconds <= 0 {
		return fallback
	}

	if seconds > maxWebhookTimeoutSec {
		seconds = maxWebhookTimeoutSec
	}

	return time.Duration(seconds) * time.Second
}

func (resolver *runtimeSettingsResolver) dedupeTTL(ctx context.Context, fallback time.Duration) time.Duration {
	seconds := intSettingValueFromContext(ctx, resolver, "deduplication.ttl_sec", int(fallback/time.Second))
	if seconds <= 0 {
		return fallback
	}

	return time.Duration(seconds) * time.Second
}

func (resolver *runtimeSettingsResolver) exportPresignExpiry(ctx context.Context, fallback time.Duration) time.Duration {
	seconds := intSettingValueFromContext(ctx, resolver, "export_worker.presign_expiry_sec", int(fallback/time.Second))
	if seconds <= 0 {
		return fallback
	}

	if seconds > maxPresignExpirySec {
		seconds = maxPresignExpirySec
	}

	return time.Duration(seconds) * time.Second
}

func (resolver *runtimeSettingsResolver) archivalPresignExpiry(ctx context.Context, fallback time.Duration) time.Duration {
	seconds := intSettingValueFromContext(ctx, resolver, "archival.presign_expiry_sec", int(fallback/time.Second))
	if seconds <= 0 {
		return fallback
	}

	if seconds > maxPresignExpirySec {
		seconds = maxPresignExpirySec
	}

	return time.Duration(seconds) * time.Second
}

func intSettingValueFromContext(ctx context.Context, resolver *runtimeSettingsResolver, key string, fallback int) int {
	values, ok := resolver.values(ctx)
	if !ok {
		return fallback
	}

	return intSettingValue(values, key, fallback)
}

func (resolver *runtimeSettingsResolver) values(ctx context.Context) (map[string]spdomain.EffectiveValue, bool) {
	if resolver == nil || resolver.managerProvider == nil {
		return nil, false
	}

	if ctx == nil {
		return nil, false
	}

	subject := resolver.subject(ctx)
	if subject.Scope == spdomain.ScopeTenant {
		if values, ok := resolver.cachedTenantValues(subject.SubjectID); ok {
			return values, true
		}
	}

	manager := resolver.managerProvider()
	if spdomain.IsNilValue(manager) {
		return nil, false
	}

	resolved, err := manager.GetSettings(ctx, subject)
	if err != nil {
		resolver.logLookupFailure(ctx, subject, err)

		return nil, false
	}

	if len(resolved.Values) == 0 {
		return nil, false
	}

	values := cloneMatcherEffectiveValues(resolved.Values)
	if subject.Scope == spdomain.ScopeTenant {
		resolver.storeTenantValues(subject.SubjectID, values)
	}

	return values, true
}

func (resolver *runtimeSettingsResolver) subject(ctx context.Context) spservice.Subject {
	if tenantID, ok := auth.LookupTenantID(ctx); ok {
		return spservice.Subject{Scope: spdomain.ScopeTenant, SubjectID: tenantID}
	}

	return spservice.Subject{Scope: spdomain.ScopeGlobal}
}

func (resolver *runtimeSettingsResolver) cachedTenantValues(tenantID string) (map[string]spdomain.EffectiveValue, bool) {
	if resolver == nil || tenantID == "" {
		return nil, false
	}

	resolver.mu.RLock()
	entry, ok := resolver.tenantCache[tenantID]
	resolver.mu.RUnlock()

	if !ok {
		return nil, false
	}

	if time.Now().UTC().After(entry.expiresAt) {
		resolver.mu.Lock()
		delete(resolver.tenantCache, tenantID)
		resolver.mu.Unlock()

		return nil, false
	}

	return cloneMatcherEffectiveValues(entry.values), true
}

func (resolver *runtimeSettingsResolver) storeTenantValues(tenantID string, values map[string]spdomain.EffectiveValue) {
	if resolver == nil || tenantID == "" || len(values) == 0 {
		return
	}

	resolver.mu.Lock()
	defer resolver.mu.Unlock()

	if resolver.tenantCache == nil {
		resolver.tenantCache = make(map[string]cachedRuntimeSettings)
	}

	now := time.Now().UTC()
	resolver.pruneExpiredTenantValuesLocked(now)

	resolver.tenantCache[tenantID] = cachedRuntimeSettings{
		values:    cloneMatcherEffectiveValues(values),
		expiresAt: now.Add(tenantRuntimeSettingsCacheTTL),
	}
}

func (resolver *runtimeSettingsResolver) invalidateSubject(subject spservice.Subject) {
	if resolver == nil {
		return
	}

	if subject.Scope == spdomain.ScopeGlobal {
		resolver.invalidateAll()
		return
	}

	if subject.Scope != spdomain.ScopeTenant || subject.SubjectID == "" {
		return
	}

	resolver.mu.Lock()
	defer resolver.mu.Unlock()

	delete(resolver.tenantCache, subject.SubjectID)
}

func (resolver *runtimeSettingsResolver) invalidateAll() {
	if resolver == nil {
		return
	}

	resolver.mu.Lock()
	defer resolver.mu.Unlock()

	clear(resolver.tenantCache)
}

func (resolver *runtimeSettingsResolver) logLookupFailure(ctx context.Context, subject spservice.Subject, err error) {
	if resolver == nil || err == nil {
		return
	}

	logger := resolver.logger
	if logger == nil {
		logger, _, _, _ = libCommons.NewTrackingFromContext(ctx)
	}

	if logger == nil {
		return
	}

	logger.Log(ctx, libLog.LevelWarn,
		"runtime settings lookup failed; falling back to config",
		libLog.String("scope", string(subject.Scope)),
		libLog.String("subject_id", subject.SubjectID),
		libLog.String("error", err.Error()),
	)
}

func (resolver *runtimeSettingsResolver) pruneExpiredTenantValuesLocked(now time.Time) {
	if resolver == nil || len(resolver.tenantCache) == 0 {
		return
	}

	for tenantID, entry := range resolver.tenantCache {
		if now.After(entry.expiresAt) {
			delete(resolver.tenantCache, tenantID)
		}
	}
}

func intSettingValue(values map[string]spdomain.EffectiveValue, key string, fallback int) int {
	if len(values) == 0 {
		return fallback
	}

	entry, ok := values[key]
	if !ok {
		return fallback
	}

	if settingMaskedByEnvOverride(key, entry) {
		return fallback
	}

	switch value := entry.Value.(type) {
	case int:
		return value
	case int64:
		return int(value)
	case float64:
		return int(value)
	case float32:
		return int(value)
	default:
		return fallback
	}
}

func boolSettingValue(values map[string]spdomain.EffectiveValue, key string, fallback bool) bool {
	if len(values) == 0 {
		return fallback
	}

	entry, ok := values[key]
	if !ok {
		return fallback
	}

	if settingMaskedByEnvOverride(key, entry) {
		return fallback
	}

	value, ok := entry.Value.(bool)
	if !ok {
		return fallback
	}

	return value
}

func settingMaskedByEnvOverride(key string, entry spdomain.EffectiveValue) bool {
	if !hasExplicitEnvOverrideForKey(key) {
		return false
	}

	return entry.Source != "tenant-override"
}
