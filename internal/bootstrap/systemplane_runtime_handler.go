// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package bootstrap

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/gofiber/fiber/v2"

	spdomain "github.com/LerianStudio/lib-commons/v4/commons/systemplane/domain"
	spports "github.com/LerianStudio/lib-commons/v4/commons/systemplane/ports"
	spservice "github.com/LerianStudio/lib-commons/v4/commons/systemplane/service"

	"github.com/LerianStudio/matcher/internal/auth"
)

type matcherSystemplaneHandler struct {
	manager      spservice.Manager
	runtime      systemplaneRuntimeManager
	configGetter func() *Config
	identity     *MatcherIdentityResolver
	settings     *runtimeSettingsResolver
}

type matcherSystemplaneValueDTO struct {
	Key             string `json:"key"`
	Value           any    `json:"value"`
	Default         any    `json:"default"`
	Source          string `json:"source"`
	Redacted        bool   `json:"redacted"`
	RestartRequired bool   `json:"restartRequired"`
	MaskedByEnv     bool   `json:"maskedByEnv"`
	EnvVar          string `json:"envVar,omitempty"`
	PersistedValue  any    `json:"persistedValue,omitempty"`
	PersistedSource string `json:"persistedSource,omitempty"`
}

type matcherSystemplaneConfigsResponse struct {
	Values   map[string]matcherSystemplaneValueDTO `json:"values"`
	Revision uint64                                `json:"revision"`
}

type matcherSystemplaneSettingsResponse struct {
	Values   map[string]matcherSystemplaneValueDTO `json:"values"`
	Revision uint64                                `json:"revision"`
	Scope    string                                `json:"scope"`
}

type matcherSystemplanePatchResponse struct {
	Revision           uint64   `json:"revision"`
	AppliedRuntime     bool     `json:"appliedRuntime"`
	PendingRestart     bool     `json:"pendingRestart"`
	AppliedKeys        []string `json:"appliedKeys,omitempty"`
	PendingRestartKeys []string `json:"pendingRestartKeys,omitempty"`
	MaskedByEnvKeys    []string `json:"maskedByEnvKeys,omitempty"`
}

type matcherSystemplanePatchRequest struct {
	Values map[string]any `json:"values"`
}

type matcherSystemplanePatchResult struct {
	Revision           spdomain.Revision
	AppliedRuntime     bool
	PendingRestart     bool
	AppliedKeys        []string
	PendingRestartKeys []string
	MaskedByEnvKeys    []string
}

var (
	errMatcherSystemplaneInvalidRequestBody = errors.New("invalid request body")
	errMatcherSystemplaneEmptyValues        = errors.New("values must not be empty")
	errMatcherSystemplaneInvalidCandidate   = errors.New("invalid runtime patch candidate")
)

func newMatcherSystemplaneHandler(
	manager spservice.Manager,
	runtime systemplaneRuntimeManager,
	configGetter func() *Config,
	settingsResolver *runtimeSettingsResolver,
) *matcherSystemplaneHandler {
	if manager == nil || runtime == nil {
		return nil
	}

	return &matcherSystemplaneHandler{
		manager:      manager,
		runtime:      runtime,
		configGetter: configGetter,
		identity:     &MatcherIdentityResolver{},
		settings:     settingsResolver,
	}
}

func (handler *matcherSystemplaneHandler) getConfigs(fiberCtx *fiber.Ctx) error {
	resolved, err := handler.manager.GetConfigs(fiberCtx.UserContext())
	if err != nil {
		return writeMatcherSystemplaneError(fiberCtx, err)
	}

	values := appendLegacyAliasesToResolvedSet(resolved.Values)
	currentCfg := handler.currentConfig()
	responseValues := make(map[string]matcherSystemplaneValueDTO, len(values))

	for key, effective := range values {
		responseValues[key] = handler.toConfigDTO(key, effective, currentCfg)
	}

	return fiberCtx.JSON(matcherSystemplaneConfigsResponse{
		Values:   responseValues,
		Revision: resolved.Revision.Uint64(),
	})
}

func (handler *matcherSystemplaneHandler) getSettings(fiberCtx *fiber.Ctx) error {
	subject, err := handler.resolveSubject(fiberCtx)
	if err != nil {
		return writeMatcherSystemplaneError(fiberCtx, err)
	}

	resolved, err := handler.manager.GetSettings(fiberCtx.UserContext(), subject)
	if err != nil {
		return writeMatcherSystemplaneError(fiberCtx, err)
	}

	currentCfg := handler.currentConfig()
	responseValues := make(map[string]matcherSystemplaneValueDTO, len(resolved.Values))

	for key, effective := range resolved.Values {
		responseValues[key] = handler.toSettingDTO(key, effective, subject, currentCfg)
	}

	return fiberCtx.JSON(matcherSystemplaneSettingsResponse{
		Values:   responseValues,
		Revision: resolved.Revision.Uint64(),
		Scope:    string(subject.Scope),
	})
}

func (handler *matcherSystemplaneHandler) patchConfigs(fiberCtx *fiber.Ctx) error {
	ctx := matcherSystemplaneUserContext(fiberCtx)

	req, err := parseMatcherSystemplanePatchRequest(fiberCtx)
	if err != nil {
		return fiberCtx.Status(http.StatusBadRequest).JSON(fiber.Map{
			"code":    "system_invalid_request",
			"message": err.Error(),
		})
	}

	revision, err := parseMatcherSystemplaneRevision(fiberCtx.Get("If-Match"))
	if err != nil {
		return fiberCtx.Status(http.StatusBadRequest).JSON(fiber.Map{
			"code":    "system_invalid_revision",
			"message": "If-Match header must contain a valid revision number",
		})
	}

	actor, err := handler.identity.Actor(ctx)
	if err != nil {
		return writeMatcherSystemplaneError(fiberCtx, err)
	}

	result, err := handler.applyConfigPatch(ctx, req.Values, revision, actor)
	if err != nil {
		return writeMatcherSystemplaneError(fiberCtx, err)
	}

	fiberCtx.Set("ETag", fmt.Sprintf(`"%d"`, result.Revision.Uint64()))

	return fiberCtx.JSON(result.toResponse())
}

func (handler *matcherSystemplaneHandler) patchSettings(fiberCtx *fiber.Ctx) error {
	ctx := matcherSystemplaneUserContext(fiberCtx)

	subject, err := handler.resolveSubject(fiberCtx)
	if err != nil {
		return writeMatcherSystemplaneError(fiberCtx, err)
	}

	req, err := parseMatcherSystemplanePatchRequest(fiberCtx)
	if err != nil {
		return fiberCtx.Status(http.StatusBadRequest).JSON(fiber.Map{
			"code":    "system_invalid_request",
			"message": err.Error(),
		})
	}

	revision, err := parseMatcherSystemplaneRevision(fiberCtx.Get("If-Match"))
	if err != nil {
		return fiberCtx.Status(http.StatusBadRequest).JSON(fiber.Map{
			"code":    "system_invalid_revision",
			"message": "If-Match header must contain a valid revision number",
		})
	}

	actor, err := handler.identity.Actor(ctx)
	if err != nil {
		return writeMatcherSystemplaneError(fiberCtx, err)
	}

	result, err := handler.applySettingsPatch(ctx, subject, req.Values, revision, actor)
	if err != nil {
		return writeMatcherSystemplaneError(fiberCtx, err)
	}

	fiberCtx.Set("ETag", fmt.Sprintf(`"%d"`, result.Revision.Uint64()))

	return fiberCtx.JSON(result.toResponse())
}

func matcherSystemplaneUserContext(fiberCtx *fiber.Ctx) context.Context {
	ctx := fiberCtx.UserContext()
	if ctx == nil {
		return context.Background()
	}

	return ctx
}

func parseMatcherSystemplanePatchRequest(fiberCtx *fiber.Ctx) (matcherSystemplanePatchRequest, error) {
	var req matcherSystemplanePatchRequest
	if err := fiberCtx.BodyParser(&req); err != nil {
		return matcherSystemplanePatchRequest{}, errMatcherSystemplaneInvalidRequestBody
	}

	if len(req.Values) == 0 {
		return matcherSystemplanePatchRequest{}, errMatcherSystemplaneEmptyValues
	}

	return req, nil
}

func (result matcherSystemplanePatchResult) toResponse() matcherSystemplanePatchResponse {
	return matcherSystemplanePatchResponse{
		Revision:           result.Revision.Uint64(),
		AppliedRuntime:     result.AppliedRuntime,
		PendingRestart:     result.PendingRestart,
		AppliedKeys:        result.AppliedKeys,
		PendingRestartKeys: result.PendingRestartKeys,
		MaskedByEnvKeys:    result.MaskedByEnvKeys,
	}
}

func (handler *matcherSystemplaneHandler) applyConfigPatch(
	ctx context.Context,
	values map[string]any,
	revision spdomain.Revision,
	actor spdomain.Actor,
) (matcherSystemplanePatchResult, error) {
	resolved, err := handler.manager.GetConfigs(ctx)
	if err != nil {
		return matcherSystemplanePatchResult{}, fmt.Errorf("get configs: %w", err)
	}

	ops := normalizePatchOps(matcherSystemplaneToWriteOps(values), appendLegacyAliasesToResolvedSet(resolved.Values))

	runtimeOps, restartOps, maskedKeys, err := handler.classifyConfigOps(ops)
	if err != nil {
		return matcherSystemplanePatchResult{}, fmt.Errorf("classify config ops: %w", err)
	}

	appliedRuntime := len(runtimeOps) > 0
	pendingRestart := len(restartOps) > 0

	if err := handler.validateConfigPatch(ctx, ops); err != nil {
		return matcherSystemplanePatchResult{}, fmt.Errorf("validate config patch: %w", err)
	}

	target, err := spdomain.NewTarget(spdomain.KindConfig, spdomain.ScopeGlobal, "")
	if err != nil {
		return matcherSystemplanePatchResult{}, fmt.Errorf("new config target: %w", err)
	}

	newRevision, err := handler.runtime.store().Put(ctx, target, ops, revision, actor, "api")
	if err != nil {
		return matcherSystemplanePatchResult{}, fmt.Errorf("patch configs put: %w", err)
	}

	result := matcherSystemplanePatchResult{
		Revision:           newRevision,
		AppliedRuntime:     appliedRuntime,
		PendingRestart:     pendingRestart,
		AppliedKeys:        matcherSystemplaneOpKeys(runtimeOps),
		PendingRestartKeys: matcherSystemplaneOpKeys(restartOps),
		MaskedByEnvKeys:    maskedKeys,
	}

	if !appliedRuntime {
		return result, nil
	}

	behavior, _, err := spservice.Escalate(handler.runtime.registry(), runtimeOps)
	if err != nil {
		return matcherSystemplanePatchResult{}, fmt.Errorf("escalate config ops: %w", err)
	}

	if err := handler.manager.ApplyChangeSignal(ctx, spports.ChangeSignal{
		Target:        target,
		Revision:      newRevision,
		ApplyBehavior: behavior,
	}); err != nil {
		if rollbackErr := handler.rollbackConfigPatch(ctx, target, newRevision, actor, resolved.Values, ops); rollbackErr != nil {
			return matcherSystemplanePatchResult{}, fmt.Errorf("apply config change signal: %w (rollback failed: %w)", err, rollbackErr)
		}

		return matcherSystemplanePatchResult{}, fmt.Errorf("apply config change signal: %w", err)
	}

	return result, nil
}

func (handler *matcherSystemplaneHandler) rollbackConfigPatch(
	ctx context.Context,
	target spdomain.Target,
	expected spdomain.Revision,
	actor spdomain.Actor,
	previous map[string]spdomain.EffectiveValue,
	ops []spports.WriteOp,
) error {
	rollbackOps := rollbackConfigOps(previous, ops)
	if len(rollbackOps) == 0 {
		return nil
	}

	if _, err := handler.runtime.store().Put(ctx, target, rollbackOps, expected, actor, "api-rollback"); err != nil {
		return fmt.Errorf("rollback config patch: %w", err)
	}

	return nil
}

func rollbackConfigOps(previous map[string]spdomain.EffectiveValue, ops []spports.WriteOp) []spports.WriteOp {
	if len(ops) == 0 {
		return nil
	}

	rollbackOps := make([]spports.WriteOp, 0, len(ops))
	for _, op := range ops {
		effective, ok := previous[op.Key]
		if !ok || effective.Source == defaultTenantSlug || effective.Source == "registry-default" {
			rollbackOps = append(rollbackOps, spports.WriteOp{Key: op.Key, Reset: true})
			continue
		}

		rollbackOps = append(rollbackOps, spports.WriteOp{Key: op.Key, Value: effective.Value})
	}

	return rollbackOps
}

func (handler *matcherSystemplaneHandler) applySettingsPatch(
	ctx context.Context,
	subject spservice.Subject,
	values map[string]any,
	revision spdomain.Revision,
	actor spdomain.Actor,
) (matcherSystemplanePatchResult, error) {
	ops := matcherSystemplaneToWriteOps(values)
	if err := handler.validateSettingPatch(ctx, subject, ops); err != nil {
		return matcherSystemplanePatchResult{}, fmt.Errorf("validate setting patch: %w", err)
	}

	result, err := handler.manager.PatchSettings(ctx, subject, spservice.PatchRequest{
		Ops:              ops,
		ExpectedRevision: revision,
		Actor:            actor,
		Source:           "api",
	})
	if err != nil {
		return matcherSystemplanePatchResult{}, fmt.Errorf("patch settings: %w", err)
	}

	if handler.settings != nil {
		handler.settings.invalidateSubject(subject)
	}

	maskedKeys := handler.maskedSettingKeys(subject, ops)
	appliedKeys := matcherSystemplaneOpKeys(ops)

	if len(maskedKeys) > 0 {
		masked := make(map[string]struct{}, len(maskedKeys))
		for _, key := range maskedKeys {
			masked[key] = struct{}{}
		}

		filtered := appliedKeys[:0]
		for _, key := range appliedKeys {
			if _, ok := masked[key]; ok {
				continue
			}

			filtered = append(filtered, key)
		}

		appliedKeys = filtered
	}

	return matcherSystemplanePatchResult{
		Revision:        result.Revision,
		AppliedRuntime:  len(appliedKeys) > 0,
		PendingRestart:  false,
		AppliedKeys:     appliedKeys,
		MaskedByEnvKeys: maskedKeys,
	}, nil
}

func (handler *matcherSystemplaneHandler) classifyConfigOps(ops []spports.WriteOp) ([]spports.WriteOp, []spports.WriteOp, []string, error) {
	runtimeOps := make([]spports.WriteOp, 0, len(ops))
	restartOps := make([]spports.WriteOp, 0, len(ops))
	maskedKeys := make([]string, 0, len(ops))

	for _, op := range ops {
		def, ok := handler.runtime.registry().Get(op.Key)
		if !ok {
			return nil, nil, nil, fmt.Errorf("key %q: %w", op.Key, spdomain.ErrKeyUnknown)
		}

		if def.Kind != spdomain.KindConfig {
			return nil, nil, nil, fmt.Errorf("key %q is kind %q, not config: %w", op.Key, def.Kind, spdomain.ErrKeyUnknown)
		}

		if hasExplicitEnvOverrideForKey(op.Key) {
			maskedKeys = append(maskedKeys, op.Key)
		}

		if def.ApplyBehavior == spdomain.ApplyBootstrapOnly || !def.MutableAtRuntime {
			restartOps = append(restartOps, op)
			continue
		}

		runtimeOps = append(runtimeOps, op)
	}

	sort.Strings(maskedKeys)

	return runtimeOps, restartOps, maskedKeys, nil
}

func (handler *matcherSystemplaneHandler) validateConfigPatch(ctx context.Context, ops []spports.WriteOp) error {
	for _, op := range ops {
		def, ok := handler.runtime.registry().Get(op.Key)
		if !ok {
			return fmt.Errorf("key %q: %w", op.Key, spdomain.ErrKeyUnknown)
		}

		if def.Kind != spdomain.KindConfig {
			return fmt.Errorf("key %q is kind %q, not config: %w", op.Key, def.Kind, spdomain.ErrKeyUnknown)
		}

		if !op.Reset && !spdomain.IsNilValue(op.Value) {
			if err := handler.runtime.registry().Validate(op.Key, op.Value); err != nil {
				return fmt.Errorf("key %q: %w", op.Key, err)
			}
		}
	}

	candidate := cloneMatcherSnapshot(handler.runtime.supervisor().Snapshot())
	if candidate.Configs == nil {
		candidate.Configs = make(map[string]spdomain.EffectiveValue)
	}

	for _, op := range ops {
		def, _ := handler.runtime.registry().Get(op.Key)
		ev := candidate.Configs[op.Key]
		ev.Key = op.Key
		ev.Default = def.DefaultValue
		ev.Redacted = def.RedactPolicy != spdomain.RedactNone || def.Secret

		if op.Reset || spdomain.IsNilValue(op.Value) {
			ev.Value = def.DefaultValue
			ev.Override = nil
			ev.Source = "default"
		} else {
			ev.Value = op.Value
			ev.Override = op.Value
			ev.Source = "preview-override"
		}

		candidate.Configs[op.Key] = ev
	}

	candidate.BuiltAt = time.Now().UTC()

	return validatePatchCandidateConfig(ctx, handler.currentConfig(), candidate)
}

func (handler *matcherSystemplaneHandler) validateSettingPatch(ctx context.Context, subject spservice.Subject, ops []spports.WriteOp) error {
	for _, op := range ops {
		def, ok := handler.runtime.registry().Get(op.Key)
		if !ok {
			return fmt.Errorf("key %q: %w", op.Key, spdomain.ErrKeyUnknown)
		}

		if def.Kind != spdomain.KindSetting {
			return fmt.Errorf("key %q is kind %q, not setting: %w", op.Key, def.Kind, spdomain.ErrKeyUnknown)
		}

		if !op.Reset && !spdomain.IsNilValue(op.Value) {
			if err := handler.runtime.registry().Validate(op.Key, op.Value); err != nil {
				return fmt.Errorf("key %q: %w", op.Key, err)
			}
		}
	}

	if subject.Scope != spdomain.ScopeGlobal {
		return nil
	}

	candidate := cloneMatcherSnapshot(handler.runtime.supervisor().Snapshot())
	if candidate.GlobalSettings == nil {
		candidate.GlobalSettings = make(map[string]spdomain.EffectiveValue)
	}

	for _, op := range ops {
		def, _ := handler.runtime.registry().Get(op.Key)
		ev := candidate.GlobalSettings[op.Key]
		ev.Key = op.Key
		ev.Default = def.DefaultValue
		ev.Redacted = def.RedactPolicy != spdomain.RedactNone || def.Secret

		if op.Reset || spdomain.IsNilValue(op.Value) {
			ev.Value = def.DefaultValue
			ev.Override = nil
			ev.Source = "default"
		} else {
			ev.Value = op.Value
			ev.Override = op.Value
			ev.Source = "preview-override"
		}

		candidate.GlobalSettings[op.Key] = ev
	}

	candidate.BuiltAt = time.Now().UTC()

	return validatePatchCandidateConfig(ctx, handler.currentConfig(), candidate)
}

func (handler *matcherSystemplaneHandler) maskedSettingKeys(subject spservice.Subject, ops []spports.WriteOp) []string {
	if subject.Scope != spdomain.ScopeGlobal {
		return nil
	}

	keys := make([]string, 0, len(ops))
	for _, op := range ops {
		if hasExplicitEnvOverrideForKey(op.Key) {
			keys = append(keys, op.Key)
		}
	}

	sort.Strings(keys)

	return keys
}

func (handler *matcherSystemplaneHandler) resolveSubject(fiberCtx *fiber.Ctx) (spservice.Subject, error) {
	scope := fiberCtx.Query("scope", "tenant")

	switch scope {
	case "global":
		return spservice.Subject{Scope: spdomain.ScopeGlobal}, nil
	case "tenant":
		tenantID, ok := auth.LookupTenantID(fiberCtx.UserContext())
		if !ok {
			return spservice.Subject{Scope: spdomain.ScopeGlobal}, nil
		}

		return spservice.Subject{Scope: spdomain.ScopeTenant, SubjectID: tenantID}, nil
	default:
		return spservice.Subject{}, spdomain.ErrScopeInvalid
	}
}

func (handler *matcherSystemplaneHandler) currentConfig() *Config {
	if handler == nil || handler.configGetter == nil {
		return nil
	}

	return handler.configGetter()
}

func (handler *matcherSystemplaneHandler) toConfigDTO(key string, effective spdomain.EffectiveValue, currentCfg *Config) matcherSystemplaneValueDTO {
	registryKey := matcherSystemplaneRegistryKey(key)
	def, _ := handler.runtime.registry().Get(registryKey)

	dto := matcherSystemplaneValueDTO{
		Key:             key,
		Value:           effective.Value,
		Default:         effective.Default,
		Source:          effective.Source,
		Redacted:        effective.Redacted,
		RestartRequired: def.ApplyBehavior == spdomain.ApplyBootstrapOnly || !def.MutableAtRuntime,
	}

	if currentCfg != nil && !effective.Redacted {
		if currentValue, ok := resolveConfigValue(currentCfg, key); ok {
			dto.Value = currentValue
		}
	}

	if hasExplicitEnvOverrideForKey(registryKey) {
		dto.MaskedByEnv = true
		dto.EnvVar, _ = resolveConfigEnvVar(registryKey)
		dto.PersistedValue = effective.Value
		dto.PersistedSource = effective.Source
		dto.Source = "env-override"
	}

	return dto
}

func (handler *matcherSystemplaneHandler) toSettingDTO(
	key string,
	effective spdomain.EffectiveValue,
	subject spservice.Subject,
	currentCfg *Config,
) matcherSystemplaneValueDTO {
	registryKey := matcherSystemplaneRegistryKey(key)
	def, _ := handler.runtime.registry().Get(registryKey)

	dto := matcherSystemplaneValueDTO{
		Key:             key,
		Value:           effective.Value,
		Default:         effective.Default,
		Source:          effective.Source,
		Redacted:        effective.Redacted,
		RestartRequired: def.ApplyBehavior == spdomain.ApplyBootstrapOnly || !def.MutableAtRuntime,
	}

	if subject.Scope == spdomain.ScopeGlobal && hasExplicitEnvOverrideForKey(registryKey) {
		dto.MaskedByEnv = true
		dto.EnvVar, _ = resolveConfigEnvVar(registryKey)
		dto.PersistedValue = effective.Value
		dto.PersistedSource = effective.Source
		dto.Source = "env-override-global"

		if currentCfg != nil && !effective.Redacted {
			if currentValue, ok := resolveConfigValue(currentCfg, key); ok {
				dto.Value = currentValue
			}
		}
	}

	if subject.Scope == spdomain.ScopeTenant && effective.Source != "tenant-override" && hasExplicitEnvOverrideForKey(registryKey) {
		dto.MaskedByEnv = true
		dto.Source = "env-override-global"

		if currentCfg != nil && !effective.Redacted {
			if currentValue, ok := resolveConfigValue(currentCfg, key); ok {
				dto.Value = currentValue
			}
		}
	}

	return dto
}

func matcherSystemplaneRegistryKey(key string) string {
	if canonicalKey, ok := canonicalConfigKey(key); ok {
		return canonicalKey
	}

	return key
}

func matcherSystemplaneToWriteOps(values map[string]any) []spports.WriteOp {
	if len(values) == 0 {
		return nil
	}

	ops := make([]spports.WriteOp, 0, len(values))
	for key, value := range values {
		op := spports.WriteOp{Key: key, Value: value}
		if value == nil {
			op.Reset = true
		}

		ops = append(ops, op)
	}

	return ops
}

func matcherSystemplaneOpKeys(ops []spports.WriteOp) []string {
	if len(ops) == 0 {
		return nil
	}

	keys := make([]string, 0, len(ops))
	for _, op := range ops {
		keys = append(keys, op.Key)
	}

	sort.Strings(keys)

	return keys
}

func parseMatcherSystemplaneRevision(header string) (spdomain.Revision, error) {
	if header == "" {
		return spdomain.RevisionZero, nil
	}

	header = strings.Trim(header, `"`)

	v, err := strconv.ParseUint(header, 10, 64)
	if err != nil {
		return spdomain.RevisionZero, fmt.Errorf("parse revision: %w", err)
	}

	return spdomain.Revision(v), nil
}

func cloneMatcherSnapshot(snapshot spdomain.Snapshot) spdomain.Snapshot {
	cloned := spdomain.Snapshot{
		Configs:        cloneMatcherEffectiveValues(snapshot.Configs),
		GlobalSettings: cloneMatcherEffectiveValues(snapshot.GlobalSettings),
		Revision:       snapshot.Revision,
		BuiltAt:        snapshot.BuiltAt,
	}

	if snapshot.TenantSettings != nil {
		cloned.TenantSettings = make(map[string]map[string]spdomain.EffectiveValue, len(snapshot.TenantSettings))
		for tenantID, values := range snapshot.TenantSettings {
			cloned.TenantSettings[tenantID] = cloneMatcherEffectiveValues(values)
		}
	}

	return cloned
}

func cloneMatcherEffectiveValues(values map[string]spdomain.EffectiveValue) map[string]spdomain.EffectiveValue {
	if values == nil {
		return nil
	}

	cloned := make(map[string]spdomain.EffectiveValue, len(values))
	for key, value := range values {
		cloned[key] = value
	}

	return cloned
}

func validatePatchCandidateConfig(ctx context.Context, baseCfg *Config, snap spdomain.Snapshot) error {
	if err := validateResolvedPatchCandidate(ctx, snapshotToFullConfig(snap, baseCfg)); err != nil {
		return fmt.Errorf("%w: %w", errMatcherSystemplaneInvalidCandidate, err)
	}

	if baseCfg == nil {
		return nil
	}

	if err := validateResolvedPatchCandidate(ctx, snapshotToPersistedConfig(snap, baseCfg)); err != nil {
		return fmt.Errorf("%w: %w", errMatcherSystemplaneInvalidCandidate, err)
	}

	return nil
}

func validateResolvedPatchCandidate(ctx context.Context, candidateCfg *Config) error {
	if err := candidateCfg.validateWithContext(ctx); err != nil {
		return err
	}

	if !IsProductionEnvironment(candidateCfg.App.EnvName) {
		return nil
	}

	if !candidateCfg.RateLimit.Enabled {
		return errRateLimitRequiredProduction
	}

	if candidateCfg.Fetcher.AllowPrivateIPs {
		return errFetcherPrivateIPsProduction
	}

	if candidateCfg.Archival.Enabled && strings.TrimSpace(candidateCfg.ObjectStorage.Endpoint) == "" {
		return errArchivalEndpointRequired
	}

	return nil
}

func writeMatcherSystemplaneError(fiberCtx *fiber.Ctx, err error) error {
	status := http.StatusInternalServerError
	code := "system_internal_error"
	message := "internal server error"

	switch {
	case errors.Is(err, errMatcherSystemplaneInvalidCandidate):
		status = http.StatusBadRequest
		code = "system_invalid_runtime_candidate"
		message = err.Error()
	case errors.Is(err, spdomain.ErrKeyUnknown):
		status = http.StatusBadRequest
		code = "system_key_unknown"
		message = err.Error()
	case errors.Is(err, spdomain.ErrValueInvalid):
		status = http.StatusBadRequest
		code = "system_value_invalid"
		message = err.Error()
	case errors.Is(err, spdomain.ErrKeyNotMutable):
		status = http.StatusBadRequest
		code = "system_key_not_mutable"
		message = err.Error()
	case errors.Is(err, spdomain.ErrScopeInvalid):
		status = http.StatusBadRequest
		code = "system_scope_invalid"
		message = err.Error()
	case errors.Is(err, spdomain.ErrRevisionMismatch):
		status = http.StatusConflict
		code = "system_revision_mismatch"
		message = err.Error()
	case errors.Is(err, spdomain.ErrPermissionDenied):
		status = http.StatusForbidden
		code = "system_permission_denied"
		message = "permission denied"
	case errors.Is(err, spdomain.ErrReloadFailed):
		status = http.StatusInternalServerError
		code = "system_reload_failed"
		message = code
	case errors.Is(err, spdomain.ErrSupervisorStopped):
		status = http.StatusServiceUnavailable
		code = "system_unavailable"
		message = code
	}

	return fiberCtx.Status(status).JSON(fiber.Map{
		"code":    code,
		"message": message,
	})
}
