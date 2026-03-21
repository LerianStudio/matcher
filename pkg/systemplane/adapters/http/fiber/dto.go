// Copyright 2025 Lerian Studio.

package fiberhttp

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/gofiber/fiber/v2"

	"github.com/LerianStudio/matcher/pkg/systemplane/domain"
	"github.com/LerianStudio/matcher/pkg/systemplane/ports"
	"github.com/LerianStudio/matcher/pkg/systemplane/service"
)

// Config DTOs.

// ConfigsResponse is the response for GET /v1/system/configs.
type ConfigsResponse struct {
	Values   map[string]EffectiveValueDTO `json:"values"`
	Revision uint64                       `json:"revision"`
}

// EffectiveValueDTO represents a resolved configuration value with its
// provenance and redaction status.
type EffectiveValueDTO struct {
	Key      string `json:"key"`
	Value    any    `json:"value"`
	Default  any    `json:"default"`
	Source   string `json:"source"`
	Redacted bool   `json:"redacted"`
}

// PatchConfigsRequest is the request body for PATCH /v1/system/configs.
type PatchConfigsRequest struct {
	Values map[string]any `json:"values"` // null value = reset to default
}

// PatchResponse is the response for PATCH operations on both configs and
// settings. It carries the new revision number after the write.
type PatchResponse struct {
	Revision uint64 `json:"revision"`
}

// SchemaResponse is the response for GET /schema endpoints.
type SchemaResponse struct {
	Keys []SchemaEntryDTO `json:"keys"`
}

// SchemaEntryDTO represents a single key's metadata in the schema response.
type SchemaEntryDTO struct {
	Key              string   `json:"key"`
	Kind             string   `json:"kind"`
	AllowedScopes    []string `json:"allowedScopes"`
	ValueType        string   `json:"valueType"`
	DefaultValue     any      `json:"defaultValue"`
	MutableAtRuntime bool     `json:"mutableAtRuntime"`
	ApplyBehavior    string   `json:"applyBehavior"`
	Secret           bool     `json:"secret"`
	Description      string   `json:"description"`
	Group            string   `json:"group"`
}

// HistoryResponse is the response for GET /history endpoints.
type HistoryResponse struct {
	Entries []HistoryEntryDTO `json:"entries"`
}

// HistoryEntryDTO represents a single configuration change record.
type HistoryEntryDTO struct {
	Revision  uint64 `json:"revision"`
	Key       string `json:"key"`
	Scope     string `json:"scope"`
	SubjectID string `json:"subjectId,omitempty"`
	OldValue  any    `json:"oldValue"`
	NewValue  any    `json:"newValue"`
	ActorID   string `json:"actorId"`
	ChangedAt string `json:"changedAt"` // RFC3339
}

// Settings DTOs.

// SettingsResponse is the response for GET /v1/system/settings.
type SettingsResponse struct {
	Values   map[string]EffectiveValueDTO `json:"values"`
	Revision uint64                       `json:"revision"`
	Scope    string                       `json:"scope"`
}

// PatchSettingsRequest is the request body for PATCH /v1/system/settings.
type PatchSettingsRequest struct {
	Values map[string]any `json:"values"`
}

// ErrorResponse is a standard error response body.
type ErrorResponse struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// ReloadResponse is the response for POST /v1/system/configs/reload.
type ReloadResponse struct {
	Status string `json:"status"`
}

// Conversion helpers.

const (
	redactedPlaceholder = "********"
	defaultHistoryLimit = 50
	maxHistoryLimit     = 100
	revisionQuoteChar   = "\""
)

// parseRevision parses the If-Match header into a domain.Revision.
// An empty header returns RevisionZero (no concurrency check). A literal "0"
// also returns RevisionZero, which the manager layer interprets as "skip
// concurrency check" — this is intentional for initial writes.
// The value may optionally be quoted in ETag format (e.g., "123").
func parseRevision(header string) (domain.Revision, error) {
	if header == "" {
		return domain.RevisionZero, nil
	}

	header = strings.Trim(header, revisionQuoteChar)

	v, err := strconv.ParseUint(header, 10, 64)
	if err != nil {
		return domain.RevisionZero, fmt.Errorf("parse revision: %w", err)
	}

	return domain.Revision(v), nil
}

// toWriteOps converts a map of key->value into a WriteOp slice. A nil/null
// JSON value becomes a reset operation that restores the key's default.
// A nil or empty map returns nil — callers must guard before invoking.
func toWriteOps(values map[string]any) []ports.WriteOp {
	if len(values) == 0 {
		return nil
	}

	ops := make([]ports.WriteOp, 0, len(values))

	for key, value := range values {
		op := ports.WriteOp{Key: key, Value: value}

		if value == nil {
			op.Reset = true
		}

		ops = append(ops, op)
	}

	return ops
}

// toConfigsResponse converts a ResolvedSet to the configs API response.
func toConfigsResponse(resolved service.ResolvedSet) ConfigsResponse {
	values := make(map[string]EffectiveValueDTO, len(resolved.Values))

	for key, ev := range resolved.Values {
		values[key] = toEffectiveValueDTO(ev)
	}

	return ConfigsResponse{
		Values:   values,
		Revision: resolved.Revision.Uint64(),
	}
}

// toEffectiveValueDTO converts a domain EffectiveValue to its DTO form.
// The service layer is the authority on redaction — values reaching here are
// already masked according to the key's RedactPolicy (full, mask-last-4, etc.).
// The DTO layer preserves the upstream shape without re-masking.
func toEffectiveValueDTO(ev domain.EffectiveValue) EffectiveValueDTO {
	return EffectiveValueDTO{
		Key:      ev.Key,
		Value:    ev.Value,
		Default:  ev.Default,
		Source:   ev.Source,
		Redacted: ev.Redacted,
	}
}

// toSchemaResponse converts a slice of SchemaEntry into the API response.
func toSchemaResponse(entries []service.SchemaEntry) SchemaResponse {
	dtos := make([]SchemaEntryDTO, len(entries))

	for i, entry := range entries {
		scopes := make([]string, len(entry.AllowedScopes))
		for j, s := range entry.AllowedScopes {
			scopes[j] = string(s)
		}

		dtos[i] = SchemaEntryDTO{
			Key:              entry.Key,
			Kind:             string(entry.Kind),
			AllowedScopes:    scopes,
			ValueType:        string(entry.ValueType),
			DefaultValue:     entry.DefaultValue,
			MutableAtRuntime: entry.MutableAtRuntime,
			ApplyBehavior:    string(entry.ApplyBehavior),
			Secret:           entry.Secret,
			Description:      entry.Description,
			Group:            entry.Group,
		}
	}

	return SchemaResponse{Keys: dtos}
}

// toSettingsResponse converts a ResolvedSet and subject into the settings API response.
func toSettingsResponse(resolved service.ResolvedSet, subject service.Subject) SettingsResponse {
	values := make(map[string]EffectiveValueDTO, len(resolved.Values))

	for key, ev := range resolved.Values {
		values[key] = toEffectiveValueDTO(ev)
	}

	return SettingsResponse{
		Values:   values,
		Revision: resolved.Revision.Uint64(),
		Scope:    string(subject.Scope),
	}
}

// toHistoryResponse converts a slice of HistoryEntry into the API response.
func toHistoryResponse(entries []ports.HistoryEntry) HistoryResponse {
	dtos := make([]HistoryEntryDTO, len(entries))

	for i, entry := range entries {
		dtos[i] = HistoryEntryDTO{
			Revision:  entry.Revision.Uint64(),
			Key:       entry.Key,
			Scope:     string(entry.Scope),
			SubjectID: entry.SubjectID,
			OldValue:  entry.OldValue,
			NewValue:  entry.NewValue,
			ActorID:   entry.ActorID,
			ChangedAt: entry.ChangedAt.Format(time.RFC3339),
		}
	}

	return HistoryResponse{Entries: dtos}
}

// parseHistoryFilter extracts pagination and filter query params from the
// Fiber context and returns a HistoryFilter with sensible defaults.
// Explicitly invalid values (non-numeric, negative, beyond max) return an error
// so the client gets a clear 400 rather than silently coerced results.
func parseHistoryFilter(fiberCtx *fiber.Ctx, kind domain.Kind) (ports.HistoryFilter, error) {
	limitStr := fiberCtx.Query("limit")
	offsetStr := fiberCtx.Query("offset")

	limit := defaultHistoryLimit
	offset := 0

	if limitStr != "" {
		parsed, err := strconv.Atoi(limitStr)
		if err != nil || parsed <= 0 || parsed > maxHistoryLimit {
			return ports.HistoryFilter{}, fmt.Errorf("limit must be a positive integer between 1 and %d: %w", maxHistoryLimit, domain.ErrValueInvalid)
		}

		limit = parsed
	}

	if offsetStr != "" {
		parsed, err := strconv.Atoi(offsetStr)
		if err != nil || parsed < 0 {
			return ports.HistoryFilter{}, fmt.Errorf("offset must be a non-negative integer: %w", domain.ErrValueInvalid)
		}

		offset = parsed
	}

	// Validate scope if provided — reject arbitrary values before they reach the store.
	var scope domain.Scope

	if scopeStr := fiberCtx.Query("scope"); scopeStr != "" {
		scope = domain.Scope(scopeStr)
		if scope != domain.ScopeGlobal && scope != domain.ScopeTenant {
			return ports.HistoryFilter{}, domain.ErrScopeInvalid
		}
	}

	return ports.HistoryFilter{
		Kind:   kind,
		Scope:  scope,
		Key:    fiberCtx.Query("key"),
		Limit:  limit,
		Offset: offset,
		// NOTE: SubjectID is intentionally NOT read from query params.
		// For settings history, the handler overrides this with the auth-resolved
		// tenant identity. For config history, subject is always empty (global).
	}, nil
}
