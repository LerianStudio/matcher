// Copyright 2025 Lerian Studio.

package service

import (
	"context"
	"errors"

	"github.com/LerianStudio/matcher/pkg/systemplane/domain"
	"github.com/LerianStudio/matcher/pkg/systemplane/ports"
	"github.com/LerianStudio/matcher/pkg/systemplane/registry"
)

var (
	errManagerRegistryRequired   = errors.New("new manager: registry is required")
	errManagerStoreRequired      = errors.New("new manager: store is required")
	errManagerHistoryRequired    = errors.New("new manager: history store is required")
	errManagerSupervisorRequired = errors.New("new manager: supervisor is required")
	errManagerBuilderRequired    = errors.New("new manager: snapshot builder is required")
)

// Subject identifies the scope and subject for a settings operation.
type Subject struct {
	Scope     domain.Scope
	SubjectID string
}

// PatchRequest contains the parameters for a config/settings patch operation.
type PatchRequest struct {
	Ops              []ports.WriteOp
	ExpectedRevision domain.Revision
	Actor            domain.Actor
	Source           string
}

// WriteResult contains the outcome of a patch operation.
type WriteResult struct {
	Revision domain.Revision
}

// ResolvedSet contains the resolved effective values and target revision.
type ResolvedSet struct {
	Values   map[string]domain.EffectiveValue
	Revision domain.Revision
}

// SchemaEntry describes a single key's metadata for the schema endpoint.
type SchemaEntry struct {
	Key              string
	Kind             domain.Kind
	AllowedScopes    []domain.Scope
	ValueType        domain.ValueType
	DefaultValue     any
	MutableAtRuntime bool
	ApplyBehavior    domain.ApplyBehavior
	Secret           bool
	RedactPolicy     domain.RedactPolicy
	Description      string
	Group            string
}

// Manager provides the application-level API for reading and writing
// configuration.
type Manager interface {
	GetConfigs(ctx context.Context) (ResolvedSet, error)
	GetSettings(ctx context.Context, subject Subject) (ResolvedSet, error)
	PatchConfigs(ctx context.Context, req PatchRequest) (WriteResult, error)
	PatchSettings(ctx context.Context, subject Subject, req PatchRequest) (WriteResult, error)
	GetConfigSchema(ctx context.Context) ([]SchemaEntry, error)
	GetSettingSchema(ctx context.Context) ([]SchemaEntry, error)
	GetConfigHistory(ctx context.Context, filter ports.HistoryFilter) ([]ports.HistoryEntry, error)
	GetSettingHistory(ctx context.Context, filter ports.HistoryFilter) ([]ports.HistoryEntry, error)
	Resync(ctx context.Context) error
}

// ManagerConfig holds the dependencies for constructing a Manager.
type ManagerConfig struct {
	Registry   registry.Registry
	Store      ports.Store
	History    ports.HistoryStore
	Supervisor Supervisor
	Builder    *SnapshotBuilder
}

// NewManager creates a new Manager with the supplied dependencies. All
// dependencies are required; a nil dependency causes a construction-time
// error rather than a runtime panic on first use.
func NewManager(cfg ManagerConfig) (Manager, error) {
	if cfg.Registry == nil {
		return nil, errManagerRegistryRequired
	}

	if cfg.Store == nil {
		return nil, errManagerStoreRequired
	}

	if cfg.History == nil {
		return nil, errManagerHistoryRequired
	}

	if cfg.Supervisor == nil {
		return nil, errManagerSupervisorRequired
	}

	if cfg.Builder == nil {
		return nil, errManagerBuilderRequired
	}

	return &defaultManager{
		registry:   cfg.Registry,
		store:      cfg.Store,
		history:    cfg.History,
		supervisor: cfg.Supervisor,
		builder:    cfg.Builder,
	}, nil
}

type defaultManager struct {
	registry   registry.Registry
	store      ports.Store
	history    ports.HistoryStore
	supervisor Supervisor
	builder    *SnapshotBuilder
}
