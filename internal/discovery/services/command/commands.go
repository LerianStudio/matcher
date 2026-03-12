// Package command provides write operations for discovery management.
package command

import (
	"errors"
	"fmt"
	"reflect"
	"time"

	libLog "github.com/LerianStudio/lib-commons/v4/commons/log"

	"github.com/LerianStudio/matcher/internal/discovery/domain/repositories"
	"github.com/LerianStudio/matcher/internal/discovery/ports"
	"github.com/LerianStudio/matcher/internal/discovery/services/syncer"
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

// Sentinel errors for discovery commands.
var (
	ErrFetcherUnavailable           = errors.New("fetcher service is unavailable")
	ErrConnectionNotFound           = errors.New("fetcher connection not found")
	ErrInvalidExtractionRequest     = errors.New("invalid extraction request")
	ErrExtractionTrackingIncomplete = errors.New("extraction tracking is incomplete")
	ErrTenantContextRequired        = errors.New("tenant context is required")
	ErrDiscoveryRefreshInProgress   = errors.New("discovery refresh already in progress")
	ErrExtractionNotFound           = errors.New("extraction request not found")
	ErrNilExtractionStatus          = errors.New("fetcher returned nil extraction status")
	ErrNilTestConnectionResult      = errors.New("fetcher returned nil connection test result")
	ErrNilFetcherClient             = errors.New("fetcher client is required")
	ErrNilConnectionRepository      = errors.New("connection repository is required")
	ErrNilSchemaRepository          = errors.New("schema repository is required")
	ErrNilExtractionRepository      = errors.New("extraction repository is required")
)

const discoveryRefreshLockTTLMultiplier = 2

// UseCase orchestrates discovery write operations.
type UseCase struct {
	fetcherClient        sharedPorts.FetcherClient
	connRepo             repositories.ConnectionRepository
	schemaRepo           repositories.SchemaRepository
	extractionRepo       repositories.ExtractionRepository
	logger               libLog.Logger
	extractionPoller     ports.ExtractionJobPoller // optional async poller
	syncer               *syncer.ConnectionSyncer
	requireTenantContext bool
	refreshLockProvider  sharedPorts.InfrastructureProvider
	refreshLockTTL       time.Duration
}

// NewUseCase creates a new discovery command use case.
func NewUseCase(
	fetcherClient sharedPorts.FetcherClient,
	connRepo repositories.ConnectionRepository,
	schemaRepo repositories.SchemaRepository,
	extractionRepo repositories.ExtractionRepository,
	logger libLog.Logger,
) (*UseCase, error) {
	if fetcherClient == nil {
		return nil, ErrNilFetcherClient
	}

	if connRepo == nil {
		return nil, ErrNilConnectionRepository
	}

	if schemaRepo == nil {
		return nil, ErrNilSchemaRepository
	}

	if extractionRepo == nil {
		return nil, ErrNilExtractionRepository
	}

	if logger == nil {
		logger = &libLog.NopLogger{}
	}

	cs, err := syncer.NewConnectionSyncer(connRepo, schemaRepo)
	if err != nil {
		return nil, fmt.Errorf("create connection syncer: %w", err)
	}

	return &UseCase{
		fetcherClient:  fetcherClient,
		connRepo:       connRepo,
		schemaRepo:     schemaRepo,
		extractionRepo: extractionRepo,
		logger:         logger,
		syncer:         cs,
	}, nil
}

// WithExtractionPoller adds an optional extraction poller for async job monitoring.
func (uc *UseCase) WithExtractionPoller(poller ports.ExtractionJobPoller) {
	if isNilExtractionPoller(poller) {
		uc.extractionPoller = nil
		return
	}

	uc.extractionPoller = poller
}

// WithSchemaCache wires an optional cache into the connection syncer so manual
// discovery refreshes immediately replace stale cached schemas.
func (uc *UseCase) WithSchemaCache(cache ports.SchemaCache, ttl time.Duration) {
	if uc == nil || uc.syncer == nil {
		return
	}

	uc.syncer.WithSchemaCache(cache, ttl)
}

// WithTenantContextRequirement toggles whether tenant-scoped commands must fail closed
// when tenant context is missing.
func (uc *UseCase) WithTenantContextRequirement(required bool) {
	if uc == nil {
		return
	}

	uc.requireTenantContext = required
}

// WithDiscoveryRefreshLock enables distributed locking for manual refresh operations.
func (uc *UseCase) WithDiscoveryRefreshLock(provider sharedPorts.InfrastructureProvider, interval time.Duration) {
	if uc == nil {
		return
	}

	uc.refreshLockProvider = provider

	if interval <= 0 {
		interval = time.Minute
	}

	uc.refreshLockTTL = discoveryRefreshLockTTLMultiplier * interval
}

func isNilExtractionPoller(poller ports.ExtractionJobPoller) bool {
	if poller == nil {
		return true
	}

	value := reflect.ValueOf(poller)
	switch value.Kind() {
	case reflect.Pointer, reflect.Interface, reflect.Map, reflect.Slice, reflect.Func, reflect.Chan:
		return value.IsNil()
	default:
		return false
	}
}
