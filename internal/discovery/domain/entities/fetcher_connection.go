// Package entities holds discovery domain entities.
package entities

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/LerianStudio/lib-commons/v5/commons/assert"

	vo "github.com/LerianStudio/matcher/internal/discovery/domain/value_objects"
	"github.com/LerianStudio/matcher/internal/shared/constants"
)

// ErrInvalidConnectionPort indicates a port outside the valid TCP range.
var ErrInvalidConnectionPort = errors.New("connection port must be between 0 and 65535")

// FetcherConnection represents a database connection discovered from the Fetcher service.
type FetcherConnection struct {
	ID               uuid.UUID
	FetcherConnID    string
	ConfigName       string
	DatabaseType     string
	Host             string
	Port             int
	DatabaseName     string
	ProductName      string
	Schema           string
	UserName         string
	Status           vo.ConnectionStatus
	LastSeenAt       time.Time
	SchemaDiscovered bool
	CreatedAt        time.Time
	UpdatedAt        time.Time
}

// NewFetcherConnection creates a new FetcherConnection with validated invariants.
func NewFetcherConnection(
	ctx context.Context,
	fetcherConnID, configName, databaseType string,
) (*FetcherConnection, error) {
	asserter := assert.New(ctx, nil, constants.ApplicationName, "discovery.fetcher_connection.new")

	if err := asserter.NotEmpty(ctx, fetcherConnID, "fetcher connection id required"); err != nil {
		return nil, fmt.Errorf("fetcher connection id: %w", err)
	}

	if err := asserter.NotEmpty(ctx, configName, "config name required"); err != nil {
		return nil, fmt.Errorf("fetcher connection config name: %w", err)
	}

	if err := asserter.NotEmpty(ctx, databaseType, "database type required"); err != nil {
		return nil, fmt.Errorf("fetcher connection database type: %w", err)
	}

	now := time.Now().UTC()

	return &FetcherConnection{
		ID:            uuid.New(),
		FetcherConnID: fetcherConnID,
		ConfigName:    configName,
		DatabaseType:  databaseType,
		Status:        vo.ConnectionStatusUnknown,
		LastSeenAt:    now,
		CreatedAt:     now,
		UpdatedAt:     now,
	}, nil
}

// MarkAvailable transitions the connection to AVAILABLE status.
// Connections present in Fetcher's list are reachable by definition.
func (fc *FetcherConnection) MarkAvailable() {
	if fc == nil {
		return
	}

	fc.Status = vo.ConnectionStatusAvailable
	fc.LastSeenAt = time.Now().UTC()
	fc.UpdatedAt = time.Now().UTC()
}

// MarkUnreachable transitions the connection to UNREACHABLE status.
func (fc *FetcherConnection) MarkUnreachable() {
	if fc == nil {
		return
	}

	fc.Status = vo.ConnectionStatusUnreachable
	fc.SchemaDiscovered = false
	fc.UpdatedAt = time.Now().UTC()
}

// MarkSchemaDiscovered records that schema discovery has completed.
func (fc *FetcherConnection) MarkSchemaDiscovered() {
	if fc == nil {
		return
	}

	fc.SchemaDiscovered = true
	fc.UpdatedAt = time.Now().UTC()
}

// UpdateDetails updates the connection metadata from Fetcher.
func (fc *FetcherConnection) UpdateDetails(host string, port int, dbName, productName, schema, userName string) error {
	if fc == nil {
		return nil
	}

	const maxPort = 65535
	if port < 0 || port > maxPort {
		return fmt.Errorf("%w: got %d", ErrInvalidConnectionPort, port)
	}

	fc.Host = host
	fc.Port = port
	fc.DatabaseName = dbName
	fc.ProductName = productName
	fc.Schema = schema
	fc.UserName = userName
	fc.UpdatedAt = time.Now().UTC()

	return nil
}
