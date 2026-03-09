// Package entities holds discovery domain entities.
package entities

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/LerianStudio/lib-commons/v4/commons/assert"

	vo "github.com/LerianStudio/matcher/internal/discovery/domain/value_objects"
	"github.com/LerianStudio/matcher/internal/shared/constants"
)

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
func (fc *FetcherConnection) MarkAvailable() {
	if fc == nil {
		return
	}

	fc.Status = vo.ConnectionStatusAvailable
	fc.LastSeenAt = time.Now().UTC()
	fc.UpdatedAt = time.Now().UTC()
}

// ApplyFetcherStatus updates the connection status from Fetcher's reported status string.
// Returns true if the status was a recognized value, false if it was mapped to Unknown.
func (fc *FetcherConnection) ApplyFetcherStatus(status string) bool {
	if fc == nil {
		return false
	}

	parsedStatus, err := vo.ParseConnectionStatus(status)
	recognized := err == nil

	if err != nil {
		parsedStatus = vo.ConnectionStatusUnknown
	}

	now := time.Now().UTC()
	fc.Status = parsedStatus
	fc.LastSeenAt = now
	fc.UpdatedAt = now

	return recognized
}

// MarkUnreachable transitions the connection to UNREACHABLE status.
func (fc *FetcherConnection) MarkUnreachable() {
	if fc == nil {
		return
	}

	fc.Status = vo.ConnectionStatusUnreachable
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
func (fc *FetcherConnection) UpdateDetails(host string, port int, dbName, productName string) {
	if fc == nil {
		return
	}

	fc.Host = host

	// Clamp port to valid TCP range; 0 means unspecified.
	if port < 0 {
		port = 0
	}

	const maxPort = 65535
	if port > maxPort {
		port = maxPort
	}

	fc.Port = port
	fc.DatabaseName = dbName
	fc.ProductName = productName
	fc.UpdatedAt = time.Now().UTC()
}
