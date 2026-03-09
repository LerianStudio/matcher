// Package connection provides PostgreSQL repository implementation for FetcherConnection entities.
package connection

import (
	"time"

	"github.com/google/uuid"

	"github.com/LerianStudio/matcher/internal/discovery/domain/entities"
	vo "github.com/LerianStudio/matcher/internal/discovery/domain/value_objects"
)

// ConnectionModel is the PostgreSQL representation of a FetcherConnection.
type ConnectionModel struct {
	ID               uuid.UUID `db:"id"`
	FetcherConnID    string    `db:"fetcher_conn_id"`
	ConfigName       string    `db:"config_name"`
	DatabaseType     string    `db:"database_type"`
	Host             string    `db:"host"`
	Port             int       `db:"port"`
	DatabaseName     string    `db:"database_name"`
	ProductName      string    `db:"product_name"`
	Status           string    `db:"status"`
	LastSeenAt       time.Time `db:"last_seen_at"`
	SchemaDiscovered bool      `db:"schema_discovered"`
	CreatedAt        time.Time `db:"created_at"`
	UpdatedAt        time.Time `db:"updated_at"`
}

// ToDomain converts the PostgreSQL model to a domain entity.
func (model *ConnectionModel) ToDomain() *entities.FetcherConnection {
	if model == nil {
		return nil
	}

	status, parseErr := vo.ParseConnectionStatus(model.Status)
	if parseErr != nil {
		// Use safe fallback for unparseable status values.
		// This can happen if DB contains a value added after this code was deployed.
		status = vo.ConnectionStatusUnknown
	}

	return &entities.FetcherConnection{
		ID:               model.ID,
		FetcherConnID:    model.FetcherConnID,
		ConfigName:       model.ConfigName,
		DatabaseType:     model.DatabaseType,
		Host:             model.Host,
		Port:             model.Port,
		DatabaseName:     model.DatabaseName,
		ProductName:      model.ProductName,
		Status:           status,
		LastSeenAt:       model.LastSeenAt,
		SchemaDiscovered: model.SchemaDiscovered,
		CreatedAt:        model.CreatedAt,
		UpdatedAt:        model.UpdatedAt,
	}
}

// FromDomain converts a domain entity to a PostgreSQL model.
func FromDomain(entity *entities.FetcherConnection) *ConnectionModel {
	if entity == nil {
		return nil
	}

	return &ConnectionModel{
		ID:               entity.ID,
		FetcherConnID:    entity.FetcherConnID,
		ConfigName:       entity.ConfigName,
		DatabaseType:     entity.DatabaseType,
		Host:             entity.Host,
		Port:             entity.Port,
		DatabaseName:     entity.DatabaseName,
		ProductName:      entity.ProductName,
		Status:           entity.Status.String(),
		LastSeenAt:       entity.LastSeenAt,
		SchemaDiscovered: entity.SchemaDiscovered,
		CreatedAt:        entity.CreatedAt,
		UpdatedAt:        entity.UpdatedAt,
	}
}
