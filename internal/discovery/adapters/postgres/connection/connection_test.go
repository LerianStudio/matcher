//go:build unit

package connection

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/discovery/domain/entities"
	vo "github.com/LerianStudio/matcher/internal/discovery/domain/value_objects"
)

// validConnectionModel returns a fully-populated ConnectionModel for test fixtures.
func validConnectionModel() *ConnectionModel {
	now := time.Date(2026, 3, 8, 12, 0, 0, 0, time.UTC)

	return &ConnectionModel{
		ID:               uuid.MustParse("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"),
		FetcherConnID:    "fetcher-conn-001",
		ConfigName:       "prod-config",
		DatabaseType:     "POSTGRESQL",
		Host:             "db.example.com",
		Port:             5432,
		DatabaseName:     "ledger",
		ProductName:      "PostgreSQL 17.2",
		Status:           "AVAILABLE",
		LastSeenAt:       now,
		SchemaDiscovered: true,
		CreatedAt:        now.Add(-24 * time.Hour),
		UpdatedAt:        now,
	}
}

// validConnectionEntity returns a fully-populated FetcherConnection entity for test fixtures.
func validConnectionEntity() *entities.FetcherConnection {
	now := time.Date(2026, 3, 8, 12, 0, 0, 0, time.UTC)

	return &entities.FetcherConnection{
		ID:               uuid.MustParse("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"),
		FetcherConnID:    "fetcher-conn-001",
		ConfigName:       "prod-config",
		DatabaseType:     "POSTGRESQL",
		Host:             "db.example.com",
		Port:             5432,
		DatabaseName:     "ledger",
		ProductName:      "PostgreSQL 17.2",
		Status:           vo.ConnectionStatusAvailable,
		LastSeenAt:       now,
		SchemaDiscovered: true,
		CreatedAt:        now.Add(-24 * time.Hour),
		UpdatedAt:        now,
	}
}

func TestConnectionModel_ToDomain_ValidModel(t *testing.T) {
	t.Parallel()

	model := validConnectionModel()
	entity := model.ToDomain()

	require.NotNil(t, entity)
	assert.Equal(t, model.ID, entity.ID)
	assert.Equal(t, model.FetcherConnID, entity.FetcherConnID)
	assert.Equal(t, model.ConfigName, entity.ConfigName)
	assert.Equal(t, model.DatabaseType, entity.DatabaseType)
	assert.Equal(t, model.Host, entity.Host)
	assert.Equal(t, model.Port, entity.Port)
	assert.Equal(t, model.DatabaseName, entity.DatabaseName)
	assert.Equal(t, model.ProductName, entity.ProductName)
	assert.Equal(t, vo.ConnectionStatusAvailable, entity.Status)
	assert.True(t, entity.SchemaDiscovered)
	assert.Equal(t, model.LastSeenAt, entity.LastSeenAt)
	assert.Equal(t, model.CreatedAt, entity.CreatedAt)
	assert.Equal(t, model.UpdatedAt, entity.UpdatedAt)
}

func TestConnectionModel_ToDomain_NilModel(t *testing.T) {
	t.Parallel()

	var model *ConnectionModel
	entity := model.ToDomain()

	assert.Nil(t, entity)
}

func TestConnectionModel_ToDomain_UnknownStatusFallback(t *testing.T) {
	t.Parallel()

	model := validConnectionModel()
	model.Status = "TOTALLY_BOGUS_STATUS"

	entity := model.ToDomain()

	require.NotNil(t, entity)
	assert.Equal(t, vo.ConnectionStatusUnknown, entity.Status,
		"unparseable status should fall back to ConnectionStatusUnknown")
}

func TestConnectionModel_ToDomain_AllStatuses(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		dbStatus string
		expected vo.ConnectionStatus
	}{
		{name: "available", dbStatus: "AVAILABLE", expected: vo.ConnectionStatusAvailable},
		{name: "unreachable", dbStatus: "UNREACHABLE", expected: vo.ConnectionStatusUnreachable},
		{name: "unknown", dbStatus: "UNKNOWN", expected: vo.ConnectionStatusUnknown},
		{name: "lowercase available", dbStatus: "available", expected: vo.ConnectionStatusAvailable},
		{name: "empty string", dbStatus: "", expected: vo.ConnectionStatusUnknown},
		{name: "garbage", dbStatus: "xyz", expected: vo.ConnectionStatusUnknown},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			model := validConnectionModel()
			model.Status = tc.dbStatus

			entity := model.ToDomain()

			require.NotNil(t, entity)
			assert.Equal(t, tc.expected, entity.Status)
		})
	}
}

func TestFromDomain_ValidEntity(t *testing.T) {
	t.Parallel()

	entity := validConnectionEntity()
	model := FromDomain(entity)

	require.NotNil(t, model)
	assert.Equal(t, entity.ID, model.ID)
	assert.Equal(t, entity.FetcherConnID, model.FetcherConnID)
	assert.Equal(t, entity.ConfigName, model.ConfigName)
	assert.Equal(t, entity.DatabaseType, model.DatabaseType)
	assert.Equal(t, entity.Host, model.Host)
	assert.Equal(t, entity.Port, model.Port)
	assert.Equal(t, entity.DatabaseName, model.DatabaseName)
	assert.Equal(t, entity.ProductName, model.ProductName)
	assert.Equal(t, "AVAILABLE", model.Status)
	assert.Equal(t, entity.SchemaDiscovered, model.SchemaDiscovered)
	assert.Equal(t, entity.LastSeenAt, model.LastSeenAt)
	assert.Equal(t, entity.CreatedAt, model.CreatedAt)
	assert.Equal(t, entity.UpdatedAt, model.UpdatedAt)
}

func TestFromDomain_NilEntity(t *testing.T) {
	t.Parallel()

	model := FromDomain(nil)

	assert.Nil(t, model)
}

func TestConnectionModel_RoundTrip_Preserves_All_Fields(t *testing.T) {
	t.Parallel()

	original := validConnectionEntity()

	model := FromDomain(original)
	require.NotNil(t, model)

	roundTripped := model.ToDomain()
	require.NotNil(t, roundTripped)

	assert.Equal(t, original.ID, roundTripped.ID)
	assert.Equal(t, original.FetcherConnID, roundTripped.FetcherConnID)
	assert.Equal(t, original.ConfigName, roundTripped.ConfigName)
	assert.Equal(t, original.DatabaseType, roundTripped.DatabaseType)
	assert.Equal(t, original.Host, roundTripped.Host)
	assert.Equal(t, original.Port, roundTripped.Port)
	assert.Equal(t, original.DatabaseName, roundTripped.DatabaseName)
	assert.Equal(t, original.ProductName, roundTripped.ProductName)
	assert.Equal(t, original.Status, roundTripped.Status)
	assert.Equal(t, original.SchemaDiscovered, roundTripped.SchemaDiscovered)
	assert.Equal(t, original.LastSeenAt, roundTripped.LastSeenAt)
	assert.Equal(t, original.CreatedAt, roundTripped.CreatedAt)
	assert.Equal(t, original.UpdatedAt, roundTripped.UpdatedAt)
}

func TestConnectionModel_RoundTrip_UnreachableStatus(t *testing.T) {
	t.Parallel()

	original := validConnectionEntity()
	original.Status = vo.ConnectionStatusUnreachable

	model := FromDomain(original)
	require.NotNil(t, model)
	assert.Equal(t, "UNREACHABLE", model.Status)

	roundTripped := model.ToDomain()
	require.NotNil(t, roundTripped)
	assert.Equal(t, vo.ConnectionStatusUnreachable, roundTripped.Status)
}

func TestConnectionModel_RoundTrip_ZeroPort(t *testing.T) {
	t.Parallel()

	original := validConnectionEntity()
	original.Port = 0

	model := FromDomain(original)
	require.NotNil(t, model)
	assert.Equal(t, 0, model.Port)

	roundTripped := model.ToDomain()
	require.NotNil(t, roundTripped)
	assert.Equal(t, 0, roundTripped.Port)
}

func TestConnectionModel_RoundTrip_SchemaNotDiscovered(t *testing.T) {
	t.Parallel()

	original := validConnectionEntity()
	original.SchemaDiscovered = false

	model := FromDomain(original)
	require.NotNil(t, model)
	assert.False(t, model.SchemaDiscovered)

	roundTripped := model.ToDomain()
	require.NotNil(t, roundTripped)
	assert.False(t, roundTripped.SchemaDiscovered)
}
