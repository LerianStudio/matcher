// Package testutil provides shared test utilities for infrastructure testing.
package testutil

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"unsafe"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/bxcodec/dbresolver/v2"
	"github.com/redis/go-redis/v9"

	libPostgres "github.com/LerianStudio/lib-commons/v4/commons/postgres"
	libRedis "github.com/LerianStudio/lib-commons/v4/commons/redis"

	"github.com/LerianStudio/matcher/internal/shared/ports"
)

var (
	// ErrNoPostgresConnection indicates no postgres connection is configured.
	ErrNoPostgresConnection = errors.New("no postgres connection configured")
	// ErrNoDatabase indicates no database is configured.
	ErrNoDatabase = errors.New("no database configured")
)

// Compile-time check that MockInfrastructureProvider implements InfrastructureProvider.
var _ ports.InfrastructureProvider = (*MockInfrastructureProvider)(nil)

// MockInfrastructureProvider is a test double for ports.InfrastructureProvider.
// Use it in unit tests to avoid real infrastructure connections.
type MockInfrastructureProvider struct {
	PostgresConn *libPostgres.Client
	RedisConn    *libRedis.Client
	PostgresErr  error
	RedisErr     error
	Tx           *sql.Tx
	TxErr        error
	ReplicaDB    *sql.DB
	ReplicaDBErr error
}

// GetPostgresConnection returns the mocked postgres connection or error.
func (provider *MockInfrastructureProvider) GetPostgresConnection(
	_ context.Context,
) (*libPostgres.Client, error) {
	if provider.PostgresErr != nil {
		return nil, provider.PostgresErr
	}

	return provider.PostgresConn, nil
}

// GetRedisConnection returns the mocked redis connection or error.
func (provider *MockInfrastructureProvider) GetRedisConnection(
	_ context.Context,
) (*libRedis.Client, error) {
	if provider.RedisErr != nil {
		return nil, provider.RedisErr
	}

	return provider.RedisConn, nil
}

// tryBeginPostgresTx attempts to begin a transaction from the configured PostgresConn.
// Returns (tx, true) if successful, (nil, false) if PostgresConn is not configured.
func (provider *MockInfrastructureProvider) tryBeginPostgresTx(
	ctx context.Context,
) (*sql.Tx, bool) {
	if provider.PostgresConn == nil {
		return nil, false
	}

	resolver, err := provider.PostgresConn.Resolver(ctx)
	if err != nil {
		return nil, false
	}

	primaryDBs := resolver.PrimaryDBs()
	if len(primaryDBs) == 0 {
		return nil, false
	}

	tx, err := primaryDBs[0].BeginTx(ctx, nil)
	if err != nil {
		return nil, false
	}

	return tx, true
}

// BeginTx returns a mock transaction for testing.
// If TxErr is set, returns the error.
// If Tx is set, returns it.
// If PostgresConn is set with a configured database, uses that to begin transaction.
// Otherwise creates a new mock transaction using sqlmock.
func (provider *MockInfrastructureProvider) BeginTx(ctx context.Context) (*sql.Tx, error) {
	if provider.TxErr != nil {
		return nil, provider.TxErr
	}

	if provider.Tx != nil {
		return provider.Tx, nil
	}

	// If PostgresConn is configured, use it to begin transaction
	// This allows existing tests with sqlmock expectations to work
	tx, ok := provider.tryBeginPostgresTx(ctx)
	if ok {
		return tx, nil
	}

	// Create a mock transaction that handles Commit/Rollback without panicking
	db, mock, err := sqlmock.New()
	if err != nil {
		return nil, fmt.Errorf("create sqlmock: %w", err)
	}

	mock.ExpectBegin()
	mock.ExpectCommit()
	mock.ExpectRollback()

	tx, err = db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("begin mock transaction: %w", err)
	}

	return tx, nil
}

// GetReplicaDB returns the mocked replica database or error.
// If ReplicaDB is set, returns it.
// If PostgresConn is set, attempts to get the replica from it.
// Falls back to primary if no replica is configured.
func (provider *MockInfrastructureProvider) GetReplicaDB(ctx context.Context) (*sql.DB, error) {
	if provider.ReplicaDBErr != nil {
		return nil, provider.ReplicaDBErr
	}

	if provider.ReplicaDB != nil {
		return provider.ReplicaDB, nil
	}

	if provider.PostgresConn == nil {
		return nil, ErrNoPostgresConnection
	}

	resolver, err := provider.PostgresConn.Resolver(ctx)
	if err != nil {
		return nil, fmt.Errorf("get postgres db: %w", err)
	}

	replicaDBs := resolver.ReplicaDBs()
	if len(replicaDBs) > 0 {
		return replicaDBs[0], nil
	}

	primaryDBs := resolver.PrimaryDBs()
	if len(primaryDBs) == 0 {
		return nil, ErrNoDatabase
	}

	return primaryDBs[0], nil
}

// NewClientWithResolver creates a *libPostgres.Client with a pre-injected resolver
// for test purposes. Since v2 postgres.Client has all unexported fields, this uses
// reflect+unsafe to set the resolver field directly. This is intentionally test-only.
func NewClientWithResolver(resolver dbresolver.DB) *libPostgres.Client {
	client := &libPostgres.Client{}

	if resolver == nil {
		return client
	}

	rv := reflect.ValueOf(client).Elem()

	rf := rv.FieldByName("resolver")
	if !rf.IsValid() {
		panic("lib-uncommons postgres.Client no longer has field 'resolver'") //nolint:forbidigo // test infrastructure: panic detects upstream library breaking changes
	}

	// Use unsafe to bypass unexported field restrictions.
	ptr := unsafe.Pointer(rf.UnsafeAddr()) //#nosec G103 -- test-only: bypassing unexported field for mock injection
	*(*dbresolver.DB)(ptr) = resolver

	primaryField := rv.FieldByName("primary")
	if !primaryField.IsValid() {
		panic("lib-uncommons postgres.Client no longer has field 'primary'") //nolint:forbidigo // test infrastructure: panic detects upstream library breaking changes
	}

	primaryDBs := resolver.PrimaryDBs()
	if len(primaryDBs) > 0 {
		pPtr := unsafe.Pointer(primaryField.UnsafeAddr()) //#nosec G103 -- test-only: bypassing unexported field for mock injection
		*(**sql.DB)(pPtr) = primaryDBs[0]
	}

	replicaField := rv.FieldByName("replica")
	if !replicaField.IsValid() {
		panic("lib-uncommons postgres.Client no longer has field 'replica'") //nolint:forbidigo // test infrastructure: panic detects upstream library breaking changes
	}

	replicaDBs := resolver.ReplicaDBs()
	if len(replicaDBs) > 0 {
		rPtr := unsafe.Pointer(replicaField.UnsafeAddr()) //#nosec G103 -- test-only: bypassing unexported field for mock injection
		*(**sql.DB)(rPtr) = replicaDBs[0]
	}

	return client
}

// NewRedisClientWithMock creates a *libRedis.Client with a pre-injected
// redis.UniversalClient for test purposes. Since v2 redis.Client has all unexported
// fields, this uses reflect+unsafe to set the 'client' and 'connected' fields directly.
// This is intentionally test-only. Pass nil to create a client that deterministically
// returns an error from GetClient (simulating a broken/unavailable redis client).
func NewRedisClientWithMock(mock redis.UniversalClient) *libRedis.Client {
	client := &libRedis.Client{}

	if mock == nil {
		rv := reflect.ValueOf(client).Elem()

		cfgField := rv.FieldByName("cfg")
		if !cfgField.IsValid() {
			panic("lib-uncommons redis.Client no longer has field 'cfg'") //nolint:forbidigo // test infrastructure: panic detects upstream library breaking changes
		}

		cfgPtr := unsafe.Pointer(cfgField.UnsafeAddr()) //#nosec G103 -- test-only: bypassing unexported field for mock injection
		*(*libRedis.Config)(cfgPtr) = libRedis.Config{
			Topology: libRedis.Topology{
				Standalone: &libRedis.StandaloneTopology{Address: "127.0.0.1:0"},
			},
			TLS: &libRedis.TLSConfig{CACertBase64: "invalid-base64"},
		}

		return client
	}

	rv := reflect.ValueOf(client).Elem()

	// Set the 'client' field (redis.UniversalClient).
	rf := rv.FieldByName("client")
	if !rf.IsValid() {
		panic("lib-uncommons redis.Client no longer has field 'client'") //nolint:forbidigo // test infrastructure: panic detects upstream library breaking changes
	}

	rPtr := unsafe.Pointer(rf.UnsafeAddr()) //#nosec G103 -- test-only: bypassing unexported field for mock injection
	*(*redis.UniversalClient)(rPtr) = mock

	// Set the 'connected' field to true.
	cf := rv.FieldByName("connected")
	if !cf.IsValid() {
		panic("lib-uncommons redis.Client no longer has field 'connected'") //nolint:forbidigo // test infrastructure: panic detects upstream library breaking changes
	}

	cPtr := unsafe.Pointer(cf.UnsafeAddr()) //#nosec G103 -- test-only: bypassing unexported field for mock injection
	*(*bool)(cPtr) = true

	return client
}

// NewRedisClientConnected creates a *libRedis.Client with connected=true but no
// underlying redis.UniversalClient. Useful for bootstrap tests that only check
// IsConnected() without performing actual Redis operations. GetClient(ctx) will
// attempt to reconnect since the underlying client is nil.
func NewRedisClientConnected() *libRedis.Client {
	client := &libRedis.Client{}
	rv := reflect.ValueOf(client).Elem()

	cf := rv.FieldByName("connected")
	if !cf.IsValid() {
		panic("lib-uncommons redis.Client no longer has field 'connected'") //nolint:forbidigo // test infrastructure: panic detects upstream library breaking changes
	}

	cPtr := unsafe.Pointer(cf.UnsafeAddr()) //#nosec G103 -- test-only: bypassing unexported field for mock injection
	*(*bool)(cPtr) = true

	return client
}

// NewMockProviderFromDB creates a MockInfrastructureProvider that wraps a *sql.DB
// (typically from sqlmock) as the primary database connection. This is the standard
// way to construct a provider for repository unit tests that need real SQL expectations.
func NewMockProviderFromDB(tb testing.TB, db *sql.DB) *MockInfrastructureProvider {
	tb.Helper()

	if db == nil {
		tb.Fatal("NewMockProviderFromDB: db must not be nil")
	}

	resolver := dbresolver.New(dbresolver.WithPrimaryDBs(db))

	return &MockInfrastructureProvider{
		PostgresConn: NewClientWithResolver(resolver),
	}
}
