// Copyright 2025 Lerian Studio.

package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"

	_ "github.com/jackc/pgx/v5/stdlib" // pgx driver registration

	"github.com/LerianStudio/matcher/pkg/systemplane/bootstrap"
)

// Sentinel errors for constructor validation.
var (
	ErrNilConfig = errors.New("postgres store: config is nil")
	ErrEmptyDSN  = errors.New("postgres store: DSN is required")
	ErrNilDB     = errors.New("postgres store: db is nil")
)

// dbCloser wraps a *sql.DB to implement io.Closer.
type dbCloser struct {
	db *sql.DB
}

// Close releases the underlying database connection.
func (c *dbCloser) Close() error {
	if c == nil || c.db == nil {
		return nil
	}

	if err := c.db.Close(); err != nil {
		return fmt.Errorf("postgres store: close db: %w", err)
	}

	return nil
}

// New creates a PostgreSQL-backed Store and HistoryStore from the given
// bootstrap configuration. It opens a connection, pings the database,
// executes the DDL to ensure the schema and tables exist, and returns
// both stores along with an io.Closer that closes the underlying connection.
//
// The caller is responsible for calling Close when the stores are no longer
// needed.
func New(ctx context.Context, cfg *bootstrap.PostgresBootstrapConfig) (*Store, *HistoryStore, io.Closer, error) {
	if cfg == nil {
		return nil, nil, nil, ErrNilConfig
	}

	if cfg.DSN == "" {
		return nil, nil, nil, ErrEmptyDSN
	}

	schema := defaultIfEmpty(cfg.Schema, bootstrap.DefaultPostgresSchema)
	entriesTable := defaultIfEmpty(cfg.EntriesTable, bootstrap.DefaultPostgresEntriesTable)
	historyTable := defaultIfEmpty(cfg.HistoryTable, bootstrap.DefaultPostgresHistoryTable)
	revisionTable := defaultIfEmpty(cfg.RevisionTable, bootstrap.DefaultPostgresRevisionTable)
	notifyChannel := defaultIfEmpty(cfg.NotifyChannel, bootstrap.DefaultPostgresNotifyChannel)

	if err := bootstrap.ValidatePostgresObjectNames(schema, entriesTable, historyTable, revisionTable, notifyChannel); err != nil {
		return nil, nil, nil, fmt.Errorf("postgres store: %w", err)
	}

	db, err := sql.Open("pgx", cfg.DSN)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("postgres store: open connection: %w", err)
	}

	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, nil, nil, fmt.Errorf("postgres store: ping: %w", err)
	}

	if err := executeDDL(ctx, db, schema, entriesTable, historyTable, revisionTable); err != nil {
		db.Close()
		return nil, nil, nil, fmt.Errorf("postgres store: execute ddl: %w", err)
	}

	store := &Store{
		db:            db,
		schema:        schema,
		entriesTable:  entriesTable,
		historyTable:  historyTable,
		revisionTable: revisionTable,
		notifyChannel: notifyChannel,
	}

	historyStore := &HistoryStore{
		db:           db,
		schema:       schema,
		historyTable: historyTable,
	}

	return store, historyStore, &dbCloser{db: db}, nil
}

// NewFromDB creates a Store and HistoryStore from an existing *sql.DB
// connection. Unlike New, it does not open or ping the connection and does not
// execute DDL. This is useful for tests and environments where the connection
// is managed externally.
func NewFromDB(db *sql.DB, schema, entriesTable, historyTable, notifyChannel string) (*Store, *HistoryStore, error) {
	validatedSchema := defaultIfEmpty(schema, bootstrap.DefaultPostgresSchema)
	validatedEntriesTable := defaultIfEmpty(entriesTable, bootstrap.DefaultPostgresEntriesTable)
	validatedHistoryTable := defaultIfEmpty(historyTable, bootstrap.DefaultPostgresHistoryTable)
	validatedNotifyChannel := defaultIfEmpty(notifyChannel, bootstrap.DefaultPostgresNotifyChannel)

	if err := bootstrap.ValidatePostgresObjectNames(
		validatedSchema,
		validatedEntriesTable,
		validatedHistoryTable,
		bootstrap.DefaultPostgresRevisionTable,
		validatedNotifyChannel,
	); err != nil {
		return nil, nil, fmt.Errorf("postgres store: %w", err)
	}

	store := &Store{
		db:            db,
		schema:        validatedSchema,
		entriesTable:  validatedEntriesTable,
		historyTable:  validatedHistoryTable,
		revisionTable: bootstrap.DefaultPostgresRevisionTable,
		notifyChannel: validatedNotifyChannel,
	}

	historyStore := &HistoryStore{
		db:           db,
		schema:       validatedSchema,
		historyTable: validatedHistoryTable,
	}

	return store, historyStore, nil
}

// executeDDL creates the schema and tables if they do not already exist.
func executeDDL(ctx context.Context, db *sql.DB, schema, entriesTable, historyTable, revisionTable string) error {
	ddlStatements := []string{
		FormatSchemaDDL(schema),
		FormatEntriesDDL(schema, entriesTable),
		FormatHistoryDDL(schema, historyTable),
		FormatRevisionsDDL(schema, revisionTable),
	}

	for _, stmt := range ddlStatements {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("exec ddl: %w", err)
		}
	}

	return nil
}

// defaultIfEmpty returns val if non-empty, otherwise returns def.
func defaultIfEmpty(val, def string) string {
	if val == "" {
		return def
	}

	return val
}
