// Copyright 2025 Lerian Studio.

// Package builtin provides a ready-to-use systemplane bootstrap entrypoint with
// the built-in PostgreSQL and MongoDB backend adapters registered.
package builtin

import (
	"context"
	"fmt"

	changefeedmongodb "github.com/LerianStudio/matcher/pkg/systemplane/adapters/changefeed/mongodb"
	changefeedpostgres "github.com/LerianStudio/matcher/pkg/systemplane/adapters/changefeed/postgres"
	mongodbstore "github.com/LerianStudio/matcher/pkg/systemplane/adapters/store/mongodb"
	postgresstore "github.com/LerianStudio/matcher/pkg/systemplane/adapters/store/postgres"
	"github.com/LerianStudio/matcher/pkg/systemplane/bootstrap"
	"github.com/LerianStudio/matcher/pkg/systemplane/domain"
)

func init() {
	if err := bootstrap.RegisterBackendFactory(domain.BackendPostgres, func(ctx context.Context, cfg *bootstrap.BootstrapConfig) (*bootstrap.BackendResources, error) {
		store, history, closer, err := postgresstore.New(ctx, cfg.Postgres, cfg.Secrets)
		if err != nil {
			return nil, fmt.Errorf("postgres backend: %w", err)
		}

		feed := changefeedpostgres.New(
			cfg.Postgres.DSN,
			cfg.Postgres.NotifyChannel,
			changefeedpostgres.WithRevisionSource(cfg.Postgres.Schema, cfg.Postgres.RevisionTable),
		)

		return &bootstrap.BackendResources{Store: store, History: history, ChangeFeed: feed, Closer: closer}, nil
	}); err != nil {
		bootstrap.RecordInitError(fmt.Errorf("register postgres backend: %w", err))
	}

	if err := bootstrap.RegisterBackendFactory(domain.BackendMongoDB, func(ctx context.Context, cfg *bootstrap.BootstrapConfig) (*bootstrap.BackendResources, error) {
		store, history, closer, err := mongodbstore.New(ctx, *cfg.MongoDB, cfg.Secrets)
		if err != nil {
			return nil, fmt.Errorf("mongodb backend: %w", err)
		}

		feed := changefeedmongodb.New(store.EntriesCollection(), cfg.MongoDB.WatchMode, cfg.MongoDB.PollInterval)

		return &bootstrap.BackendResources{Store: store, History: history, ChangeFeed: feed, Closer: closer}, nil
	}); err != nil {
		bootstrap.RecordInitError(fmt.Errorf("register mongodb backend: %w", err))
	}
}

// NewBackendFromConfig constructs backend resources using the built-in adapter
// registrations for PostgreSQL and MongoDB.
func NewBackendFromConfig(ctx context.Context, cfg *bootstrap.BootstrapConfig) (*bootstrap.BackendResources, error) {
	res, err := bootstrap.NewBackendFromConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("builtin backend: %w", err)
	}

	return res, nil
}
