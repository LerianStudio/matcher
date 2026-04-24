// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package cross

import (
	"context"
	"database/sql"

	"github.com/google/uuid"

	libHTTP "github.com/LerianStudio/lib-commons/v5/commons/net/http"

	configEntities "github.com/LerianStudio/matcher/internal/configuration/domain/entities"
	configVO "github.com/LerianStudio/matcher/internal/configuration/domain/value_objects"
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

// mockInfraProvider is a minimal InfrastructureProvider stub used across
// adapter tests that only need a non-nil provider to pass constructor guards.
type mockInfraProvider struct {
	beginTxErr error
	beginTxFn  func(ctx context.Context) (*sql.Tx, error)
}

func (m *mockInfraProvider) GetRedisConnection(
	_ context.Context,
) (*sharedPorts.RedisConnectionLease, error) {
	return nil, nil
}

func (m *mockInfraProvider) BeginTx(ctx context.Context) (*sharedPorts.TxLease, error) {
	if m.beginTxFn != nil {
		tx, err := m.beginTxFn(ctx)
		if err != nil {
			return nil, err
		}

		return sharedPorts.NewTxLease(tx, nil), nil
	}

	return nil, m.beginTxErr
}

func (m *mockInfraProvider) GetReplicaDB(_ context.Context) (*sharedPorts.DBLease, error) {
	return nil, nil
}

func (m *mockInfraProvider) GetPrimaryDB(_ context.Context) (*sharedPorts.DBLease, error) {
	return nil, nil
}

// stubContextRepository implements configRepositories.ContextRepository
// for adapter tests. Shared across ingestion and auto-match adapter tests
// to avoid duplication.
type stubContextRepository struct {
	ctx *configEntities.ReconciliationContext
	err error
}

func (r *stubContextRepository) FindByID(
	_ context.Context,
	_ uuid.UUID,
) (*configEntities.ReconciliationContext, error) {
	return r.ctx, r.err
}

func (r *stubContextRepository) FindByName(
	_ context.Context,
	_ string,
) (*configEntities.ReconciliationContext, error) {
	return nil, nil
}

func (r *stubContextRepository) Create(
	_ context.Context,
	_ *configEntities.ReconciliationContext,
) (*configEntities.ReconciliationContext, error) {
	return nil, nil
}

func (r *stubContextRepository) Update(
	_ context.Context,
	_ *configEntities.ReconciliationContext,
) (*configEntities.ReconciliationContext, error) {
	return nil, nil
}

func (r *stubContextRepository) Delete(_ context.Context, _ uuid.UUID) error {
	return nil
}

func (r *stubContextRepository) FindAll(
	_ context.Context,
	_ string,
	_ int,
	_ *shared.ContextType,
	_ *configVO.ContextStatus,
) ([]*configEntities.ReconciliationContext, libHTTP.CursorPagination, error) {
	return nil, libHTTP.CursorPagination{}, nil
}

func (r *stubContextRepository) Count(_ context.Context) (int64, error) {
	return 0, nil
}
