// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package command

import (
	"context"
	"database/sql"
	"testing"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/configuration/domain/entities"
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
	"github.com/LerianStudio/matcher/internal/shared/infrastructure/testutil"
)

type contextRepoTxStub struct {
	*contextRepoStub
	createWithTxFn   func(context.Context, *sql.Tx, *entities.ReconciliationContext) (*entities.ReconciliationContext, error)
	findByIDWithTxFn func(context.Context, *sql.Tx, uuid.UUID) (*entities.ReconciliationContext, error)
}

func (stub *contextRepoTxStub) CreateWithTx(
	ctx context.Context,
	tx *sql.Tx,
	entity *entities.ReconciliationContext,
) (*entities.ReconciliationContext, error) {
	if stub.createWithTxFn != nil {
		return stub.createWithTxFn(ctx, tx, entity)
	}

	return nil, errCreateNotImplemented
}

func (stub *contextRepoTxStub) FindByIDWithTx(
	ctx context.Context,
	tx *sql.Tx,
	identifier uuid.UUID,
) (*entities.ReconciliationContext, error) {
	if stub.findByIDWithTxFn != nil {
		return stub.findByIDWithTxFn(ctx, tx, identifier)
	}

	return nil, errFindByIDNotImplemented
}

type sourceRepoTxStub struct {
	*sourceRepoStub
	createWithTxFn func(context.Context, *sql.Tx, *entities.ReconciliationSource) (*entities.ReconciliationSource, error)
}

func (stub *sourceRepoTxStub) CreateWithTx(
	ctx context.Context,
	tx *sql.Tx,
	entity *entities.ReconciliationSource,
) (*entities.ReconciliationSource, error) {
	if stub.createWithTxFn != nil {
		return stub.createWithTxFn(ctx, tx, entity)
	}

	return nil, errCreateNotImplemented
}

type fieldMapRepoTxStub struct {
	*fieldMapRepoStub
	createWithTxFn func(context.Context, *sql.Tx, *shared.FieldMap) (*shared.FieldMap, error)
}

func (stub *fieldMapRepoTxStub) CreateWithTx(
	ctx context.Context,
	tx *sql.Tx,
	entity *shared.FieldMap,
) (*shared.FieldMap, error) {
	if stub.createWithTxFn != nil {
		return stub.createWithTxFn(ctx, tx, entity)
	}

	return nil, errCreateNotImplemented
}

type matchRuleRepoTxStub struct {
	*matchRuleRepoStub
	createWithTxFn func(context.Context, *sql.Tx, *entities.MatchRule) (*entities.MatchRule, error)
}

func (stub *matchRuleRepoTxStub) CreateWithTx(
	ctx context.Context,
	tx *sql.Tx,
	entity *entities.MatchRule,
) (*entities.MatchRule, error) {
	if stub.createWithTxFn != nil {
		return stub.createWithTxFn(ctx, tx, entity)
	}

	return nil, errCreateNotImplemented
}

func setupInfraProviderWithSQLMock(t *testing.T) (*testutil.MockInfrastructureProvider, sqlmock.Sqlmock, *sql.DB) {
	t.Helper()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	provider := testutil.NewMockProviderFromDB(t, db)

	return provider, mock, db
}
