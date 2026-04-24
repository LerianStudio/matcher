// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package command

import (
	"context"
	"database/sql"
	"time"

	"github.com/google/uuid"

	libHTTP "github.com/LerianStudio/lib-commons/v5/commons/net/http"

	"github.com/LerianStudio/matcher/internal/ingestion/domain/entities"
	"github.com/LerianStudio/matcher/internal/ingestion/domain/repositories"
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
)

type fakePublisher struct{ called bool }

func (f *fakePublisher) PublishIngestionCompleted(
	_ context.Context,
	_ *entities.IngestionCompletedEvent,
) error {
	f.called = true

	return nil
}

func (f *fakePublisher) PublishIngestionFailed(
	_ context.Context,
	_ *entities.IngestionFailedEvent,
) error {
	f.called = true

	return nil
}

type fakeJobRepo struct {
	createErr            error
	created              *entities.IngestionJob
	withTxErr            error
	updateErr            error
	updated              *entities.IngestionJob
	byExtraction         map[string]*entities.IngestionJob
	findByExtractionErr  error
	findByExtractionCall int
}

func (jobRepo *fakeJobRepo) Create(
	_ context.Context,
	_ *entities.IngestionJob,
) (*entities.IngestionJob, error) {
	return jobRepo.created, jobRepo.createErr
}

func (jobRepo *fakeJobRepo) FindByID(
	_ context.Context,
	_ uuid.UUID,
) (*entities.IngestionJob, error) {
	return nil, nil
}

func (jobRepo *fakeJobRepo) FindByContextID(
	_ context.Context,
	_ uuid.UUID,
	_ repositories.CursorFilter,
) ([]*entities.IngestionJob, libHTTP.CursorPagination, error) {
	return nil, libHTTP.CursorPagination{}, nil
}

func (jobRepo *fakeJobRepo) Update(
	_ context.Context,
	_ *entities.IngestionJob,
) (*entities.IngestionJob, error) {
	return nil, nil
}

func (jobRepo *fakeJobRepo) FindLatestByExtractionID(
	_ context.Context,
	extractionID uuid.UUID,
) (*entities.IngestionJob, error) {
	jobRepo.findByExtractionCall++

	if jobRepo.findByExtractionErr != nil {
		return nil, jobRepo.findByExtractionErr
	}

	if jobRepo.byExtraction == nil {
		return nil, nil
	}

	if existing, ok := jobRepo.byExtraction[extractionID.String()]; ok {
		return existing, nil
	}

	return nil, nil
}

func (jobRepo *fakeJobRepo) WithTx(_ context.Context, fn func(*sql.Tx) error) error {
	if jobRepo.withTxErr != nil {
		return jobRepo.withTxErr
	}

	return fn(&sql.Tx{})
}

func (jobRepo *fakeJobRepo) UpdateWithTx(
	_ context.Context,
	_ *sql.Tx,
	job *entities.IngestionJob,
) (*entities.IngestionJob, error) {
	if jobRepo.updateErr != nil {
		return nil, jobRepo.updateErr
	}

	if jobRepo.updated != nil {
		return jobRepo.updated, nil
	}

	return job, nil
}

type fakeOutboxRepo struct {
	createErr error
	created   *shared.OutboxEvent
}

func (f *fakeOutboxRepo) Create(
	_ context.Context,
	_ *shared.OutboxEvent,
) (*shared.OutboxEvent, error) {
	return f.created, f.createErr
}

func (f *fakeOutboxRepo) CreateWithTx(
	_ context.Context,
	_ *sql.Tx,
	_ *shared.OutboxEvent,
) (*shared.OutboxEvent, error) {
	return f.created, f.createErr
}

func (f *fakeOutboxRepo) ListPending(
	_ context.Context,
	_ int,
) ([]*shared.OutboxEvent, error) {
	return nil, nil
}

func (f *fakeOutboxRepo) ListPendingByType(
	_ context.Context,
	_ string,
	_ int,
) ([]*shared.OutboxEvent, error) {
	return nil, nil
}

func (f *fakeOutboxRepo) ListTenants(_ context.Context) ([]string, error) {
	return nil, nil
}

func (f *fakeOutboxRepo) GetByID(
	_ context.Context,
	_ uuid.UUID,
) (*shared.OutboxEvent, error) {
	return f.created, f.createErr
}

func (f *fakeOutboxRepo) MarkPublished(_ context.Context, _ uuid.UUID, _ time.Time) error {
	return nil
}

func (f *fakeOutboxRepo) MarkFailed(_ context.Context, _ uuid.UUID, _ string, _ int) error {
	return nil
}

func (f *fakeOutboxRepo) ListFailedForRetry(
	_ context.Context,
	_ int,
	_ time.Time,
	_ int,
) ([]*shared.OutboxEvent, error) {
	return nil, nil
}

func (f *fakeOutboxRepo) ResetForRetry(
	_ context.Context,
	_ int,
	_ time.Time,
	_ int,
) ([]*shared.OutboxEvent, error) {
	return nil, nil
}

func (f *fakeOutboxRepo) ResetStuckProcessing(
	_ context.Context,
	_ int,
	_ time.Time,
	_ int,
) ([]*shared.OutboxEvent, error) {
	return nil, nil
}

func (f *fakeOutboxRepo) MarkInvalid(_ context.Context, _ uuid.UUID, _ string) error {
	return nil
}

type fakeTxRepo struct {
	exists   bool
	batchErr error
}

func (t fakeTxRepo) Create(_ context.Context, _ *shared.Transaction) (*shared.Transaction, error) {
	return nil, nil
}

func (t fakeTxRepo) CreateBatch(
	_ context.Context,
	_ []*shared.Transaction,
) ([]*shared.Transaction, error) {
	return nil, t.batchErr
}

func (t fakeTxRepo) FindByID(_ context.Context, _ uuid.UUID) (*shared.Transaction, error) {
	return nil, nil
}

func (t fakeTxRepo) FindByJobID(
	_ context.Context,
	_ uuid.UUID,
	_ repositories.CursorFilter,
) ([]*shared.Transaction, libHTTP.CursorPagination, error) {
	return nil, libHTTP.CursorPagination{}, nil
}

func (t fakeTxRepo) FindByJobAndContextID(
	_ context.Context,
	_, _ uuid.UUID,
	_ repositories.CursorFilter,
) ([]*shared.Transaction, libHTTP.CursorPagination, error) {
	return nil, libHTTP.CursorPagination{}, nil
}

func (t fakeTxRepo) FindBySourceAndExternalID(
	_ context.Context,
	_ uuid.UUID,
	_ string,
) (*shared.Transaction, error) {
	return nil, nil
}

func (t fakeTxRepo) ExistsBySourceAndExternalID(
	_ context.Context,
	_ uuid.UUID,
	_ string,
) (bool, error) {
	return t.exists, nil
}

func (t fakeTxRepo) ExistsBulkBySourceAndExternalID(
	_ context.Context,
	_ []repositories.ExternalIDKey,
) (map[repositories.ExternalIDKey]bool, error) {
	return make(map[repositories.ExternalIDKey]bool), nil
}

func (t fakeTxRepo) UpdateStatus(
	_ context.Context,
	_, _ uuid.UUID,
	_ shared.TransactionStatus,
) (*shared.Transaction, error) {
	return nil, nil
}

func (t fakeTxRepo) SearchTransactions(
	_ context.Context,
	_ uuid.UUID,
	_ repositories.TransactionSearchParams,
) ([]*shared.Transaction, int64, error) {
	return nil, 0, nil
}

func (t fakeTxRepo) CleanupFailedJobTransactionsWithTx(
	_ context.Context,
	_ *sql.Tx,
	_ uuid.UUID,
) error {
	return nil
}

type fakeFieldMapRepo struct {
	fieldMap *shared.FieldMap
	err      error
}

func (f *fakeFieldMapRepo) FindBySourceID(
	_ context.Context,
	_ uuid.UUID,
) (*shared.FieldMap, error) {
	return f.fieldMap, f.err
}

type fakeSourceRepo struct {
	source *shared.ReconciliationSource
	err    error
}

func (f *fakeSourceRepo) FindByID(
	_ context.Context,
	_, _ uuid.UUID,
) (*shared.ReconciliationSource, error) {
	return f.source, f.err
}

type fakeContextProvider struct {
	enabled bool
	err     error
}

func (f fakeContextProvider) IsAutoMatchEnabled(_ context.Context, _ uuid.UUID) (bool, error) {
	return f.enabled, f.err
}

type fakeMatchTrigger struct {
	called    bool
	tenantID  uuid.UUID
	contextID uuid.UUID
}

func (f *fakeMatchTrigger) TriggerMatchForContext(_ context.Context, tenantID, contextID uuid.UUID) {
	f.called = true
	f.tenantID = tenantID
	f.contextID = contextID
}

func newTestDeps() UseCaseDeps {
	return UseCaseDeps{
		JobRepo:         &fakeJobRepo{},
		TransactionRepo: fakeTxRepo{},
		Dedupe:          fakeDedupe{},
		Publisher:       &fakePublisher{},
		OutboxRepo:      &fakeOutboxRepo{},
		Parsers:         fakeRegistry{parser: parserWithRange{}},
		FieldMapRepo:    &fakeFieldMapRepo{},
		SourceRepo:      &fakeSourceRepo{},
	}
}
