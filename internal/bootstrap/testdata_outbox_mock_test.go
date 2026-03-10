//go:build unit

package bootstrap

import (
	"context"
	"database/sql"
	"sync"
	"time"

	"github.com/google/uuid"

	sharedDomain "github.com/LerianStudio/matcher/internal/shared/domain"
)

// testOutboxMock is a shared mock for sharedPorts.OutboxRepository.
// It captures Create calls and supports error injection. Both config_api_test.go
// and config_audit_test.go use this type to avoid duplication (M17).
//
// Only Create is exercised by ConfigAuditPublisher; remaining methods are stubs
// returning zero values.
type testOutboxMock struct {
	mu            sync.Mutex
	createCalled  bool
	createdEvents []*sharedDomain.OutboxEvent
	createErr     error
}

func (m *testOutboxMock) Create(_ context.Context, event *sharedDomain.OutboxEvent) (*sharedDomain.OutboxEvent, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.createCalled = true

	if m.createErr != nil {
		return nil, m.createErr
	}

	m.createdEvents = append(m.createdEvents, event)

	return event, nil
}

func (m *testOutboxMock) CreateWithTx(_ context.Context, _ *sql.Tx, _ *sharedDomain.OutboxEvent) (*sharedDomain.OutboxEvent, error) {
	return nil, nil
}

func (m *testOutboxMock) ListPending(context.Context, int) ([]*sharedDomain.OutboxEvent, error) {
	return nil, nil
}

func (m *testOutboxMock) ListPendingByType(context.Context, string, int) ([]*sharedDomain.OutboxEvent, error) {
	return nil, nil
}

func (m *testOutboxMock) ListTenants(context.Context) ([]string, error) { return nil, nil }

func (m *testOutboxMock) GetByID(context.Context, uuid.UUID) (*sharedDomain.OutboxEvent, error) {
	return nil, nil
}

func (m *testOutboxMock) MarkPublished(_ context.Context, _ uuid.UUID, _ time.Time) error {
	return nil
}

func (m *testOutboxMock) MarkFailed(context.Context, uuid.UUID, string, int) error { return nil }

func (m *testOutboxMock) ListFailedForRetry(_ context.Context, _ int, _ time.Time, _ int) ([]*sharedDomain.OutboxEvent, error) {
	return nil, nil
}

func (m *testOutboxMock) ResetForRetry(_ context.Context, _ int, _ time.Time, _ int) ([]*sharedDomain.OutboxEvent, error) {
	return nil, nil
}

func (m *testOutboxMock) ResetStuckProcessing(_ context.Context, _ int, _ time.Time, _ int) ([]*sharedDomain.OutboxEvent, error) {
	return nil, nil
}

func (m *testOutboxMock) MarkInvalid(_ context.Context, _ uuid.UUID, _ string) error { return nil }
