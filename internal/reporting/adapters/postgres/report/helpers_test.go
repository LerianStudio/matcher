// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package report

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/shared/infrastructure/testutil"
	"github.com/LerianStudio/matcher/internal/shared/ports"
)

var (
	errTestScanFailed                = errors.New("scan failed")
	errTestUnexpectedScanValuesCount = errors.New("unexpected scan values count")
	errTestUnsupportedScanDestType   = errors.New("unsupported scan dest type")
	errTestValueNotUUID              = errors.New("value is not uuid.UUID")
	errTestValueNotString            = errors.New("value is not string")
	errTestValueNotDecimal           = errors.New("value is not decimal.Decimal")
)

type mockInfrastructureProvider struct{}

func (m *mockInfrastructureProvider) GetRedisConnection(
	_ context.Context,
) (*ports.RedisConnectionLease, error) {
	return nil, nil
}

func (m *mockInfrastructureProvider) BeginTx(_ context.Context) (*ports.TxLease, error) {
	return nil, nil
}

func (m *mockInfrastructureProvider) GetReplicaDB(_ context.Context) (*ports.DBLease, error) {
	return nil, nil
}

func (m *mockInfrastructureProvider) GetPrimaryDB(_ context.Context) (*ports.DBLease, error) {
	return nil, nil
}

type fakeVarianceScanner struct {
	values []any
	err    error
}

func (scanner fakeVarianceScanner) Scan(dest ...any) error {
	if scanner.err != nil {
		return scanner.err
	}

	if len(dest) != len(scanner.values) {
		return errTestUnexpectedScanValuesCount
	}

	for idx, value := range scanner.values {
		switch target := dest[idx].(type) {
		case *uuid.UUID:
			v, ok := value.(uuid.UUID)
			if !ok {
				return fmt.Errorf("value at index %d: %w", idx, errTestValueNotUUID)
			}

			*target = v
		case *string:
			v, ok := value.(string)
			if !ok {
				return fmt.Errorf("value at index %d: %w", idx, errTestValueNotString)
			}

			*target = v
		case *decimal.Decimal:
			v, ok := value.(decimal.Decimal)
			if !ok {
				return fmt.Errorf("value at index %d: %w", idx, errTestValueNotDecimal)
			}

			*target = v
		default:
			return fmt.Errorf("%w: %T", errTestUnsupportedScanDestType, dest[idx])
		}
	}

	return nil
}

type fakeMatchedScanner struct {
	values []any
	err    error
}

func (scanner *fakeMatchedScanner) Scan(dest ...any) error {
	if scanner.err != nil {
		return scanner.err
	}

	if len(dest) != len(scanner.values) {
		return errTestUnexpectedScanValuesCount
	}

	for idx, value := range scanner.values {
		switch target := dest[idx].(type) {
		case *uuid.UUID:
			v, ok := value.(uuid.UUID)
			if !ok {
				return fmt.Errorf("value at index %d: %w", idx, errTestValueNotUUID)
			}

			*target = v
		case *string:
			v, ok := value.(string)
			if !ok {
				return fmt.Errorf("value at index %d: %w", idx, errTestValueNotString)
			}

			*target = v
		case *decimal.Decimal:
			v, ok := value.(decimal.Decimal)
			if !ok {
				return fmt.Errorf("value at index %d: %w", idx, errTestValueNotDecimal)
			}

			*target = v
		case *time.Time:
			v, ok := value.(time.Time)
			if !ok {
				return fmt.Errorf("value at index %d: expected time.Time", idx)
			}

			*target = v
		default:
			return fmt.Errorf("%w: %T", errTestUnsupportedScanDestType, dest[idx])
		}
	}

	return nil
}

type fakeUnmatchedScanner struct {
	values []any
	err    error
}

func (scanner *fakeUnmatchedScanner) Scan(dest ...any) error {
	if scanner.err != nil {
		return scanner.err
	}

	if len(dest) != len(scanner.values) {
		return errTestUnexpectedScanValuesCount
	}

	for idx, value := range scanner.values {
		switch target := dest[idx].(type) {
		case *uuid.UUID:
			v, ok := value.(uuid.UUID)
			if !ok {
				return fmt.Errorf("value at index %d: %w", idx, errTestValueNotUUID)
			}

			*target = v
		case *string:
			v, ok := value.(string)
			if !ok {
				return fmt.Errorf("value at index %d: %w", idx, errTestValueNotString)
			}

			*target = v
		case *decimal.Decimal:
			v, ok := value.(decimal.Decimal)
			if !ok {
				return fmt.Errorf("value at index %d: %w", idx, errTestValueNotDecimal)
			}

			*target = v
		case *time.Time:
			v, ok := value.(time.Time)
			if !ok {
				return fmt.Errorf("value at index %d: expected time.Time", idx)
			}

			*target = v
		case **uuid.UUID:
			if value == nil {
				*target = nil
			} else {
				v, ok := value.(*uuid.UUID)
				if !ok {
					return fmt.Errorf("value at index %d: expected *uuid.UUID", idx)
				}

				*target = v
			}
		case **time.Time:
			if value == nil {
				*target = nil
			} else {
				v, ok := value.(*time.Time)
				if !ok {
					return fmt.Errorf("value at index %d: expected *time.Time", idx)
				}

				*target = v
			}
		default:
			return fmt.Errorf("%w: %T", errTestUnsupportedScanDestType, dest[idx])
		}
	}

	return nil
}

func setupReportRepository(t *testing.T) (*Repository, sqlmock.Sqlmock, func()) {
	t.Helper()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	provider := testutil.NewMockProviderFromDB(t, db)
	repo := NewRepository(provider)

	finish := func() {
		mock.ExpectClose()
		require.NoError(t, db.Close())
		require.NoError(t, mock.ExpectationsWereMet())
	}

	return repo, mock, finish
}
