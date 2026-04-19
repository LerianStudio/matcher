// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package cross

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/google/uuid"

	libCommons "github.com/LerianStudio/lib-commons/v5/commons"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v5/commons/opentelemetry"

	pgcommon "github.com/LerianStudio/matcher/internal/shared/adapters/postgres/common"
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

// bridgeSourceResolverFormatJSON is the content format used when intake
// downstream of the bridge parses custody plaintext. Fetcher always emits
// JSON today, so the resolver hard-codes this. A future format expansion
// can read the format from source.config if needed.
const bridgeSourceResolverFormatJSON = "json"

// Sentinel errors for the bridge source resolver.
var (
	// ErrNilBridgeSourceResolverProvider indicates the adapter was
	// constructed without an infrastructure provider. Exported so
	// bootstrap can assert on it via errors.Is.
	ErrNilBridgeSourceResolverProvider = errors.New(
		"bridge source resolver requires infrastructure provider",
	)

	// ErrBridgeSourceResolverConnectionIDRequired indicates the caller
	// invoked ResolveSourceForConnection with the zero UUID. Exported so
	// callers can discriminate the validation error from genuine resolver
	// failures via errors.Is.
	ErrBridgeSourceResolverConnectionIDRequired = errors.New(
		"connection id is required for bridge source resolution",
	)

	// ErrMultipleFetcherSourcesForConnection indicates more than one
	// FETCHER-typed reconciliation source links to the same Fetcher
	// connection id. Migration 000032 enforces this as a DB-level unique
	// index; the code-level guard remains as a defense against legacy data
	// that predates the constraint and to surface the condition with a
	// typed error instead of silently picking the oldest row.
	ErrMultipleFetcherSourcesForConnection = errors.New(
		"multiple FETCHER reconciliation sources for the same connection id",
	)
)

// BridgeSourceResolverAdapter implements sharedPorts.BridgeSourceResolver.
// It queries reconciliation_sources for a FETCHER-typed source whose
// config->>'connection_id' matches the given Fetcher connection id, in the
// tenant schema resolved from ctx.
//
// Living in shared/adapters/cross respects the cross-context rule: the
// bridge worker (discovery) must not import configuration, so the
// resolver contract lives in shared/ports and the SQL implementation
// lives here in the shared kernel.
type BridgeSourceResolverAdapter struct {
	provider sharedPorts.InfrastructureProvider
}

// Compile-time interface check.
var _ sharedPorts.BridgeSourceResolver = (*BridgeSourceResolverAdapter)(nil)

// NewBridgeSourceResolverAdapter validates the provider and returns the
// adapter. Nil provider is a bootstrap bug, surfaced up-front.
func NewBridgeSourceResolverAdapter(
	provider sharedPorts.InfrastructureProvider,
) (*BridgeSourceResolverAdapter, error) {
	if provider == nil {
		return nil, ErrNilBridgeSourceResolverProvider
	}

	return &BridgeSourceResolverAdapter{provider: provider}, nil
}

// ResolveSourceForConnection finds the reconciliation_source whose config
// links to the given fetcher connection id. The query:
//
//	SELECT id, context_id
//	FROM reconciliation_sources
//	WHERE type = 'FETCHER'
//	  AND config->>'connection_id' = $1::text
//	ORDER BY created_at ASC
//	LIMIT 2
//
// runs inside a tenant-scoped transaction via WithTenantTxProvider, so it
// only sees rows for the tenant resolved from ctx (plus the default tenant
// when auth is disabled).
//
// Returns sharedPorts.ErrBridgeSourceUnresolvable when no match is found.
// Multiple matches for a single connection id are a config violation that
// migration 000032 now prevents at the DB layer via a partial unique index.
// The code path still reads up to two rows and returns
// ErrMultipleFetcherSourcesForConnection if a second row is seen — this
// protects against legacy data that predates the constraint and surfaces
// the condition with a typed error instead of silently picking the oldest
// row.
func (adapter *BridgeSourceResolverAdapter) ResolveSourceForConnection(
	ctx context.Context,
	connectionID uuid.UUID,
) (sharedPorts.BridgeSourceTarget, error) {
	_, tracer, _, _ := libCommons.NewTrackingFromContext(ctx) //nolint:dogsled

	ctx, span := tracer.Start(ctx, "cross.bridge_source_resolver.resolve")
	defer span.End()

	if adapter == nil || adapter.provider == nil {
		err := ErrNilBridgeSourceResolverProvider
		libOpentelemetry.HandleSpanError(span, "bridge source resolver not initialised", err)

		return sharedPorts.BridgeSourceTarget{}, err
	}

	if connectionID == uuid.Nil {
		err := ErrBridgeSourceResolverConnectionIDRequired
		libOpentelemetry.HandleSpanBusinessErrorEvent(span, "missing connection id", err)

		return sharedPorts.BridgeSourceTarget{}, err
	}

	result, err := pgcommon.WithTenantTxProvider(ctx, adapter.provider, func(tx *sql.Tx) (sharedPorts.BridgeSourceTarget, error) {
		return queryBridgeSourceTarget(ctx, tx, connectionID)
	})
	if err != nil {
		wrapped := fmt.Errorf("resolve source for connection: %w", err)

		switch {
		case errors.Is(err, sharedPorts.ErrBridgeSourceUnresolvable):
			libOpentelemetry.HandleSpanBusinessErrorEvent(span, "no fetcher source for connection", err)
		case errors.Is(err, ErrMultipleFetcherSourcesForConnection):
			libOpentelemetry.HandleSpanError(span, "multiple fetcher sources for connection", err)
		default:
			libOpentelemetry.HandleSpanError(span, "source resolver failed", wrapped)
		}

		return sharedPorts.BridgeSourceTarget{}, wrapped
	}

	return result, nil
}

// queryBridgeSourceTarget runs the tenant-scoped SQL to find the FETCHER
// reconciliation_source for a given Fetcher connection id. Extracted from
// ResolveSourceForConnection so the outer method stays within the cyclop
// complexity budget; behavior is identical.
func queryBridgeSourceTarget(
	ctx context.Context,
	tx *sql.Tx,
	connectionID uuid.UUID,
) (sharedPorts.BridgeSourceTarget, error) {
	rows, queryErr := tx.QueryContext(ctx,
		`SELECT id, context_id
		FROM reconciliation_sources
		WHERE type = 'FETCHER' AND config->>'connection_id' = $1::text
		ORDER BY created_at ASC
		LIMIT 2`,
		connectionID.String(),
	)
	if queryErr != nil {
		return sharedPorts.BridgeSourceTarget{}, fmt.Errorf("query bridge source target: %w", queryErr)
	}

	defer func() { _ = rows.Close() }()

	if !rows.Next() {
		if rowsErr := rows.Err(); rowsErr != nil {
			return sharedPorts.BridgeSourceTarget{}, fmt.Errorf("iterate bridge source target: %w", rowsErr)
		}

		return sharedPorts.BridgeSourceTarget{}, sharedPorts.ErrBridgeSourceUnresolvable
	}

	var sourceIDStr, contextIDStr string
	if scanErr := rows.Scan(&sourceIDStr, &contextIDStr); scanErr != nil {
		return sharedPorts.BridgeSourceTarget{}, fmt.Errorf("scan bridge source target: %w", scanErr)
	}

	// A second row is a uniqueness violation. Migration 000032 prevents
	// new duplicates at the DB layer; this guard catches legacy data
	// that predates the constraint.
	if rows.Next() {
		return sharedPorts.BridgeSourceTarget{}, ErrMultipleFetcherSourcesForConnection
	}

	if rowsErr := rows.Err(); rowsErr != nil {
		return sharedPorts.BridgeSourceTarget{}, fmt.Errorf("iterate bridge source target: %w", rowsErr)
	}

	sourceID, parseErr := uuid.Parse(sourceIDStr)
	if parseErr != nil {
		return sharedPorts.BridgeSourceTarget{}, fmt.Errorf("parse source id: %w", parseErr)
	}

	contextID, parseErr := uuid.Parse(contextIDStr)
	if parseErr != nil {
		return sharedPorts.BridgeSourceTarget{}, fmt.Errorf("parse context id: %w", parseErr)
	}

	return sharedPorts.BridgeSourceTarget{
		SourceID:  sourceID,
		ContextID: contextID,
		Format:    bridgeSourceResolverFormatJSON,
	}, nil
}
