// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package bootstrap

import (
	"database/sql"
	"reflect"
	"unsafe"

	"github.com/bxcodec/dbresolver/v2"

	libPostgres "github.com/LerianStudio/lib-commons/v4/commons/postgres"
)

// newLibPostgresClientFromResolver creates a *libPostgres.Client with a pre-injected
// dbresolver.DB for use as a bridge between the canonical tmpostgres.Manager and the
// InfrastructureProvider lease system.
//
// This is necessary because:
//   - InfrastructureProvider returns PostgresConnectionLease which wraps *libPostgres.Client
//   - tmpostgres.Manager.GetConnection returns PostgresConnection with an unexported client field
//   - PostgresConnection exposes the dbresolver.DB via GetDB() but not the *libPostgres.Client
//   - All 96 PG repositories depend on the lease.Resolver(ctx) call chain
//
// This bridge will be removed when repositories are migrated to use
// core.ResolvePostgres(ctx, fallback) directly (future gate).
func newLibPostgresClientFromResolver(resolver dbresolver.DB) *libPostgres.Client {
	pgClient := &libPostgres.Client{}

	if resolver == nil {
		return pgClient
	}

	rv := reflect.ValueOf(pgClient).Elem()

	// Inject the resolver field
	rf := rv.FieldByName("resolver")
	if rf.IsValid() {
		ptr := unsafe.Pointer(rf.UnsafeAddr()) //#nosec G103 -- bridge infrastructure: inject resolver into lib-commons Client
		*(*dbresolver.DB)(ptr) = resolver
	}

	// Inject the primary field from the resolver's primary DBs
	primaryField := rv.FieldByName("primary")
	if primaryField.IsValid() {
		primaryDBs := resolver.PrimaryDBs()
		if len(primaryDBs) > 0 {
			pPtr := unsafe.Pointer(primaryField.UnsafeAddr()) //#nosec G103 -- bridge infrastructure: inject primary DB
			*(**sql.DB)(pPtr) = primaryDBs[0]
		}
	}

	// Inject the replica field from the resolver's replica DBs
	replicaField := rv.FieldByName("replica")
	if replicaField.IsValid() {
		replicaDBs := resolver.ReplicaDBs()
		if len(replicaDBs) > 0 {
			rPtr := unsafe.Pointer(replicaField.UnsafeAddr()) //#nosec G103 -- bridge infrastructure: inject replica DB
			*(**sql.DB)(rPtr) = replicaDBs[0]
		}
	}

	return pgClient
}
