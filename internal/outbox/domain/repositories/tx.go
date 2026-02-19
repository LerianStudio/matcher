// Package repositories provides outbox repository abstractions, including the
// Tx alias used to represent database transactions.
package repositories

import "database/sql"

// Tx is an alias for database transaction used by the outbox repository.
type Tx = *sql.Tx
