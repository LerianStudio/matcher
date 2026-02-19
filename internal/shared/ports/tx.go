// Package ports defines shared interfaces (ports) for infrastructure abstraction.
package ports

import "database/sql"

// Tx is the shared transaction alias for repository interfaces.
type Tx = *sql.Tx
