// Package repositories provides exception repository abstractions, including the
// Tx alias used to represent database transactions.
package repositories

import sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"

// Tx is an alias for the shared transaction type used by repositories.
type Tx = sharedPorts.Tx
