// Package common provides shared utilities for postgres adapters.
package common

import "errors"

// ErrTransactionRequired indicates a database transaction is required for this operation.
// This is the canonical definition; adapter packages re-export it for backward compatibility.
var ErrTransactionRequired = errors.New("transaction is required")
