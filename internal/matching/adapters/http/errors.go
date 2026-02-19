// Package http provides HTTP adapters for the matching module.
package http

import "errors"

// ErrRunModeRequired indicates the run mode is missing.
var ErrRunModeRequired = errors.New("mode is required")

// ErrTransactionIDsRequired indicates the transaction IDs are missing.
var ErrTransactionIDsRequired = errors.New("transaction_ids is required")
