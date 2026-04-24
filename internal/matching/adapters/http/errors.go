// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

// Package http provides HTTP adapters for the matching module.
package http

import "errors"

// ErrRunModeRequired indicates the run mode is missing.
var ErrRunModeRequired = errors.New("mode is required")

// ErrTransactionIDsRequired indicates the transaction IDs are missing.
var ErrTransactionIDsRequired = errors.New("transaction_ids is required")
