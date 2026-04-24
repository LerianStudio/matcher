// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

// Package repositories provides exception repository abstractions, including the
// Tx alias used to represent database transactions.
package repositories

import sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"

// Tx is an alias for the shared transaction type used by repositories.
type Tx = sharedPorts.Tx
