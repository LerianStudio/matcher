// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

// Package ports defines shared interfaces (ports) for infrastructure abstraction.
package ports

import "database/sql"

// Tx is the shared transaction alias for repository interfaces.
type Tx = *sql.Tx
