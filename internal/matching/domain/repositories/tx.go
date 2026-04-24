// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

// Package repositories provides matching persistence abstractions.
package repositories

import sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"

// Tx is the canonical shared SQL transaction alias.
type Tx = sharedPorts.Tx
