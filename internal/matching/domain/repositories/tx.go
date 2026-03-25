// Package repositories provides matching persistence abstractions.
package repositories

import sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"

// Tx is the canonical shared SQL transaction alias.
type Tx = sharedPorts.Tx
