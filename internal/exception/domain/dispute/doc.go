// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

// Package dispute provides domain types for managing dispute lifecycles.
//
// A dispute represents a formal challenge to a reconciliation outcome.
// The dispute workflow follows a state machine with the following states:
//
//   - Draft: Initial state when a dispute is being prepared
//   - Open: Dispute has been submitted and is under review
//   - PendingEvidence: Additional evidence has been requested
//   - Won: Dispute resolved in favor of the challenger (terminal)
//   - Lost: Dispute resolved against the challenger (terminal, can be reopened)
//
// State transitions follow specific rules to ensure a valid dispute lifecycle.
package dispute
