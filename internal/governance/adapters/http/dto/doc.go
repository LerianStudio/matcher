// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

// Package dto provides Data Transfer Objects for governance HTTP API responses.
//
// DTOs decouple the external API contract from the internal domain model.
// This separation allows:
//   - Independent evolution of domain and API
//   - Stable API contracts for external consumers
//   - Clean SDK generation without domain type issues
//   - Consistent JSON serialization (strings for UUIDs, RFC3339 for timestamps)
//
// Conventions:
//   - Response DTOs are suffixed with "Response"
//   - Use primitive types (string for UUID, decimal, timestamps)
//   - Include swagger annotations for documentation
//   - Never embed domain entities
//   - Converters return empty structs for nil inputs (not nil)
//   - List responses return [] not null for empty collections
package dto
