// Package repositories defines repository interfaces for the Configuration bounded context.
// These interfaces specify persistence contracts for domain aggregates, following the
// Hexagonal Architecture pattern where the domain defines its own storage requirements.
//
// The package exports the following repository interfaces:
//
//   - [ContextRepository]: Manages [entities.ReconciliationContext] aggregates, which define
//     the scope and configuration for reconciliation operations (e.g., account-to-account,
//     payment gateway). Supports CRUD operations plus filtering by context type and status.
//
//   - [SourceRepository]: Manages [entities.ReconciliationSource] aggregates, representing
//     data sources within a reconciliation context (internal ledger, external gateway).
//     Operations are scoped to a parent context ID.
//
//   - [FieldMapRepository]: Manages [shared.FieldMap] entities that define field mappings
//     from raw source data to normalized transaction fields. Supports lookup by source ID
//     and batch existence checks for validation.
//
//   - [MatchRuleRepository]: Manages [entities.MatchRule] entities that define matching
//     criteria and thresholds for transaction reconciliation. Supports priority-based
//     ordering, filtering by rule type, and bulk priority reordering.
//
//   - [FeeRuleRepository]: Manages [fee.FeeRule] entities that map transaction metadata
//     predicates to fee schedules. Rules are evaluated in priority order during match
//     runs to determine which fee schedule applies to each transaction side.
//
// Implementations of these interfaces are provided in the adapters/postgres package,
// which handles tenant isolation, transaction management, and cursor-based pagination.
// Use cases in the services/command and services/query packages depend on these
// interfaces for database-agnostic persistence.
package repositories
