# Matching Context

The `internal/matching` bounded context is the core engine of the Matcher service. It executes reconciliation logic, applying configured rules to datasets to identify matches, variances, and exceptions.

## Overview

This context handles:
1. **Match Runs**: Orchestrating the end-to-end matching process (fetching candidates, locking, matching, persisting).
2. **Rule Execution**: Applying deterministic and tolerance-based rules (Exact Match, Date Window, Amount Tolerance, One-to-Many).
3. **Fee Verification**: Validating transaction fees against configured rates and tolerances.
4. **Locking**: Ensuring transactions are not matched by concurrent processes using distributed locks via Redis.
5. **Persistence**: Saving `MatchRun`, `MatchGroup`, `MatchItem`, `Adjustment`, and `FeeVariance` records.
6. **Manual Match / Unmatch**: Creating manual matches or breaking existing ones.
7. **Adjustments**: Creating amount adjustments (write-off, correction) for matched groups.
8. **Currency Conversion**: Cross-currency matching with FX rates.
9. **Confidence Scoring**: Scoring match quality (0–100) based on rule satisfaction.

## Architecture

### Hexagonal Layers

```
internal/matching/
├── adapters/
│   ├── http/                # Match execution, manual match, unmatch, adjustment handlers
│   │   └── dto/             # Response DTOs
│   ├── postgres/            # Repositories
│   │   ├── adjustment/      # Adjustment repository
│   │   ├── exception_creator/ # Exception creator adapter
│   │   ├── fee_schedule/    # Fee schedule reader
│   │   ├── fee_variance/    # Fee variance repository
│   │   ├── match_group/     # Match group repository
│   │   ├── match_item/      # Match item repository
│   │   ├── match_run/       # Match run repository
│   │   └── rate/            # Fee rate repository
│   ├── rabbitmq/            # Event publisher
│   └── redis/               # Distributed lock manager
├── domain/
│   ├── entities/            # MatchRun, MatchGroup, MatchItem, Adjustment, FeeVariance, events
│   ├── enums/               # Exception reason codes
│   ├── repositories/        # Repository interfaces
│   ├── services/            # Core matching engine: rule engine, confidence scorer, allocation
│   │                        #   (incl. failure handling), currency conversion, date math,
│   │                        #   date lag evaluator, deterministic sort, exact/tolerance evaluators,
│   │                        #   rule config decode, rule definition types
│   └── value_objects/       # ConfidenceScore, FxRate, MatchGroupStatus, MatchRunMode/Status
├── ports/                   # ContextProvider, EventPublisher, ExceptionCreator, FxSource,
│                            #   LockManager, MatchRuleProvider, SourceProvider, TransactionRepository
└── services/
    ├── command/             # RunMatch, ManualMatch, Unmatch, Adjustment, rule execution commands
    └── query/               # Match run, group, and item queries
```

## Domain Model

### Entities

1. **MatchRun**: Represents a single execution of the engine.
   - **Modes**: `DRY_RUN` (simulate only) or `COMMIT` (persist results).
   - **Status**: `PROCESSING` -> `COMPLETED` / `FAILED`.
   - **Stats**: Tracks matched count, unmatched count, auto-matched vs. pending review.

2. **MatchGroup**: A set of matched transactions.
   - Contains 1+ Left items and 1+ Right items.
   - **Status**: `CONFIRMED` (auto-matched) or `PROPOSED` (needs review).
   - **Score**: Confidence score (0–100).

3. **MatchItem**: A link between a `MatchGroup` and a `Transaction`.
   - Tracks allocated amount (for partial matches).

4. **Adjustment**: Represents amount adjustments to matched groups (write-off, correction).

5. **FeeVariance**: Records fee verification results and variances between expected and actual fees.

### Fee Verification

The engine verifies fees if a `RateID` is configured on the context.
- **Process**: Extracts actual fee from transaction metadata -> Calculates expected fee based on `Rate` -> Compares delta against `Tolerance`.
- **Outcome**:
  - Within tolerance: Pass.
  - Outside tolerance: Creates a `FeeVariance` record and optionally an exception.

## Domain Services

- **Rule Engine**: Evaluates match rules against transaction pairs.
- **Confidence Scorer**: Scores match quality (0–100) based on how well transactions satisfy configured rules.
- **Allocation Engine**: Handles partial and many-to-many allocation strategies for distributing amounts across match items.
- **Currency Conversion**: Cross-currency matching with FX rate lookups and base-currency normalization.
- **Deterministic Sorting**: Ensures consistent match ordering across runs for reproducible results.
- **Date Math**: Date window evaluation with lag tolerance for handling settlement delays.
- **Fee Normalization**: Net-to-gross fee conversion during rule execution for accurate fee comparison.

> **Note**: The core matching engine implementation lives in `domain/services/` (not `services/command/`). This includes `engine.go` (orchestrator), `allocation.go` / `allocation_failure.go` (amount distribution), `confidence_scorer.go`, the evaluators (`exact_evaluator.go`, `tolerance_evaluator.go`, `date_lag_evaluator.go`), `currency_conversion.go`, `date_math.go`, `deterministic_sort.go`, and rule configuration decoding (`rule_config_decode.go`, `rule_definition.go`). The `services/command/` layer orchestrates use cases (run, manual match, unmatch, adjustments) that delegate to these domain services.

## Matching Process

1. **Validation**: Check context state, sources, and dependencies.
2. **Fetch Candidates**: Load unmatched transactions for the requested period.
3. **Locking**: Acquire a distributed lock on candidate transaction IDs to prevent race conditions.
4. **Rule Execution**: Iterate through configured `MatchRules`.
   - **Exact**: Fields must match exactly.
   - **Tolerance**: Numeric fields within +/- range.
   - **Window**: Date fields within time window.
5. **Proposal Generation**: Create `MatchProposals`.
6. **Confidence Scoring**: Score each proposal based on rule satisfaction.
7. **Processing**: Convert proposals to `MatchGroups` and `MatchItems`.
8. **Commit**:
   - If `DRY_RUN`: Return stats and groups without persistence.
   - If `COMMIT`: Persist groups, update transaction status to `MATCHED` (or `PENDING_REVIEW`), create exceptions for unmatched items, and emit events.

## API Endpoints

| Method | Path | Description |
|--------|------|-------------|
| POST | `/v1/matching/contexts/:contextId/run` | Run matching engine |
| GET | `/v1/matching/contexts/:contextId/runs` | List match runs |
| GET | `/v1/matching/runs/:runId` | Get match run details |
| GET | `/v1/matching/runs/:runId/groups` | Get match run results |
| DELETE | `/v1/matching/groups/:matchGroupId` | Unmatch a group |
| POST | `/v1/matching/manual` | Create manual match |
| POST | `/v1/matching/adjustments` | Create adjustment |
