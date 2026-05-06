# Context 01: Card Acquirer Settlement

## Business Story

This context models card transaction reconciliation between a Midaz ledger export and an acquirer settlement file. Optional auxiliary files add fee detail and chargeback activity so users can test gross-vs-net handling, fee variance review, and negative chargeback flows.

## Files

| File | Represents | Shape |
| --- | --- | --- |
| `data/ledger-midaz.csv` | Midaz ledger card sale and chargeback postings | 1000 CSV rows plus header |
| `data/acquirer-settlement.json` | Acquirer settlement records with gross, fee, and net amounts | 1000 JSON objects in a top-level array |
| `data/acquirer-fees.csv` | Acquirer fee line details by order and settlement batch | 1000 CSV rows plus header |
| `data/chargebacks.csv` | Chargeback events linked to original authorizations where available | 1000 CSV rows plus header |

The `scenario` column/key is metadata to explain why a row exists. It is not required for canonical mapping, but it is useful during demos and manual review.

## Suggested LEFT/RIGHT Setup

Primary reconciliation:

| Side | Source |
| --- | --- |
| LEFT | `ledger-midaz.csv` |
| RIGHT | `acquirer-settlement.json` |

Optional auxiliary checks:

| Purpose | Source |
| --- | --- |
| Fee verification | `acquirer-fees.csv` |
| Chargeback review | `chargebacks.csv` |

## Suggested Field Maps

| File | `external_id` | `amount` | `currency` | `date` | `description` | Metadata worth keeping |
| --- | --- | --- | --- | --- | --- | --- |
| `ledger-midaz.csv` | `ledger_tx_id` | `amount` | `currency` | `occurred_at` | `description` | `order_id`, `merchant_id`, `authorization_code`, `scenario` |
| `acquirer-settlement.json` | `acquirer_tx_id` | `gross_amount` or `net_amount` depending on the rule | `currency` | `settlement_date` | empty or generated from `order_id` | `order_id`, `fee_amount`, `settlement_id`, `merchant_id`, `authorization_code`, `scenario` |
| `acquirer-fees.csv` | `fee_id` | `fee_amount` | fixed `BRL` for BRL demos or metadata-only | `charged_at` | `fee_type` | `order_id`, `settlement_id`, `scenario` |
| `chargebacks.csv` | `chargeback_id` | `amount` | `currency` | `occurred_at` | `reason_code` | `order_id`, `authorization_code`, `scenario` |

## Suggested Match Rules

| Rule | Intent |
| --- | --- |
| Exact `order_id` plus `authorization_code` | Strongest match between ledger sales and acquirer settlements |
| Same `order_id`, same `currency`, amount tolerance up to `0.05` | Finds small rounding or cent-level tolerance candidates |
| Same `order_id`, date window `0..4` days between `occurred_at` and `settlement_date` | Handles D+1 to D+4 settlement lag |
| Compare ledger `amount` to acquirer `gross_amount` | Sale amount reconciliation |
| Compare acquirer `fee_amount` to `acquirer-fees.csv.fee_amount` by `settlement_id` and `order_id` | Fee variance review |
| Negative ledger or chargeback amount by `order_id` and `authorization_code` | Chargeback linkage and exception review |
| Duplicate detection on repeated `order_id` in acquirer settlement | Duplicate settlement candidate review |

## Expected Outcomes

Most early `ledger-midaz.csv` sale rows have matching settlement rows by `order_id` and `authorization_code`. A smaller population deliberately has settlement lag, amount tolerance, fee variance, duplicate settlement, ledger-only, acquirer-only, fee-only, and chargeback-only scenarios.

Approximate useful populations:

| Population | Expected behavior |
| --- | --- |
| `exact_order_and_authorization_match` | Auto-match candidates |
| `amount_tolerance_candidate` | Match if tolerance rules are enabled |
| `settlement_lag_candidate` | Match if date-window rules are enabled |
| `amount_mismatch_outside_tolerance` | Exception or manual review |
| `duplicate_settlement_candidate` | Duplicate exception review |
| `ledger_only`, `acquirer_only_settlement`, `fee_only_unmatched`, `chargeback_only_unmatched` | Unmatched records |

## Concrete Keys To Try

| Key | Why it matters |
| --- | --- |
| `ORD-C01-000001` with `AUTH-C01-000001` | Should match between `ledger-midaz.csv` and `acquirer-settlement.json` |
| `SET-C01-0001` | Appears in settlement and fee files, useful for fee validation |
| `ORD-C01-000120` | Appears in ledger and chargebacks, useful for negative chargeback review |
| `ORD-C01-002881` | Ledger-only order from `ledger-midaz.csv` |
| `ORD-C01-003851` | Acquirer-only settlement from `acquirer-settlement.json` |
