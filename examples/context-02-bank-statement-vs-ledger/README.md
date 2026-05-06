# Context 02: Bank Statement Vs Ledger

## Business Story

This context models reconciliation between a Midaz ledger export and a bank statement. A third adjustment file lets users test operational adjustments, bank fee lines, and manual correction workflows.

## Files

| File | Represents | Shape |
| --- | --- | --- |
| `data/midaz-ledger-export.json` | Midaz ledger entries with E2E IDs, references, accounts, and posting timestamps | 1000 JSON objects in a top-level array |
| `data/bank-statement.csv` | Bank statement lines with booking dates and bank references | 1000 CSV rows plus header |
| `data/bank-adjustments.csv` | Manual adjustments and bank fee corrections | 1000 CSV rows plus header |

The `scenario` metadata explains whether a row is an exact candidate, missing-ID fallback candidate, duplicate, fee, adjustment, or unmatched record.

## Suggested LEFT/RIGHT Setup

Primary reconciliation:

| Side | Source |
| --- | --- |
| LEFT | `midaz-ledger-export.json` |
| RIGHT | `bank-statement.csv` |

Optional auxiliary source:

| Purpose | Source |
| --- | --- |
| Adjustment and bank fee review | `bank-adjustments.csv` |

## Suggested Field Maps

| File | `external_id` | `amount` | `currency` | `date` | `description` | Metadata worth keeping |
| --- | --- | --- | --- | --- | --- | --- |
| `midaz-ledger-export.json` | `ledger_entry_id` | `amount` | `currency` | `posted_at` | `description` | `end_to_end_id`, `account_id`, `reference`, `scenario` |
| `bank-statement.csv` | `bank_entry_id` | `amount` | `currency` | `booking_date` | `description` | `end_to_end_id`, `bank_account`, `reference`, `scenario` |
| `bank-adjustments.csv` | `adjustment_id` | `amount` | `currency` | `adjustment_date` | `reason` | `reference`, `end_to_end_id`, `scenario` |

## Suggested Match Rules

| Rule | Intent |
| --- | --- |
| Exact `end_to_end_id` when present | Strong auto-match for bank rails that preserve E2E IDs |
| Exact `reference` plus same `amount` and `currency` | Fallback when the bank or ledger side is missing E2E ID |
| Same `reference`, amount tolerance up to `0.05` | Finds cent-level bank rounding candidates |
| Same `reference`, date window `0..1` day between `posted_at` and `booking_date` | Handles D+1 bank booking lag |
| Duplicate detection on `end_to_end_id` or `reference` in bank statement | Flags duplicate bank lines |
| Adjustment review by `reference` or `end_to_end_id` | Links adjustment rows to ledger/bank exceptions |
| Negative amount review | Highlights fees, reversals, and correction rows |

## Expected Outcomes

The first large block of ledger and bank rows should match by E2E ID or reference. Some rows intentionally omit E2E ID and require reference fallback. Other rows create D+1 lag, duplicate bank lines, bank fees, amount mismatches, adjustment-only rows, ledger-only rows, and bank-only rows.

Approximate useful populations:

| Population | Expected behavior |
| --- | --- |
| `bank_statement_match_candidate` | Auto-match candidate by E2E ID/reference |
| `missing_e2e_uses_reference_fallback` | Match only if fallback metadata rules are configured |
| `amount_tolerance_candidate` | Match with tolerance rules |
| `amount_mismatch_outside_tolerance` | Manual review or exception |
| `duplicate_bank_line` | Duplicate exception review |
| `bank_fee_only`, `ledger_only`, `bank_only_unmatched`, `adjustment_only_unmatched` | Unmatched or auxiliary review |

## Concrete Keys To Try

| Key | Why it matters |
| --- | --- |
| `E2E-C02-000000001` | Should match between ledger JSON and bank CSV |
| `REF-C02-000083` | Bank row intentionally omits E2E ID and can match by reference fallback |
| `REF-C02-000120` | Appears in ledger, bank duplicate region, and adjustment data |
| `LED-C02-000881` / `REF-C02-000881` | Ledger-only row |
| `BANKONLY-C02-000841` | Bank-only statement row |
