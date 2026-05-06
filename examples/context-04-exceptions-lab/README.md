# Context 04: Exceptions Lab

## Business Story

This context is a compact exception lab with three 1000-row source files. It is designed to stress manual review flows without making most rows invalid: missing references are blank but parseable, dates remain ISO/RFC3339-compatible, amounts remain numeric, and currencies stay valid ISO codes.

## Files

| File | Represents | Shape |
| --- | --- | --- |
| `data/ledger.csv` | Ledger-side transfers and adjustments | 1000 CSV rows plus header |
| `data/external-bank.csv` | External bank records for the same operating flow | 1000 CSV rows plus header |
| `data/gateway.csv` | Gateway captures for the same reference universe | 1000 CSV rows plus header |

The `scenario` metadata identifies missing references, duplicate references, typos, amount mismatches, negative amounts, and unmatched records.

## Suggested LEFT/RIGHT Setup

Bank exception demo:

| Side | Source |
| --- | --- |
| LEFT | `ledger.csv` |
| RIGHT | `external-bank.csv` |

Gateway exception demo:

| Side | Source |
| --- | --- |
| LEFT | `ledger.csv` |
| RIGHT | `gateway.csv` |

## Suggested Field Maps

| File | `external_id` | `amount` | `currency` | `date` | `description` | Metadata worth keeping |
| --- | --- | --- | --- | --- | --- | --- |
| `ledger.csv` | `ledger_tx_id` | `amount` | `currency` | `occurred_at` | `description` | `reference`, `customer_ref`, `scenario` |
| `external-bank.csv` | `bank_tx_id` | `amount` | `currency` | `booking_date` | `description` | `reference`, `customer_ref`, `scenario` |
| `gateway.csv` | `gateway_tx_id` | `amount` | `currency` | `captured_at` | `description` | `reference`, `authorization_code`, `scenario` |

## Suggested Match Rules

| Rule | Intent |
| --- | --- |
| Exact `reference` plus exact `amount` and `currency` | Baseline auto-match |
| Exact `customer_ref` plus same amount/date window when `reference` is blank | Missing-reference fallback for bank records |
| Same `reference`, amount tolerance up to `0.05` | Near-match amount tolerance |
| Same `reference`, date window `0..2` days | Bank booking and gateway capture lag |
| Fuzzy `description` similarity | Catches typo scenarios like `trasnfer` vs `transfer` |
| Duplicate detection by `reference` | Flags repeated references across source files |
| Negative amount rule | Sends reversals or corrections to review |

## Expected Outcomes

The first major block of rows can match by `reference`; several rows need fallback metadata, amount tolerance, or date-window rules. Duplicate references, blank references, typo descriptions, amount mismatches outside tolerance, negative amounts, ledger-only rows, bank-only rows, and gateway-only rows should remain available for exception workflows.

Approximate useful populations:

| Population | Expected behavior |
| --- | --- |
| `exception_lab_match_candidate`, `bank_match_candidate`, `gateway_match_candidate` | Auto-match candidates |
| `small_tolerance_candidate` | Match only with tolerance rules |
| `near_match_description_typo` | Manual or fuzzy review candidate |
| `amount_mismatch_manual_review`, `gateway_amount_mismatch` | Exception review |
| `duplicate_reference_candidate`, `duplicate_bank_reference`, `gateway_duplicate_reference` | Duplicate review |
| `missing_reference_manual_review`, `missing_bank_reference` | Fallback or manual review |
| `ledger_only_unmatched`, `bank_only_unmatched`, `gateway_only_unmatched` | Unmatched records |

## Concrete Keys To Try

| Key | Why it matters |
| --- | --- |
| `REF-C04-000001` | Should match across ledger, external bank, and gateway |
| `REF-C04-000047` | Small tolerance candidate in bank data |
| `REF-C04-000071` | Description typo candidate in bank data |
| `LED-C04-000101` | Ledger row with intentionally blank reference |
| `REF-C04-000901` | Ledger-only row |
| `REF-C04-006761` | Bank-only row |
