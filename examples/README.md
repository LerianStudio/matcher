# Matcher Raw Example Datasets

These examples are synthetic raw source files for trying Matcher reconciliation flows. They are deliberately not configuration seed files. Upload the CSV and JSON files into Matcher, choose which source is LEFT or RIGHT, then configure field maps, match rules, fee rules, and manual review behavior inside the product.

Every data file contains exactly 1000 data rows or JSON objects. The files are generated deterministically by `examples/scripts/generate_examples.py`, so the same keys and scenarios are reproducible across machines.

## Folder Structure

| Folder | Story | Data files |
| --- | --- | --- |
| `context-01-card-acquirer-settlement/` | Midaz ledger vs card acquirer settlement, fees, and chargebacks | `ledger-midaz.csv`, `acquirer-settlement.json`, `acquirer-fees.csv`, `chargebacks.csv` |
| `context-02-bank-statement-vs-ledger/` | Midaz ledger export vs bank statement and adjustments | `midaz-ledger-export.json`, `bank-statement.csv`, `bank-adjustments.csv` |
| `context-03-marketplace-payouts/` | Marketplace orders, gateway transactions, seller payouts, and refunds | `orders.csv`, `gateway-transactions.json`, `seller-payouts.csv`, `refunds.csv` |
| `context-04-exceptions-lab/` | Exception-heavy ledger, bank, and gateway reconciliation lab | `ledger.csv`, `external-bank.csv`, `gateway.csv` |

## How To Use In Matcher

1. Pick one context folder.
2. Upload the files under that folder's `data/` directory as raw source files.
3. Use the context README to choose LEFT and RIGHT sources.
4. Configure field maps so source-specific columns map into canonical transaction fields like `external_id`, `amount`, `currency`, `date`, and optional `description`.
5. Keep extra columns such as `order_id`, `authorization_code`, `settlement_id`, `end_to_end_id`, `seller_id`, `payout_id`, `reference`, and `scenario` as metadata for match rules and manual analysis.
6. Configure match rules from exact keys first, then add date windows, amount tolerances, fee variance checks, duplicate detection, and manual review rules.

## Data Guarantees

All records are synthetic. No real customer, company, bank, or card data is present.

The datasets intentionally include exact matches, fuzzy amount tolerance candidates, date-window candidates, amount mismatches outside tolerance, duplicates, refunds, chargebacks, negative amounts, LEFT-only records, RIGHT-only records, fee variance candidates, and partial or aggregate payout cases.

Descriptions are safe synthetic text. The generator does not create formula-like display fields that begin with `=`, `+`, `-`, or `@`.

## Regenerate And Validate

Run from the repository root:

```bash
python3 examples/scripts/generate_examples.py
python3 examples/scripts/validate_examples.py
```

The validator is standalone. It parses every CSV and JSON file, checks the exact row/object counts, validates required columns and keys, validates date/currency/amount fields, and verifies that related files have overlapping relationship keys.
