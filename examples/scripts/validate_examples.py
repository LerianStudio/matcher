#!/usr/bin/env python3
"""Validate raw Matcher example datasets without using Matcher internals."""

from __future__ import annotations

import csv
import json
from datetime import datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
VALID_CURRENCIES = {"BRL", "USD", "EUR"}


EXPECTED = {
    "context-01-card-acquirer-settlement": {
        "ledger-midaz.csv": {
            "kind": "csv",
            "required": [
                "ledger_tx_id",
                "order_id",
                "amount",
                "currency",
                "occurred_at",
                "description",
                "merchant_id",
                "authorization_code",
            ],
            "amounts": ["amount"],
            "dates": ["occurred_at"],
            "currencies": ["currency"],
        },
        "acquirer-settlement.json": {
            "kind": "json",
            "required": [
                "acquirer_tx_id",
                "order_id",
                "gross_amount",
                "net_amount",
                "fee_amount",
                "currency",
                "settlement_date",
                "authorization_code",
                "settlement_id",
                "merchant_id",
            ],
            "amounts": ["gross_amount", "net_amount", "fee_amount"],
            "dates": ["settlement_date"],
            "currencies": ["currency"],
        },
        "acquirer-fees.csv": {
            "kind": "csv",
            "required": [
                "fee_id",
                "order_id",
                "fee_amount",
                "fee_type",
                "charged_at",
                "settlement_id",
            ],
            "amounts": ["fee_amount"],
            "dates": ["charged_at"],
            "currencies": [],
        },
        "chargebacks.csv": {
            "kind": "csv",
            "required": [
                "chargeback_id",
                "order_id",
                "amount",
                "currency",
                "occurred_at",
                "reason_code",
                "authorization_code",
            ],
            "amounts": ["amount"],
            "dates": ["occurred_at"],
            "currencies": ["currency"],
        },
    },
    "context-02-bank-statement-vs-ledger": {
        "midaz-ledger-export.json": {
            "kind": "json",
            "required": [
                "ledger_entry_id",
                "end_to_end_id",
                "amount",
                "currency",
                "posted_at",
                "description",
                "account_id",
                "reference",
            ],
            "amounts": ["amount"],
            "dates": ["posted_at"],
            "currencies": ["currency"],
        },
        "bank-statement.csv": {
            "kind": "csv",
            "required": [
                "bank_entry_id",
                "end_to_end_id",
                "amount",
                "currency",
                "booking_date",
                "description",
                "bank_account",
                "reference",
            ],
            "amounts": ["amount"],
            "dates": ["booking_date"],
            "currencies": ["currency"],
        },
        "bank-adjustments.csv": {
            "kind": "csv",
            "required": [
                "adjustment_id",
                "reference",
                "end_to_end_id",
                "amount",
                "currency",
                "adjustment_date",
                "reason",
            ],
            "amounts": ["amount"],
            "dates": ["adjustment_date"],
            "currencies": ["currency"],
        },
    },
    "context-03-marketplace-payouts": {
        "orders.csv": {
            "kind": "csv",
            "required": [
                "order_id",
                "seller_id",
                "buyer_id",
                "gross_amount",
                "currency",
                "order_date",
                "status",
                "payment_id",
            ],
            "amounts": ["gross_amount"],
            "dates": ["order_date"],
            "currencies": ["currency"],
        },
        "gateway-transactions.json": {
            "kind": "json",
            "required": [
                "gateway_tx_id",
                "order_id",
                "seller_id",
                "payment_id",
                "gross_amount",
                "fee_amount",
                "net_amount",
                "currency",
                "captured_at",
                "status",
            ],
            "amounts": ["gross_amount", "fee_amount", "net_amount"],
            "dates": ["captured_at"],
            "currencies": ["currency"],
        },
        "seller-payouts.csv": {
            "kind": "csv",
            "required": [
                "payout_id",
                "seller_id",
                "payout_batch_id",
                "amount",
                "currency",
                "payout_date",
                "included_order_ids",
            ],
            "amounts": ["amount"],
            "dates": ["payout_date"],
            "currencies": ["currency"],
        },
        "refunds.csv": {
            "kind": "csv",
            "required": [
                "refund_id",
                "order_id",
                "gateway_tx_id",
                "seller_id",
                "amount",
                "currency",
                "refunded_at",
                "reason",
            ],
            "amounts": ["amount"],
            "dates": ["refunded_at"],
            "currencies": ["currency"],
        },
    },
    "context-04-exceptions-lab": {
        "ledger.csv": {
            "kind": "csv",
            "required": [
                "ledger_tx_id",
                "reference",
                "amount",
                "currency",
                "occurred_at",
                "description",
                "customer_ref",
            ],
            "amounts": ["amount"],
            "dates": ["occurred_at"],
            "currencies": ["currency"],
        },
        "external-bank.csv": {
            "kind": "csv",
            "required": [
                "bank_tx_id",
                "reference",
                "amount",
                "currency",
                "booking_date",
                "description",
                "customer_ref",
            ],
            "amounts": ["amount"],
            "dates": ["booking_date"],
            "currencies": ["currency"],
        },
        "gateway.csv": {
            "kind": "csv",
            "required": [
                "gateway_tx_id",
                "reference",
                "amount",
                "currency",
                "captured_at",
                "description",
                "authorization_code",
            ],
            "amounts": ["amount"],
            "dates": ["captured_at"],
            "currencies": ["currency"],
        },
    },
}


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def read_json(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, list):
        raise AssertionError(f"{path}: expected a top-level JSON array")
    if not all(isinstance(item, dict) for item in payload):
        raise AssertionError(f"{path}: expected every JSON array item to be an object")
    return payload


def parse_amount(path: Path, row_number: int, field: str, value: str) -> None:
    try:
        Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise AssertionError(
            f"{path}:{row_number}: invalid amount in {field}: {value!r}"
        ) from exc


def parse_date(path: Path, row_number: int, field: str, value: str) -> None:
    normalized = str(value).replace("Z", "+00:00")
    try:
        datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise AssertionError(
            f"{path}:{row_number}: invalid date in {field}: {value!r}"
        ) from exc


def validate_file(
    context: str, filename: str, spec: dict[str, object]
) -> list[dict[str, str]]:
    path = ROOT / context / "data" / filename
    if not path.exists():
        raise AssertionError(f"missing file: {path}")

    rows = read_csv(path) if spec["kind"] == "csv" else read_json(path)
    if len(rows) != 1_000:
        raise AssertionError(f"{path}: expected 1000 rows/objects, got {len(rows)}")

    required = set(spec["required"])
    for row_number, row in enumerate(rows, start=2 if spec["kind"] == "csv" else 1):
        missing = sorted(required - set(row))
        if missing:
            raise AssertionError(
                f"{path}:{row_number}: missing required fields: {', '.join(missing)}"
            )
        for field in spec["amounts"]:
            parse_amount(path, row_number, field, row[field])
        for field in spec["dates"]:
            parse_date(path, row_number, field, row[field])
        for field in spec["currencies"]:
            if row[field] not in VALID_CURRENCIES:
                raise AssertionError(
                    f"{path}:{row_number}: invalid currency in {field}: {row[field]!r}"
                )

    return rows


def values(
    rows: list[dict[str, str]], field: str, *, allow_blank: bool = False
) -> set[str]:
    return {row[field] for row in rows if allow_blank or row[field]}


def assert_overlap(label: str, left: set[str], right: set[str], minimum: int) -> None:
    overlap = left & right
    if len(overlap) < minimum:
        raise AssertionError(
            f"{label}: expected at least {minimum} overlapping keys, got {len(overlap)}"
        )


def validate_relationships(loaded: dict[str, dict[str, list[dict[str, str]]]]) -> None:
    c1 = loaded["context-01-card-acquirer-settlement"]
    assert_overlap(
        "context 01 ledger/acquirer order_id",
        values(c1["ledger-midaz.csv"], "order_id"),
        values(c1["acquirer-settlement.json"], "order_id"),
        700,
    )
    assert_overlap(
        "context 01 ledger/acquirer authorization_code",
        values(c1["ledger-midaz.csv"], "authorization_code"),
        values(c1["acquirer-settlement.json"], "authorization_code"),
        700,
    )
    assert_overlap(
        "context 01 acquirer/fees settlement_id",
        values(c1["acquirer-settlement.json"], "settlement_id"),
        values(c1["acquirer-fees.csv"], "settlement_id"),
        8,
    )
    assert_overlap(
        "context 01 ledger/chargebacks order_id",
        values(c1["ledger-midaz.csv"], "order_id"),
        values(c1["chargebacks.csv"], "order_id"),
        250,
    )

    c2 = loaded["context-02-bank-statement-vs-ledger"]
    assert_overlap(
        "context 02 ledger/bank end_to_end_id",
        values(c2["midaz-ledger-export.json"], "end_to_end_id"),
        values(c2["bank-statement.csv"], "end_to_end_id"),
        700,
    )
    assert_overlap(
        "context 02 ledger/bank reference",
        values(c2["midaz-ledger-export.json"], "reference"),
        values(c2["bank-statement.csv"], "reference"),
        700,
    )
    assert_overlap(
        "context 02 ledger/adjustments reference",
        values(c2["midaz-ledger-export.json"], "reference"),
        values(c2["bank-adjustments.csv"], "reference"),
        450,
    )

    c3 = loaded["context-03-marketplace-payouts"]
    payout_order_ids = {
        order_id
        for row in c3["seller-payouts.csv"]
        for order_id in row["included_order_ids"].split("|")
        if order_id
    }
    assert_overlap(
        "context 03 orders/gateway order_id",
        values(c3["orders.csv"], "order_id"),
        values(c3["gateway-transactions.json"], "order_id"),
        900,
    )
    assert_overlap(
        "context 03 orders/payout included_order_ids",
        values(c3["orders.csv"], "order_id"),
        payout_order_ids,
        900,
    )
    assert_overlap(
        "context 03 gateway/refunds gateway_tx_id",
        values(c3["gateway-transactions.json"], "gateway_tx_id"),
        values(c3["refunds.csv"], "gateway_tx_id"),
        200,
    )

    c4 = loaded["context-04-exceptions-lab"]
    assert_overlap(
        "context 04 ledger/bank reference",
        values(c4["ledger.csv"], "reference"),
        values(c4["external-bank.csv"], "reference"),
        650,
    )
    assert_overlap(
        "context 04 ledger/gateway reference",
        values(c4["ledger.csv"], "reference"),
        values(c4["gateway.csv"], "reference"),
        600,
    )
    assert_overlap(
        "context 04 ledger/bank customer_ref",
        values(c4["ledger.csv"], "customer_ref"),
        values(c4["external-bank.csv"], "customer_ref"),
        400,
    )


def main() -> None:
    loaded: dict[str, dict[str, list[dict[str, str]]]] = {}
    for context, files in EXPECTED.items():
        loaded[context] = {}
        for filename, spec in files.items():
            loaded[context][filename] = validate_file(context, filename, spec)
    validate_relationships(loaded)
    print("Validated 14 example data files: 14,000 rows/objects total.")


if __name__ == "__main__":
    main()
