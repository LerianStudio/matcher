#!/usr/bin/env python3
"""Generate deterministic raw Matcher example datasets.

The generated files are intentionally source-like CSV/JSON exports. They are
not Matcher configuration seeds; users configure sources, field maps, and rules
inside Matcher when uploading these files.
"""

from __future__ import annotations

import csv
import json
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SEED = 20260503
CURRENCIES = ("BRL", "USD", "EUR")


def money(cents: int) -> str:
    sign = "-" if cents < 0 else ""
    absolute = abs(cents)
    return f"{sign}{absolute // 100}.{absolute % 100:02d}"


def cents_for(namespace: int, index: int, minimum: int, maximum: int) -> int:
    rng = random.Random(SEED + namespace * 1_000_003 + index * 9_176)
    return rng.randint(minimum, maximum)


def fee_for(gross_cents: int, index: int) -> int:
    return max(30, round(gross_cents * 0.029) + 49 + (index % 5) * 7)


def currency_for(index: int) -> str:
    if index % 12 == 0:
        return CURRENCIES[(index // 12) % len(CURRENCIES)]
    return "BRL"


def timestamp(base: datetime, days: int, minutes: int = 0) -> str:
    return (
        (base + timedelta(days=days, minutes=minutes))
        .isoformat()
        .replace("+00:00", "Z")
    )


def date_value(base: datetime, days: int) -> str:
    return (base + timedelta(days=days)).date().isoformat()


def write_csv(
    relative_path: str, fieldnames: list[str], rows: list[dict[str, str]]
) -> None:
    path = ROOT / relative_path
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def write_json(relative_path: str, rows: list[dict[str, str]]) -> None:
    path = ROOT / relative_path
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        json.dump(rows, handle, indent=2)
        handle.write("\n")


def context01() -> None:
    base = datetime(2026, 1, 5, 10, 0, tzinfo=timezone.utc)

    def order_id(index: int) -> str:
        return f"ORD-C01-{index:06d}"

    def auth_code(index: int) -> str:
        return f"AUTH-C01-{index % 1_000_000:06d}"

    def merchant_id(index: int) -> str:
        return f"MRC-C01-{(index % 24) + 1:03d}"

    def sale_cents(index: int) -> int:
        return cents_for(1, index, 1_500, 185_000)

    def chargeback_cents(index: int) -> int:
        return min(sale_cents(index), sale_cents(index) // 2 + (index % 7) * 125)

    def settlement_id(index: int) -> str:
        return f"SET-C01-{((index - 1) // 100) + 1:04d}"

    def settlement_order_index(row_index: int) -> int:
        if 801 <= row_index <= 830:
            return 650 + ((row_index - 801) % 15)
        if row_index <= 850:
            return row_index
        return 3_000 + row_index

    ledger_rows: list[dict[str, str]] = []
    for row_index in range(1, 1_001):
        if row_index <= 820:
            source_index = row_index
            amount_cents = sale_cents(source_index)
            ledger_tx_id = f"LED-C01-{row_index:06d}"
            description = f"Card sale for synthetic order {order_id(source_index)}"
            scenario = "ledger_sale_candidate"
        elif row_index <= 880:
            source_index = 120 + (row_index - 821)
            amount_cents = -chargeback_cents(source_index)
            ledger_tx_id = f"LED-C01-CB-{row_index:06d}"
            description = (
                f"Chargeback posted for synthetic order {order_id(source_index)}"
            )
            scenario = "ledger_chargeback"
        else:
            source_index = 2_000 + row_index
            amount_cents = sale_cents(source_index)
            ledger_tx_id = f"LED-C01-ONLY-{row_index:06d}"
            description = f"Ledger-only synthetic order {order_id(source_index)}"
            scenario = "ledger_only"

        ledger_rows.append(
            {
                "ledger_tx_id": ledger_tx_id,
                "order_id": order_id(source_index),
                "amount": money(amount_cents),
                "currency": currency_for(source_index),
                "occurred_at": timestamp(
                    base, source_index % 35, (source_index % 12) * 11
                ),
                "description": description,
                "merchant_id": merchant_id(source_index),
                "authorization_code": auth_code(source_index),
                "scenario": scenario,
            }
        )

    settlement_rows: list[dict[str, str]] = []
    for row_index in range(1, 1_001):
        source_index = settlement_order_index(row_index)
        gross_cents = sale_cents(source_index)
        scenario = "exact_order_and_authorization_match"
        lag_days = 1 + (source_index % 3)

        if row_index > 850:
            scenario = "acquirer_only_settlement"
        elif 801 <= row_index <= 830:
            scenario = "duplicate_settlement_candidate"
        elif row_index % 53 == 0:
            gross_cents += 725
            scenario = "amount_mismatch_outside_tolerance"
        elif row_index % 41 == 0:
            gross_cents += 3
            scenario = "amount_tolerance_candidate"
        elif row_index % 17 == 0:
            lag_days += 2
            scenario = "settlement_lag_candidate"

        fee_cents = fee_for(gross_cents, source_index)
        if row_index <= 850 and row_index % 47 == 0:
            fee_cents += 125
            scenario = "fee_variance_candidate"

        settlement_rows.append(
            {
                "acquirer_tx_id": f"ACQ-C01-{row_index:06d}",
                "order_id": order_id(source_index),
                "gross_amount": money(gross_cents),
                "net_amount": money(gross_cents - fee_cents),
                "fee_amount": money(fee_cents),
                "currency": currency_for(source_index),
                "settlement_date": date_value(base, (source_index % 35) + lag_days),
                "authorization_code": auth_code(source_index),
                "settlement_id": settlement_id(row_index),
                "merchant_id": merchant_id(source_index),
                "scenario": scenario,
            }
        )

    fee_rows: list[dict[str, str]] = []
    for row_index in range(1, 1_001):
        if row_index <= 850:
            source_index = settlement_order_index(row_index)
            fee_cents = fee_for(sale_cents(source_index), source_index)
            settlement_key = settlement_id(row_index)
            scenario = "fee_matches_settlement"
            if row_index % 47 == 0:
                fee_cents += 125
                scenario = "fee_variance_candidate"
        else:
            source_index = 6_000 + row_index
            fee_cents = fee_for(sale_cents(source_index), source_index)
            settlement_key = settlement_id(4_000 + row_index)
            scenario = "fee_only_unmatched"

        fee_rows.append(
            {
                "fee_id": f"FEE-C01-{row_index:06d}",
                "order_id": order_id(source_index),
                "fee_amount": money(fee_cents),
                "fee_type": "interchange" if row_index % 5 else "scheme",
                "charged_at": timestamp(
                    base, (source_index % 35) + 2, (row_index % 24) * 5
                ),
                "settlement_id": settlement_key,
                "scenario": scenario,
            }
        )

    chargeback_rows: list[dict[str, str]] = []
    for row_index in range(1, 1_001):
        if row_index <= 360:
            source_index = 100 + ((row_index - 1) % 520)
            scenario = "chargeback_links_to_original_order"
        else:
            source_index = 7_000 + row_index
            scenario = "chargeback_only_unmatched"
        if row_index % 111 == 0:
            source_index = 120 + (row_index % 50)
            scenario = "duplicate_chargeback_candidate"

        chargeback_rows.append(
            {
                "chargeback_id": f"CBK-C01-{row_index:06d}",
                "order_id": order_id(source_index),
                "amount": money(-chargeback_cents(source_index)),
                "currency": currency_for(source_index),
                "occurred_at": timestamp(
                    base, (source_index % 35) + 9, (row_index % 24) * 7
                ),
                "reason_code": f"RC-{(row_index % 8) + 1:02d}",
                "authorization_code": auth_code(source_index),
                "scenario": scenario,
            }
        )

    prefix = "context-01-card-acquirer-settlement/data"
    write_csv(
        f"{prefix}/ledger-midaz.csv",
        [
            "ledger_tx_id",
            "order_id",
            "amount",
            "currency",
            "occurred_at",
            "description",
            "merchant_id",
            "authorization_code",
            "scenario",
        ],
        ledger_rows,
    )
    write_json(f"{prefix}/acquirer-settlement.json", settlement_rows)
    write_csv(
        f"{prefix}/acquirer-fees.csv",
        [
            "fee_id",
            "order_id",
            "fee_amount",
            "fee_type",
            "charged_at",
            "settlement_id",
            "scenario",
        ],
        fee_rows,
    )
    write_csv(
        f"{prefix}/chargebacks.csv",
        [
            "chargeback_id",
            "order_id",
            "amount",
            "currency",
            "occurred_at",
            "reason_code",
            "authorization_code",
            "scenario",
        ],
        chargeback_rows,
    )


def context02() -> None:
    base = datetime(2026, 2, 2, 9, 30, tzinfo=timezone.utc)

    def end_to_end_id(index: int) -> str:
        return f"E2E-C02-{index:09d}"

    def reference(index: int) -> str:
        return f"REF-C02-{index:06d}"

    def account_id(index: int) -> str:
        return f"ACC-C02-{(index % 9) + 1:03d}"

    def amount_cents(index: int) -> int:
        value = cents_for(2, index, 900, 95_000)
        if index % 37 == 0:
            return -max(250, value // 15)
        return value

    ledger_rows: list[dict[str, str]] = []
    for row_index in range(1, 1_001):
        e2e = "" if 861 <= row_index <= 880 else end_to_end_id(row_index)
        scenario = "ledger_bank_candidate"
        if row_index > 880:
            scenario = "ledger_only"
        elif e2e == "":
            scenario = "missing_e2e_uses_reference_fallback"

        ledger_rows.append(
            {
                "ledger_entry_id": f"LED-C02-{row_index:06d}",
                "end_to_end_id": e2e,
                "amount": money(amount_cents(row_index)),
                "currency": currency_for(row_index),
                "posted_at": timestamp(base, row_index % 42, (row_index % 18) * 9),
                "description": f"Synthetic ledger posting {reference(row_index)}",
                "account_id": account_id(row_index),
                "reference": reference(row_index),
                "scenario": scenario,
            }
        )

    bank_rows: list[dict[str, str]] = []
    for row_index in range(1, 1_001):
        if row_index <= 760:
            source_index = row_index
            scenario = "bank_statement_match_candidate"
        elif row_index <= 790:
            source_index = 120 + (row_index - 761)
            scenario = "duplicate_bank_line"
        elif row_index <= 840:
            source_index = 8_000 + row_index
            scenario = "bank_fee_only"
        else:
            source_index = 9_000 + row_index
            scenario = "bank_only_unmatched"

        amount = amount_cents(source_index)
        e2e = end_to_end_id(source_index)
        if row_index <= 760 and row_index % 83 == 0:
            e2e = ""
            scenario = "missing_e2e_uses_reference_fallback"
        if row_index <= 760 and row_index % 67 == 0:
            amount += 1_500
            scenario = "amount_mismatch_outside_tolerance"
        elif row_index <= 760 and row_index % 41 == 0:
            amount += 4
            scenario = "amount_tolerance_candidate"
        if 791 <= row_index <= 840:
            amount = -(350 + (row_index % 11) * 25)

        booking_lag = 1 if row_index <= 760 and row_index % 11 == 0 else 0
        bank_rows.append(
            {
                "bank_entry_id": f"BNK-C02-{row_index:06d}",
                "end_to_end_id": e2e,
                "amount": money(amount),
                "currency": currency_for(source_index),
                "booking_date": date_value(base, (source_index % 42) + booking_lag),
                "description": f"Synthetic bank statement line {reference(source_index)}",
                "bank_account": account_id(source_index).replace("ACC", "BANK"),
                "reference": reference(source_index)
                if row_index <= 790
                else f"BANKONLY-C02-{row_index:06d}",
                "scenario": scenario,
            }
        )

    adjustment_rows: list[dict[str, str]] = []
    for row_index in range(1, 1_001):
        if row_index <= 500:
            source_index = row_index
            ref = reference(source_index)
            e2e = end_to_end_id(source_index)
            amount = 100 + (row_index % 17) * 12
            reason = "manual_reconciliation_adjustment"
            scenario = "adjustment_links_to_ledger_and_bank"
        elif row_index <= 650:
            source_index = 8_000 + row_index
            ref = f"BANKONLY-C02-{row_index - 500 + 790:06d}"
            e2e = end_to_end_id(source_index)
            amount = -(250 + (row_index % 13) * 20)
            reason = "bank_fee_adjustment"
            scenario = "bank_fee_adjustment"
        else:
            source_index = 10_000 + row_index
            ref = f"ADJONLY-C02-{row_index:06d}"
            e2e = end_to_end_id(source_index)
            amount = 125 + (row_index % 19) * 31
            reason = "adjustment_only_unmatched"
            scenario = "adjustment_only_unmatched"

        adjustment_rows.append(
            {
                "adjustment_id": f"ADJ-C02-{row_index:06d}",
                "reference": ref,
                "end_to_end_id": e2e,
                "amount": money(amount),
                "currency": currency_for(source_index),
                "adjustment_date": date_value(base, (source_index % 42) + 2),
                "reason": reason,
                "scenario": scenario,
            }
        )

    prefix = "context-02-bank-statement-vs-ledger/data"
    write_json(f"{prefix}/midaz-ledger-export.json", ledger_rows)
    write_csv(
        f"{prefix}/bank-statement.csv",
        [
            "bank_entry_id",
            "end_to_end_id",
            "amount",
            "currency",
            "booking_date",
            "description",
            "bank_account",
            "reference",
            "scenario",
        ],
        bank_rows,
    )
    write_csv(
        f"{prefix}/bank-adjustments.csv",
        [
            "adjustment_id",
            "reference",
            "end_to_end_id",
            "amount",
            "currency",
            "adjustment_date",
            "reason",
            "scenario",
        ],
        adjustment_rows,
    )


def context03() -> None:
    base = datetime(2026, 3, 4, 12, 0, tzinfo=timezone.utc)

    def order_id(index: int) -> str:
        return f"ORD-C03-{index:06d}"

    def seller_id(index: int) -> str:
        return f"SEL-C03-{((index - 1) % 50) + 1:03d}"

    def buyer_id(index: int) -> str:
        return f"BUY-C03-{((index * 7) % 900) + 1:04d}"

    def payment_id(index: int) -> str:
        return f"PAY-C03-{index:06d}"

    def gross_cents(index: int) -> int:
        return cents_for(3, index, 2_000, 220_000)

    def net_cents(index: int) -> int:
        gross = gross_cents(index)
        return gross - fee_for(gross, index)

    order_rows: list[dict[str, str]] = []
    for row_index in range(1, 1_001):
        if row_index % 31 == 0:
            status = "pending"
            scenario = "order_not_captured_yet"
        elif row_index % 17 == 0:
            status = "refunded"
            scenario = "order_has_refund"
        elif row_index % 29 == 0:
            status = "disputed"
            scenario = "order_needs_review"
        else:
            status = "paid"
            scenario = "order_gateway_payout_candidate"

        order_rows.append(
            {
                "order_id": order_id(row_index),
                "seller_id": seller_id(row_index),
                "buyer_id": buyer_id(row_index),
                "gross_amount": money(gross_cents(row_index)),
                "currency": currency_for(row_index),
                "order_date": timestamp(base, row_index % 31, (row_index % 20) * 13),
                "status": status,
                "payment_id": payment_id(row_index),
                "scenario": scenario,
            }
        )

    gateway_rows: list[dict[str, str]] = []
    for row_index in range(1, 1_001):
        source_index = row_index if row_index <= 920 else 5_000 + row_index
        gross = gross_cents(source_index)
        scenario = "gateway_matches_order"
        status = "captured"

        if row_index > 920:
            scenario = "gateway_only_unmatched"
        elif row_index % 73 == 0:
            gross += 950
            scenario = "gateway_amount_mismatch"
        elif row_index % 41 == 0:
            gross += 5
            scenario = "gateway_amount_tolerance_candidate"
        elif row_index % 17 == 0:
            status = "refunded"
            scenario = "gateway_refunded"
        elif row_index % 31 == 0:
            status = "failed"
            scenario = "gateway_failed_no_payout"

        fee = fee_for(gross, source_index)
        gateway_rows.append(
            {
                "gateway_tx_id": f"GTW-C03-{row_index:06d}",
                "order_id": order_id(source_index),
                "seller_id": seller_id(source_index),
                "payment_id": payment_id(source_index),
                "gross_amount": money(gross),
                "fee_amount": money(fee),
                "net_amount": money(gross - fee),
                "currency": currency_for(source_index),
                "captured_at": timestamp(
                    base, (source_index % 31) + 1, (row_index % 20) * 17
                ),
                "status": status,
                "scenario": scenario,
            }
        )

    payout_rows: list[dict[str, str]] = []
    for row_index in range(1, 1_001):
        if row_index <= 250:
            seller_number = ((row_index - 1) % 50) + 1
            group_number = (row_index - 1) // 50
            order_indexes = [
                seller_number + 50 * (group_number * 3 + offset) for offset in range(3)
            ]
            included = [index for index in order_indexes if index <= 1_000]
            amount = sum(net_cents(index) for index in included) - 99
            scenario = "aggregate_payout_many_orders"
            seller_key = seller_id(seller_number)
        elif row_index <= 350:
            order_index = 750 + (row_index - 250)
            included = [order_index]
            amount = round(net_cents(order_index) * 0.65)
            scenario = "partial_payout_candidate"
            seller_key = seller_id(order_index)
        elif row_index <= 420:
            order_index = 850 + (row_index - 350)
            included = [order_index]
            amount = max(
                0, net_cents(order_index) - cents_for(33, order_index, 300, 3_000)
            )
            scenario = "refund_adjusted_payout"
            seller_key = seller_id(order_index)
        else:
            included = [8_000 + row_index]
            amount = cents_for(34, row_index, 1_000, 80_000)
            scenario = "payout_only_unmatched"
            seller_key = seller_id(8_000 + row_index)

        payout_rows.append(
            {
                "payout_id": f"PO-C03-{row_index:06d}",
                "seller_id": seller_key,
                "payout_batch_id": f"PB-C03-{((row_index - 1) // 25) + 1:04d}",
                "amount": money(amount),
                "currency": currency_for(included[0]),
                "payout_date": date_value(base, (included[0] % 31) + 4),
                "included_order_ids": "|".join(order_id(index) for index in included),
                "scenario": scenario,
            }
        )

    refund_rows: list[dict[str, str]] = []
    for row_index in range(1, 1_001):
        if row_index <= 240:
            source_index = 1 + ((row_index * 7) % 920)
            gateway_key = f"GTW-C03-{source_index:06d}"
            scenario = "refund_links_to_gateway_and_order"
        else:
            source_index = 9_000 + row_index
            gateway_key = f"GTW-C03-ONLY-{row_index:06d}"
            scenario = "refund_only_unmatched"

        refund_rows.append(
            {
                "refund_id": f"REFUND-C03-{row_index:06d}",
                "order_id": order_id(source_index),
                "gateway_tx_id": gateway_key,
                "seller_id": seller_id(source_index),
                "amount": money(
                    min(
                        gross_cents(source_index),
                        gross_cents(source_index) // 2 + (row_index % 9) * 210,
                    )
                ),
                "currency": currency_for(source_index),
                "refunded_at": timestamp(
                    base, (source_index % 31) + 6, (row_index % 19) * 13
                ),
                "reason": "customer_request" if row_index % 4 else "fraud_review",
                "scenario": scenario,
            }
        )

    prefix = "context-03-marketplace-payouts/data"
    write_csv(
        f"{prefix}/orders.csv",
        [
            "order_id",
            "seller_id",
            "buyer_id",
            "gross_amount",
            "currency",
            "order_date",
            "status",
            "payment_id",
            "scenario",
        ],
        order_rows,
    )
    write_json(f"{prefix}/gateway-transactions.json", gateway_rows)
    write_csv(
        f"{prefix}/seller-payouts.csv",
        [
            "payout_id",
            "seller_id",
            "payout_batch_id",
            "amount",
            "currency",
            "payout_date",
            "included_order_ids",
            "scenario",
        ],
        payout_rows,
    )
    write_csv(
        f"{prefix}/refunds.csv",
        [
            "refund_id",
            "order_id",
            "gateway_tx_id",
            "seller_id",
            "amount",
            "currency",
            "refunded_at",
            "reason",
            "scenario",
        ],
        refund_rows,
    )


def context04() -> None:
    base = datetime(2026, 4, 6, 8, 0, tzinfo=timezone.utc)

    def reference(index: int) -> str:
        return f"REF-C04-{index:06d}"

    def customer_ref(index: int) -> str:
        return f"CUST-C04-{((index * 11) % 700) + 1:04d}"

    def amount_cents(index: int) -> int:
        value = cents_for(4, index, 700, 130_000)
        if index % 43 == 0:
            return -max(200, value // 12)
        return value

    ledger_rows: list[dict[str, str]] = []
    for row_index in range(1, 1_001):
        ref = reference(row_index)
        scenario = "exception_lab_match_candidate"
        if row_index % 101 == 0:
            ref = ""
            scenario = "missing_reference_manual_review"
        elif row_index % 89 == 0:
            ref = reference(row_index - 1)
            scenario = "duplicate_reference_candidate"
        elif row_index > 900:
            scenario = "ledger_only_unmatched"

        ledger_rows.append(
            {
                "ledger_tx_id": f"LED-C04-{row_index:06d}",
                "reference": ref,
                "amount": money(amount_cents(row_index)),
                "currency": currency_for(row_index),
                "occurred_at": timestamp(base, row_index % 28, (row_index % 16) * 10),
                "description": f"Synthetic ledger transfer {reference(row_index)}",
                "customer_ref": customer_ref(row_index),
                "scenario": scenario,
            }
        )

    bank_rows: list[dict[str, str]] = []
    for row_index in range(1, 1_001):
        if row_index <= 720:
            source_index = row_index
            scenario = "bank_match_candidate"
        elif row_index <= 760:
            source_index = 200 + ((row_index - 721) % 20)
            scenario = "duplicate_bank_reference"
        else:
            source_index = 6_000 + row_index
            scenario = "bank_only_unmatched"

        amount = amount_cents(source_index)
        if row_index <= 720 and row_index % 59 == 0:
            amount += 1_250
            scenario = "amount_mismatch_manual_review"
        elif row_index <= 720 and row_index % 47 == 0:
            amount += 3
            scenario = "small_tolerance_candidate"
        booking_lag = 2 if row_index <= 720 and row_index % 61 == 0 else 0
        ref = reference(source_index)
        if row_index <= 720 and row_index % 97 == 0:
            ref = ""
            scenario = "missing_bank_reference"

        description = f"Synthetic bank transfer {reference(source_index)}"
        if row_index <= 720 and row_index % 71 == 0:
            description = f"Synthetic bank trasnfer {reference(source_index)}"
            scenario = "near_match_description_typo"

        bank_rows.append(
            {
                "bank_tx_id": f"BANK-C04-{row_index:06d}",
                "reference": ref,
                "amount": money(amount),
                "currency": currency_for(source_index),
                "booking_date": date_value(base, (source_index % 28) + booking_lag),
                "description": description,
                "customer_ref": customer_ref(source_index),
                "scenario": scenario,
            }
        )

    gateway_rows: list[dict[str, str]] = []
    for row_index in range(1, 1_001):
        if row_index <= 650:
            source_index = row_index
            scenario = "gateway_match_candidate"
        elif row_index <= 700:
            source_index = 300 + ((row_index - 651) % 25)
            scenario = "gateway_duplicate_reference"
        else:
            source_index = 7_000 + row_index
            scenario = "gateway_only_unmatched"

        amount = amount_cents(source_index)
        if row_index <= 650 and row_index % 67 == 0:
            amount -= 875
            scenario = "gateway_amount_mismatch"
        elif row_index <= 650 and row_index % 43 == 0:
            scenario = "negative_amount_review"

        gateway_rows.append(
            {
                "gateway_tx_id": f"GTW-C04-{row_index:06d}",
                "reference": reference(source_index),
                "amount": money(amount),
                "currency": currency_for(source_index),
                "captured_at": timestamp(
                    base, (source_index % 28) + 1, (row_index % 16) * 12
                ),
                "description": f"Synthetic gateway capture {reference(source_index)}",
                "authorization_code": f"AUTH-C04-{source_index % 1_000_000:06d}",
                "scenario": scenario,
            }
        )

    prefix = "context-04-exceptions-lab/data"
    write_csv(
        f"{prefix}/ledger.csv",
        [
            "ledger_tx_id",
            "reference",
            "amount",
            "currency",
            "occurred_at",
            "description",
            "customer_ref",
            "scenario",
        ],
        ledger_rows,
    )
    write_csv(
        f"{prefix}/external-bank.csv",
        [
            "bank_tx_id",
            "reference",
            "amount",
            "currency",
            "booking_date",
            "description",
            "customer_ref",
            "scenario",
        ],
        bank_rows,
    )
    write_csv(
        f"{prefix}/gateway.csv",
        [
            "gateway_tx_id",
            "reference",
            "amount",
            "currency",
            "captured_at",
            "description",
            "authorization_code",
            "scenario",
        ],
        gateway_rows,
    )


def main() -> None:
    context01()
    context02()
    context03()
    context04()


if __name__ == "__main__":
    main()
