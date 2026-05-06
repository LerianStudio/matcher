# Context 03: Marketplace Payouts

## Business Story

This context models a marketplace flow where customer orders are captured by a payment gateway and later paid out to sellers in batches. Refunds and payout adjustments create realistic reconciliation pressure around aggregation, partial payouts, fees, and unmatched payout records.

## Files

| File | Represents | Shape |
| --- | --- | --- |
| `data/orders.csv` | Marketplace order export by seller and buyer | 1000 CSV rows plus header |
| `data/gateway-transactions.json` | Gateway capture/refund/failure records with gross, fee, and net amounts | 1000 JSON objects in a top-level array |
| `data/seller-payouts.csv` | Seller payout batches with pipe-separated included order IDs | 1000 CSV rows plus header |
| `data/refunds.csv` | Refund records linked to gateway transactions where available | 1000 CSV rows plus header |

The `scenario` metadata explains aggregate payouts, partial payouts, refund-adjusted payouts, failed gateway captures, and unmatched records.

## Suggested LEFT/RIGHT Setup

Order-to-payout demo:

| Side | Source |
| --- | --- |
| LEFT | `orders.csv` |
| RIGHT | `seller-payouts.csv` |

Gateway-to-payout demo:

| Side | Source |
| --- | --- |
| LEFT | `gateway-transactions.json` |
| RIGHT | `seller-payouts.csv` |

Optional auxiliary source:

| Purpose | Source |
| --- | --- |
| Refund and payout adjustment review | `refunds.csv` |

## Suggested Field Maps

| File | `external_id` | `amount` | `currency` | `date` | `description` | Metadata worth keeping |
| --- | --- | --- | --- | --- | --- | --- |
| `orders.csv` | `order_id` | `gross_amount` | `currency` | `order_date` | `status` | `seller_id`, `buyer_id`, `payment_id`, `scenario` |
| `gateway-transactions.json` | `gateway_tx_id` | `net_amount` for payout matching or `gross_amount` for order matching | `currency` | `captured_at` | `status` | `order_id`, `seller_id`, `payment_id`, `fee_amount`, `scenario` |
| `seller-payouts.csv` | `payout_id` | `amount` | `currency` | `payout_date` | `payout_batch_id` | `seller_id`, `included_order_ids`, `scenario` |
| `refunds.csv` | `refund_id` | `amount` | `currency` | `refunded_at` | `reason` | `order_id`, `gateway_tx_id`, `seller_id`, `scenario` |

## Suggested Match Rules

| Rule | Intent |
| --- | --- |
| Exact `order_id` between orders and gateway transactions | Captured order reconciliation |
| Exact `payment_id` plus same `seller_id` | Secondary gateway/order confirmation |
| Aggregate sum of gateway `net_amount` by `included_order_ids` equals payout `amount` plus configured payout fee | Many-orders-to-one-payout matching |
| Same `seller_id`, payout date window `1..7` days after capture/order date | Seller payout timing rule |
| Partial payout rule when payout amount is below aggregate net amount | Manual review for staged payouts |
| Refund rule by `gateway_tx_id` or `order_id` | Explains reduced or negative payout variance |
| Unmatched payout rule | Finds payout-only records and operational exceptions |

## Expected Outcomes

Orders `1..920` have gateway transaction candidates. Payout rows include many-orders-to-one batches, partial payouts, refund-adjusted payouts, and payout-only rows. Refund rows include a deliberate population linked to gateway transactions plus many unmatched refund-only rows.

Approximate useful populations:

| Population | Expected behavior |
| --- | --- |
| `gateway_matches_order` | Auto-match candidate by `order_id` or `payment_id` |
| `aggregate_payout_many_orders` | Match by aggregation over `included_order_ids` |
| `partial_payout_candidate` | Manual review or staged-payout rule |
| `refund_adjusted_payout` | Match when refund adjustment is considered |
| `gateway_amount_mismatch`, `gateway_failed_no_payout` | Exception review |
| `payout_only_unmatched`, `refund_only_unmatched` | Unmatched auxiliary records |

## Concrete Keys To Try

| Key | Why it matters |
| --- | --- |
| `ORD-C03-000001` and `GTW-C03-000001` | Should match between orders and gateway transactions |
| `PO-C03-000001` | Includes `ORD-C03-000001|ORD-C03-000051|ORD-C03-000101` for aggregate payout testing |
| `ORD-C03-000751` | Appears in a partial payout candidate |
| `GTW-C03-000008` | Has a linked refund candidate through generated refund rows |
| `ORD-C03-008421` | Payout-only included order ID with no matching order row |
