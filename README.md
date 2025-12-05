-- ============================================================
-- SQL Test Solutions (solutions.sql)
-- Assumptions:
--  1) A refunded purchase has refund_time IS NOT NULL.
--  2) Timestamps are timezone-aware or comparable; functions used are Postgres-compatible.
--  3) "Within 72 hours" = <= 72 hours after purchase_time.
--  4) Use only the snapshot data; no external enrichment.
-- ------------------------------------------------------------
-- Tables:
--   transactions(buyer_id, purchase_time, refund_time, item_id, store_id, gross_transaction_value)
--   items(store_id, item_id, item_category, item_name)
-- ============================================================


-- =========================
-- 1) Count of purchases per month (excluding refunded purchases)
-- =========================
-- Explanation: Remove rows where refund_time IS NOT NULL, then group by month.
SELECT
  DATE_TRUNC('month', purchase_time) AS month_start,
  COUNT(*) AS purchase_count
FROM transactions
WHERE refund_time IS NULL
GROUP BY 1
ORDER BY 1;


-- =========================
-- 2) Number of stores with at least 5 orders in October 2020
-- =========================
-- Explanation: Filter to October 2020, group by store_id, count stores having >= 5 orders.
SELECT COUNT(*) AS stores_with_at_least_5_orders_in_oct_2020
FROM (
  SELECT store_id, COUNT(*) AS orders_in_oct
  FROM transactions
  WHERE purchase_time >= '2020-10-01'::timestamp
    AND purchase_time <  '2020-11-01'::timestamp
  GROUP BY store_id
  HAVING COUNT(*) >= 5
) q;


-- =========================
-- 3) For each store: shortest interval (in minutes) from purchase to refund
-- =========================
-- Explanation: Consider only rows with refund_time; compute difference in minutes and take MIN per store.
SELECT
  store_id,
  MIN(EXTRACT(EPOCH FROM (refund_time - purchase_time)) / 60.0) AS min_refund_interval_minutes
FROM transactions
WHERE refund_time IS NOT NULL
GROUP BY store_id
ORDER BY store_id;


-- =========================
-- 4) gross_transaction_value of every store’s first order
-- =========================
-- Explanation: Use ROW_NUMBER() partitioned by store, ordered by purchase_time,
--              select rows where rn = 1 (first order per store).
WITH first_order_per_store AS (
  SELECT
    store_id,
    purchase_time,
    gross_transaction_value,
    ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY purchase_time ASC) AS rn
  FROM transactions
)
SELECT
  store_id,
  purchase_time AS first_order_time,
  gross_transaction_value AS first_order_gross_value
FROM first_order_per_store
WHERE rn = 1
ORDER BY store_id;


-- =========================
-- 5) Most popular item_name that buyers order on their first purchase
-- =========================
-- Explanation:
--  1) For each buyer, find their first transaction (ROW_NUMBER = 1).
--  2) Join to items to get item_name.
--  3) Count occurrences and return top item_name(s).
WITH first_purchase_per_buyer AS (
  SELECT
    buyer_id,
    item_id,
    ROW_NUMBER() OVER (PARTITION BY buyer_id ORDER BY purchase_time ASC) AS rn
  FROM transactions
)
SELECT
  it.item_name,
  COUNT(*) AS first_purchase_count
FROM first_purchase_per_buyer fp
JOIN items it
  ON fp.item_id = it.item_id
WHERE fp.rn = 1
GROUP BY it.item_name
ORDER BY first_purchase_count DESC
LIMIT 1;


-- =========================
-- 6) Create a flag indicating whether the refund can be processed (within 72 hours)
-- =========================
-- Explanation: 72 hours = 72 * 60 minutes = 4320 minutes.
--              Refund allowed if refund_time IS NOT NULL AND difference <= 72 hours.
SELECT
  *,
  CASE
    WHEN refund_time IS NOT NULL
      AND EXTRACT(EPOCH FROM (refund_time - purchase_time)) / 3600.0 <= 72.0
      THEN 'Refund_Allowed'
    WHEN refund_time IS NOT NULL THEN 'Refund_Not_Allowed'
    ELSE 'No_Refund'   -- no refund requested
  END AS refund_flag
FROM transactions
ORDER BY purchase_time;


-- If you prefer a boolean flag instead:
-- SELECT
--   *,
--   (refund_time IS NOT NULL AND EXTRACT(EPOCH FROM (refund_time - purchase_time)) <= 72*3600) AS is_refund_allowed
-- FROM transactions;


-- =========================
-- 7) Create a rank by buyer_id and filter only the second purchase per buyer (ignore refunds)
-- =========================
-- Explanation: Exclude refunded transactions, rank by purchase_time per buyer, select rn = 2.
WITH ranked_purchases AS (
  SELECT
    buyer_id,
    purchase_time,
    item_id,
    store_id,
    gross_transaction_value,
    ROW_NUMBER() OVER (PARTITION BY buyer_id ORDER BY purchase_time ASC) AS rn
  FROM transactions
  WHERE refund_time IS NULL
)
SELECT *
FROM ranked_purchases
WHERE rn = 2
ORDER BY buyer_id;


-- =========================
-- 8) Find the second transaction time per buyer (don't use MIN/MAX)
-- =========================
-- Explanation: Use ROW_NUMBER() over partition by buyer_id to pick rn = 2.
WITH ordered_tx AS (
  SELECT
    buyer_id,
    purchase_time,
    ROW_NUMBER() OVER (PARTITION BY buyer_id ORDER BY purchase_time ASC) AS rn
  FROM transactions
)
SELECT
  buyer_id,
  purchase_time AS second_transaction_time
FROM ordered_tx
WHERE rn = 2
ORDER BY buyer_id;
