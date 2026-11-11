-- Used by 20_capstone_sales_etl.
-- %(batch_id)s is a query PARAMETER (filled safely by the database driver), not Jinja.
-- The operator passes parameters={"batch_id": "{{ run_id }}"}.
-- Deleting first makes the task IDEMPOTENT: re-running it gives the same result.
DELETE FROM sales_daily_summary WHERE batch_id = %(batch_id)s;

INSERT INTO sales_daily_summary (batch_id, region, orders, units, revenue)
SELECT batch_id, region, COUNT(*), SUM(quantity), SUM(revenue)
FROM sales_clean
WHERE batch_id = %(batch_id)s
GROUP BY batch_id, region;
