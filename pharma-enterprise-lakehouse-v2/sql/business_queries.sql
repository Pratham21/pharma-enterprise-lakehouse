-- Example queries: change `main` to your Unity Catalog catalog.

-- Daily volume by therapeutic area
SELECT rx_date, therapeutic_area,
       SUM(rx_count) AS prescription_count,
       SUM(units) AS total_units
FROM main.pharma_gold.vw_rx_metrics
GROUP BY rx_date, therapeutic_area
ORDER BY rx_date, therapeutic_area;

-- Top products by total units
SELECT product_name, SUM(units) AS total_units
FROM main.pharma_gold.vw_rx_metrics
GROUP BY product_name
ORDER BY total_units DESC
LIMIT 10;

-- Data quality: invalid source records
SELECT _reject_reason, COUNT(*) AS rejected_rows
FROM main.pharma_silver.rx_quarantine
GROUP BY _reject_reason
ORDER BY rejected_rows DESC;

-- CDC tombstones retained in Silver but not published to Gold
SELECT COUNT(*) AS logical_deletes
FROM main.pharma_silver.rx_current
WHERE is_deleted = true;
