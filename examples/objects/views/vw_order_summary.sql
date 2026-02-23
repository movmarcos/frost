-- Declarative: Derived view — order summary per sample
-- frost automatically detects dependencies on both orders and vw_active_samples
-- Database is set at connection level — use SCHEMA.OBJECT naming

CREATE OR ALTER VIEW PUBLIC.VW_ORDER_SUMMARY AS
SELECT
    s.id            AS sample_id,
    s.name          AS sample_name,
    COUNT(o.order_id) AS order_count,
    SUM(o.total_amount) AS total_amount,
    MAX(o.order_date)   AS last_order_date
FROM PUBLIC.VW_ACTIVE_SAMPLES s
LEFT JOIN PUBLIC.ORDERS o ON o.sample_id = s.id
GROUP BY s.id, s.name;
