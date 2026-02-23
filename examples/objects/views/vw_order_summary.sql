-- Declarative: Derived view — order summary per sample
-- frost automatically detects dependencies on both orders and vw_active_samples

USE DATABASE {{database_name}};
USE SCHEMA {{schema_name}};

CREATE OR ALTER VIEW vw_order_summary AS
SELECT
    s.id            AS sample_id,
    s.name          AS sample_name,
    COUNT(o.order_id) AS order_count,
    SUM(o.total_amount) AS total_amount,
    MAX(o.order_date)   AS last_order_date
FROM vw_active_samples s
LEFT JOIN orders o ON o.sample_id = s.id
GROUP BY s.id, s.name;
