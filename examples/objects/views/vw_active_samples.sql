-- Declarative: Base view — active samples
-- No dependency annotation needed; frost parses the FROM clause

USE DATABASE {{database_name}};
USE SCHEMA {{schema_name}};

CREATE OR ALTER VIEW vw_active_samples AS
SELECT
    id,
    name,
    description,
    created_at,
    created_by
FROM sample_table
WHERE status = 'ACTIVE';
