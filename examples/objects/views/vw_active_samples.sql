-- Declarative: Base view — active samples
-- No dependency annotation needed; frost parses the FROM clause
-- Database is set at connection level — use SCHEMA.OBJECT naming

CREATE OR ALTER VIEW PUBLIC.VW_ACTIVE_SAMPLES AS
SELECT
    id,
    name,
    description,
    created_at,
    created_by
FROM PUBLIC.SAMPLE_TABLE
WHERE status = 'ACTIVE';
