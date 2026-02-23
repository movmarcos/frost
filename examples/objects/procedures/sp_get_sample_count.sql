-- Declarative: Stored procedure
-- Database is set at connection level — use SCHEMA.OBJECT naming

CREATE OR ALTER PROCEDURE PUBLIC.SP_GET_SAMPLE_COUNT()
RETURNS INTEGER
LANGUAGE SQL
AS
$$
DECLARE
    row_count INTEGER;
BEGIN
    SELECT COUNT(*) INTO :row_count FROM PUBLIC.SAMPLE_TABLE;
    RETURN row_count;
END;
$$;
