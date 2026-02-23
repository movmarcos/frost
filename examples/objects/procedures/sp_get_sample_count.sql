-- Declarative: Stored procedures

USE DATABASE {{database_name}};
USE SCHEMA {{schema_name}};

CREATE OR REPLACE PROCEDURE sp_get_sample_count()
RETURNS INTEGER
LANGUAGE SQL
AS
$$
DECLARE
    row_count INTEGER;
BEGIN
    SELECT COUNT(*) INTO :row_count FROM sample_table;
    RETURN row_count;
END;
$$;
