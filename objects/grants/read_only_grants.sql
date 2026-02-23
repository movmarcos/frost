-- Grants — uses @depends_on for explicit dependency on objects
-- frost re-runs this whenever its content changes
-- @depends_on: {{database_name}}.{{schema_name}}.SAMPLE_TABLE, {{database_name}}.{{schema_name}}.ORDERS, {{database_name}}.{{schema_name}}.VW_ACTIVE_SAMPLES, {{database_name}}.{{schema_name}}.VW_ORDER_SUMMARY

USE DATABASE {{database_name}};
USE SCHEMA {{schema_name}};

-- Grant usage
GRANT USAGE ON DATABASE {{database_name}} TO ROLE {{read_role}};
GRANT USAGE ON SCHEMA {{schema_name}} TO ROLE {{read_role}};

-- Grant select on all tables and views
GRANT SELECT ON ALL TABLES IN SCHEMA {{schema_name}} TO ROLE {{read_role}};
GRANT SELECT ON ALL VIEWS IN SCHEMA {{schema_name}} TO ROLE {{read_role}};
