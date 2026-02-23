-- Grants — uses @depends_on for explicit dependency on objects
-- frost re-runs this whenever its content changes
-- @depends_on: PUBLIC.SAMPLE_TABLE, PUBLIC.ORDERS, PUBLIC.VW_ACTIVE_SAMPLES, PUBLIC.VW_ORDER_SUMMARY

-- Grant select on all tables and views
GRANT SELECT ON ALL TABLES IN SCHEMA PUBLIC TO ROLE {{read_role}};
GRANT SELECT ON ALL VIEWS IN SCHEMA PUBLIC TO ROLE {{read_role}};
