-- Declarative: orders table
-- Database is set at connection level — use SCHEMA.OBJECT naming

CREATE OR ALTER TABLE PUBLIC.ORDERS (
    order_id        NUMBER AUTOINCREMENT PRIMARY KEY,
    sample_id       NUMBER NOT NULL REFERENCES PUBLIC.SAMPLE_TABLE(id),
    order_date      DATE NOT NULL DEFAULT CURRENT_DATE(),
    total_amount    NUMBER(12, 2),
    status          VARCHAR(50) DEFAULT 'PENDING',
    created_at      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
