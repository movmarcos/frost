-- Declarative: orders table

USE DATABASE {{database_name}};
USE SCHEMA {{schema_name}};

CREATE OR ALTER TABLE orders (
    order_id        NUMBER AUTOINCREMENT PRIMARY KEY,
    sample_id       NUMBER NOT NULL REFERENCES sample_table(id),
    order_date      DATE NOT NULL DEFAULT CURRENT_DATE(),
    total_amount    NUMBER(12, 2),
    status          VARCHAR(50) DEFAULT 'PENDING',
    created_at      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
