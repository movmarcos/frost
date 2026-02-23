-- Declarative: sample_table
-- frost detects this CREATE OR ALTER and resolves dependencies automatically
-- Database is set at connection level — use SCHEMA.OBJECT naming

CREATE OR ALTER TABLE PUBLIC.SAMPLE_TABLE (
    id              NUMBER AUTOINCREMENT PRIMARY KEY,
    name            VARCHAR(255) NOT NULL,
    description     VARCHAR(1000),
    status          VARCHAR(50)  DEFAULT 'ACTIVE',
    created_at      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_at      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    created_by      VARCHAR(255),
    updated_by      VARCHAR(255)
);
