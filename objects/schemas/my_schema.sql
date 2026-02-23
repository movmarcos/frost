-- Declarative: Schema setup

USE ROLE {{admin_role}};
USE DATABASE {{database_name}};

CREATE SCHEMA IF NOT EXISTS {{schema_name}};
