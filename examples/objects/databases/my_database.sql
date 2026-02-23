-- Declarative: Database setup
-- frost will detect this as a DATABASE object

USE ROLE {{admin_role}};

CREATE DATABASE IF NOT EXISTS {{database_name}};
