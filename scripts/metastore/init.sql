
/*
This is the initialization SQL script for the Kionas metastore. It sets up the necessary 
database schema and tables for storing metadata about datasets, tables, and partitions.

Kionas metastore is PostgreSQL-based and is designed to store metadata for datasets that are 
registered in the Kionas data warehouse.
*/

-- Create the metastore database
CREATE DATABASE IF NOT EXISTS kionas_metastore;
-- Use the metastore database
\c kionas_metastore;

-- Catalog table to store information about datasets
CREATE TABLE IF NOT EXISTS catalogs (
    id BIGSERIAL PRIMARY KEY,
    name VARCHAR(255) UNIQUE NOT NULL,
    description TEXT,
    owner VARCHAR(255) NOT NULL DEFAULT 'kionas',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table to store information about tables in the metastore
CREATE TABLE IF NOT EXISTS tables (
    id BIGSERIAL PRIMARY KEY,
    catalog_id INT NOT NULL,
    name VARCHAR(255) NOT NULL,
    engine VARCHAR(255) NOT NULL,    
    schema JSONB NOT NULL,
    location VARCHAR(1024) NOT NULL,
    owner VARCHAR(255) NOT NULL DEFAULT 'kionas',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (catalog_id) REFERENCES catalogs(id) ON DELETE CASCADE
);

-- Table to store information about partitions for partitioned tables
CREATE TABLE IF NOT EXISTS partitions (
    id BIGSERIAL PRIMARY KEY,
    table_id INT NOT NULL,
    values JSONB NOT NULL,
    location VARCHAR(1024) NOT NULL,
    owner VARCHAR(255) NOT NULL DEFAULT 'kionas',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (table_id) REFERENCES tables(id) ON DELETE CASCADE
);

-- Indexes for faster lookups
CREATE INDEX IF NOT EXISTS idx_tables_catalog_id ON tables(catalog_id);
CREATE INDEX IF NOT EXISTS idx_partitions_table_id ON partitions(table_id);
CREATE INDEX IF NOT EXISTS idx_tables_name ON tables(name);
CREATE INDEX IF NOT EXISTS idx_partitions_values ON partitions(values);

-- statistics table
CREATE TABLE IF NOT EXISTS statistics (
    id BIGSERIAL PRIMARY KEY,
    table_id INT NOT NULL,
    column_name VARCHAR(255) NOT NULL,
    num_rows BIGINT,
    num_distinct_values BIGINT,
    null_count BIGINT,
    min_value VARCHAR(255),
    max_value VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (table_id) REFERENCES tables(id) ON DELETE CASCADE
);
