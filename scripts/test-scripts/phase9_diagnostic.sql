-- Phase 9 Diagnostic Test Script
-- Tests only Phase 9a (IS/IS NOT) which should be fully working
-- Skip Phase 9c (BETWEEN/IN) for now since it's gated

-- ============================================================================
-- Setup: Create database, schema, table
-- ============================================================================

CREATE DATABASE IF NOT EXISTS test_phase9_diag;

CREATE SCHEMA IF NOT EXISTS test_phase9_diag.diag_schema;

-- Simple table for basic testing
CREATE TABLE IF NOT EXISTS test_phase9_diag.diag_schema.test_data (
    id INT,
    name VARCHAR(100),
    price INT
);

-- ============================================================================
-- Insert test data
-- ============================================================================

INSERT INTO test_phase9_diag.diag_schema.test_data VALUES
(1, 'Product A', 100),
(2, NULL, 200),
(3, 'Product C', NULL),
(4, 'Product D', 400),
(5, NULL, NULL);

-- ============================================================================
-- Phase 9a: ONLY IS NULL and IS NOT NULL (these should work)
-- ============================================================================

-- Test 1: Find NULL names
SELECT 'Test 1: IS NULL' AS test_name, id, name FROM test_phase9_diag.diag_schema.test_data WHERE name IS NULL;

-- Test 2: Find non-NULL names
SELECT 'Test 2: IS NOT NULL' AS test_name, id, name FROM test_phase9_diag.diag_schema.test_data WHERE name IS NOT NULL;

-- Test 3: Find NULL prices
SELECT 'Test 3: IS NULL price' AS test_name, id, price FROM test_phase9_diag.diag_schema.test_data WHERE price IS NULL;

-- Test 4: Find non-NULL prices
SELECT 'Test 4: IS NOT NULL price' AS test_name, id, price FROM test_phase9_diag.diag_schema.test_data WHERE price IS NOT NULL;

-- Test 5: Combined IS NULL checks
SELECT 'Test 5: Combined IS NULL' AS test_name, id, name, price FROM test_phase9_diag.diag_schema.test_data WHERE name IS NULL AND price IS NULL;

-- Test 6: Basic comparison (should work for Phase 9b)
SELECT 'Test 6: Basic = operator' AS test_name, id, name, price FROM test_phase9_diag.diag_schema.test_data WHERE id = 1;

-- Test 7: Greater than (should work for Phase 9b)
SELECT 'Test 7: > operator' AS test_name, id, name, price FROM test_phase9_diag.diag_schema.test_data WHERE price > 100;

-- Cleanup
-- DROP TABLE test_phase9_diag.diag_schema.test_data;
-- DROP SCHEMA test_phase9_diag.diag_schema;
-- DROP DATABASE test_phase9_diag;
