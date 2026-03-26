-- Phase 9 Storage Persistence Diagnostic
-- Tests: Focus on storage layer and data availability

-- ============================================================================
-- Setup
-- ============================================================================

CREATE DATABASE IF NOT EXISTS phase9_storage_test;

CREATE SCHEMA IF NOT EXISTS phase9_storage_test.diag;

-- Simple table 
CREATE TABLE IF NOT EXISTS phase9_storage_test.diag.simple_data (
    id INT,
    value VARCHAR(50)
);

-- ============================================================================
-- Test 1: Insert and immediate verification
-- ============================================================================

-- Insert 3 rows
INSERT INTO phase9_storage_test.diag.simple_data VALUES
(1, 'A'),
(2, 'B'),
(3, NULL);

-- Immediate query - does data appear?
SELECT 'Test 1: Immediate query after INSERT' AS diagnostic, id, value 
FROM phase9_storage_test.diag.simple_data;

-- ============================================================================
-- Test 2: Phase 9a predicates (should work if storage works)
-- ============================================================================

SELECT 'Test 2a: IS NULL' AS diagnostic, id, value 
FROM phase9_storage_test.diag.simple_data 
WHERE value IS NULL;

SELECT 'Test 2b: IS NOT NULL' AS diagnostic, id, value 
FROM phase9_storage_test.diag.simple_data 
WHERE value IS NOT NULL;

-- ============================================================================
-- Test 3: Phase 9b predicates (type coercion)
-- ============================================================================

SELECT 'Test 3a: Basic = comparison' AS diagnostic, id, value 
FROM phase9_storage_test.diag.simple_data 
WHERE id = 1;

SELECT 'Test 3b: > comparison' AS diagnostic, id, value 
FROM phase9_storage_test.diag.simple_data 
WHERE id > 1;

-- ============================================================================
-- Test 4: Phase 9c predicates (BETWEEN/IN - now gated open)
-- ============================================================================

SELECT 'Test 4a: BETWEEN' AS diagnostic, id, value 
FROM phase9_storage_test.diag.simple_data 
WHERE id BETWEEN 1 AND 2;

SELECT 'Test 4b: IN' AS diagnostic, id, value 
FROM phase9_storage_test.diag.simple_data 
WHERE id IN (1, 3);

-- Cleanup
-- DROP TABLE phase9_storage_test.diag.simple_data;
-- DROP SCHEMA phase9_storage_test.diag;
-- DROP DATABASE phase9_storage_test;
