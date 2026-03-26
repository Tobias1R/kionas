-- Phase 9: Simplified test focusing on supported operators
-- Status: Testing what's actually implemented in worker runtime

USE WAREHOUSE kionas-worker1;

CREATE DATABASE IF NOT EXISTS phase9_simple;
CREATE SCHEMA IF NOT EXISTS phase9_simple.test;

-- Reset table so repeated test runs do not accumulate duplicate rows.
DROP TABLE IF EXISTS phase9_simple.test.data;

CREATE TABLE IF NOT EXISTS phase9_simple.test.data (
    id INT,
    name VARCHAR(100),
    price INT,
    category VARCHAR(50)
);

-- Insert test data
INSERT INTO phase9_simple.test.data (id, name, price, category) VALUES (1, 'Laptop', 1200, 'Electronics');
INSERT INTO phase9_simple.test.data (id, name, price, category) VALUES (2, 'Mouse', 25, 'Electronics');
INSERT INTO phase9_simple.test.data (id, name, price, category) VALUES (3, 'Monitor', 350, 'Electronics');
INSERT INTO phase9_simple.test.data (id, name, price, category) VALUES (4, 'Desk', 200, 'Furniture');
INSERT INTO phase9_simple.test.data (id, name, price, category) VALUES (5, 'Chair', 180, 'Furniture');

-- ============================================================================
-- SUPPORTED: Phase 9b Comparisons (=, !=, >, >=, <, <=)
-- ============================================================================

-- Test: Exact equality
SELECT id, name, price FROM phase9_simple.test.data WHERE id = 1;

-- Test: Not equal
SELECT id, name, price FROM phase9_simple.test.data WHERE id != 1;

-- Test: Greater than
SELECT id, name, price FROM phase9_simple.test.data WHERE price > 100;

-- Test: Greater than or equal
SELECT id, name, price FROM phase9_simple.test.data WHERE price >= 200;

-- Test: Less than
SELECT id, name, price FROM phase9_simple.test.data WHERE price < 100;

-- Test: Less than or equal
SELECT id, name, price FROM phase9_simple.test.data WHERE price <= 200;

-- Test: String equality
SELECT id, name, category FROM phase9_simple.test.data WHERE category = 'Electronics';

-- Test: String inequality
SELECT id, name, category FROM phase9_simple.test.data WHERE category != 'Furniture';

-- Test: Combined predicates (AND)
SELECT id, name, price FROM phase9_simple.test.data WHERE price > 100 AND id <= 3;

-- ============================================================================
-- TESTING: Phase 9c BETWEEN/IN (recently enabled in planner)
-- ============================================================================

-- Test: BETWEEN integer
SELECT id, name, price FROM phase9_simple.test.data WHERE price BETWEEN 100 AND 300;

-- Test: BETWEEN string
SELECT id, name, category FROM phase9_simple.test.data WHERE category BETWEEN 'E' AND 'G';

-- Test: IN with integers
SELECT id, name FROM phase9_simple.test.data WHERE id IN (1, 3, 5);

-- Test: IN with strings
SELECT id, category FROM phase9_simple.test.data WHERE category IN ('Electronics', 'Furniture');

-- Test: NOT BETWEEN
SELECT id, name, price FROM phase9_simple.test.data WHERE price NOT BETWEEN 100 AND 300;

-- Test: Complex (BETWEEN AND IN)
SELECT id, name, price, category FROM phase9_simple.test.data 
WHERE price BETWEEN 50 AND 500 AND category IN ('Electronics', 'Furniture');

-- ============================================================================
-- NOT YET SUPPORTED: Phase 9a IS NULL / IS NOT NULL
-- These currently fail with: "expected one of =, !=, >, >=, <, <="
-- ============================================================================

-- Test: IS NULL (will fail - not implemented)
-- SELECT id, name FROM phase9_simple.test.data WHERE name IS NULL;

-- Test: IS NOT NULL (will fail - not implemented)
-- SELECT id, name FROM phase9_simple.test.data WHERE name IS NOT NULL;

-- ============================================================================
-- Cleanup
-- ============================================================================
-- DROP TABLE phase9_simple.test.data;
-- DROP SCHEMA phase9_simple.test;
-- DROP DATABASE phase9_simple;
