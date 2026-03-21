-- Phase 9: Query part II - NULL semantics and type coercion test script
-- Tests: IS/IS NOT (9a), Type Coercion (9b), BETWEEN/IN (9c)

-- ============================================================================
-- Setup: Create database, schema, table
-- ============================================================================
USE WAREHOUSE kionas-worker1;

CREATE DATABASE IF NOT EXISTS zxczzx;

CREATE SCHEMA IF NOT EXISTS zxczzx.test_schema;

-- Create test table with various data types for comprehensive testing
CREATE TABLE IF NOT EXISTS zxczzx.test_schema.products (
    id INT,
    name VARCHAR(100),
    price INT,
    quantity INT,
    category VARCHAR(50),
    description VARCHAR(255),
    discount_rate FLOAT,
    is_active BOOLEAN
);

-- ============================================================================
-- Insert test data: Mix of valid values and NULLs
-- ============================================================================

INSERT INTO zxczzx.test_schema.products (id, name, price, quantity, category, description, discount_rate, is_active) VALUES
(1, 'Laptop', 1200, 50, 'Electronics', 'High-performance laptop', 10.5, true);

INSERT INTO zxczzx.test_schema.products (id, name, price, quantity, category, description, discount_rate, is_active) VALUES
(2, 'Mouse', 25, NULL, 'Electronics', NULL, 5.0, true);

INSERT INTO zxczzx.test_schema.products (id, name, price, quantity, category, description, discount_rate, is_active) VALUES
(3, 'Keyboard', 75, 100, NULL, 'Mechanical keyboard', NULL, true);

INSERT INTO zxczzx.test_schema.products (id, name, price, quantity, category, description, discount_rate, is_active) VALUES
(4, 'Monitor', 350, 30, 'Electronics', '4K display', 15.0, false);

INSERT INTO zxczzx.test_schema.products (id, name, price, quantity, category, description, discount_rate, is_active) VALUES
(5, NULL, 500, 20, 'Accessories', 'Wireless charger', 8.0, true);

INSERT INTO zxczzx.test_schema.products (id, name, price, quantity, category, description, discount_rate, is_active) VALUES
(6, 'Headphones', 150, NULL, 'Electronics', NULL, 12.0, NULL);

INSERT INTO zxczzx.test_schema.products (id, name, price, quantity, category, description, discount_rate, is_active) VALUES
(7, 'Desk', 200, 15, 'Furniture', 'Ergonomic desk', 20.0, true);

INSERT INTO zxczzx.test_schema.products (id, name, price, quantity, category, description, discount_rate, is_active) VALUES
(8, 'Chair', 180, NULL, 'Furniture', NULL, NULL, false);

INSERT INTO zxczzx.test_schema.products (id, name, price, quantity, category, description, discount_rate, is_active) VALUES
(9, 'Lamp', 45, 80, 'Lighting', 'LED lamp', 5.5, true);

INSERT INTO zxczzx.test_schema.products (id, name, price, quantity, category, description, discount_rate, is_active) VALUES
(10, 'Cable', 15, 500, 'Accessories', 'USB-C cable', 2.0, true);

-- ============================================================================
-- Phase 9a: IS NULL and IS NOT NULL Predicates
-- ============================================================================

-- Test 1: Find products with NULL names
SELECT 'Phase 9a Test 1: NULL names' AS test_name, id, name FROM zxczzx.test_schema.products WHERE name IS NULL;

SELECT * FROM zxczzx.test_schema.products;

-- Test 2: Find products with non-NULL names
SELECT id, name FROM zxczzx.test_schema.products WHERE name IS NOT NULL;

-- Test 3: Find products with NULL descriptions
SELECT 'Phase 9a Test 3: NULL descriptions' AS test_name, id, description FROM zxczzx.test_schema.products WHERE description IS NULL;

-- Test 4: Find products with non-NULL descriptions
SELECT 'Phase 9a Test 4: Non-NULL descriptions' AS test_name, id, description FROM zxczzx.test_schema.products WHERE description IS NOT NULL;

-- Test 5: Find products with NULL quantity (combined with other predicates)
SELECT 'Phase 9a Test 5: NULL quantity' AS test_name, id, name, quantity FROM zxczzx.test_schema.products WHERE quantity IS NULL;

-- Test 6: Find active products (non-NULL is_active AND is_active = true)
SELECT 'Phase 9a Test 6: Active products' AS test_name, id, name, is_active FROM zxczzx.test_schema.products WHERE is_active IS NOT NULL;

-- Test 7: Complex NULL predicate (multiple columns)
SELECT 'Phase 9a Test 7: Multiple NULL checks' AS test_name, id, name, description, quantity 
FROM zxczzx.test_schema.products 
WHERE name IS NOT NULL AND description IS NOT NULL AND quantity IS NOT NULL;

-- ============================================================================
-- Phase 9b: Type Coercion with Comparison Operators
-- ============================================================================

-- Test 8: Type matching - integer comparison (price = 150)
SELECT id, name, price FROM zxczzx.test_schema.products WHERE price = 150;

-- Test 9: Type matching - greater than (price > 100)
SELECT 'Phase 9b Test 9: Price greater than 100' AS test_name, id, name, price FROM zxczzx.test_schema.products WHERE price > 100;

-- Test 10: Type matching - less than (price < 100)
SELECT 'Phase 9b Test 10: Price less than 100' AS test_name, id, name, price FROM zxczzx.test_schema.products WHERE price < 100;

-- Test 11: Type matching - less than or equal (price <= 75)
SELECT 'Phase 9b Test 11: Price <= 75' AS test_name, id, name, price FROM zxczzx.test_schema.products WHERE price <= 75;

-- Test 12: Type matching - greater than or equal (price >= 200)
SELECT 'Phase 9b Test 12: Price >= 200' AS test_name, id, name, price FROM zxczzx.test_schema.products WHERE price >= 200;

-- Test 13: Type matching - not equal (id != 1)
SELECT 'Phase 9b Test 13: ID not equal to 1' AS test_name, id, name FROM zxczzx.test_schema.products WHERE id != 1;

-- Test 14: String comparison (category = 'Electronics')
SELECT 'Phase 9b Test 14: Category equals Electronics' AS test_name, id, name, category FROM zxczzx.test_schema.products WHERE category = 'Electronics';

-- Test 15: String comparison with non-match
SELECT 'Phase 9b Test 15: Category not Furniture' AS test_name, id, name, category FROM zxczzx.test_schema.products WHERE category != 'Furniture';

-- Test 16: Combining type-safe predicates (price > 100 AND quantity IS NOT NULL)
SELECT 'Phase 9b Test 16: Combined predicates' AS test_name, id, name, price, quantity 
FROM zxczzx.test_schema.products 
WHERE price > 100 AND quantity IS NOT NULL;

-- ============================================================================
-- Phase 9c: BETWEEN and IN Predicates
-- ============================================================================

-- Test 17: BETWEEN with integer bounds (price BETWEEN 50 AND 200)
SELECT 'Phase 9c Test 17: Price BETWEEN 50 and 200' AS test_name, id, name, price 
FROM zxczzx.test_schema.products 
WHERE price BETWEEN 50 AND 200;

-- Test 18: IN with single value
SELECT 'Phase 9c Test 18: ID IN (1,5,9)' AS test_name, id, name 
FROM zxczzx.test_schema.products 
WHERE id IN (1, 5, 9);

-- Test 19: IN with multiple string values
SELECT 'Phase 9c Test 19: Category IN Electronics,Furniture' AS test_name, id, name, category 
FROM zxczzx.test_schema.products 
WHERE category IN ('Electronics', 'Furniture');

-- Test 20: BETWEEN with string bounds (lexicographic)
SELECT 'Phase 9c Test 20: Category BETWEEN A and M' AS test_name, id, name, category 
FROM zxczzx.test_schema.products 
WHERE category BETWEEN 'A' AND 'M';

-- Test 21: NOT BETWEEN (complement)
SELECT 'Phase 9c Test 21: Price NOT BETWEEN 50 and 150' AS test_name, id, name, price 
FROM zxczzx.test_schema.products 
WHERE price NOT BETWEEN 50 AND 150;

-- Test 22: BETWEEN with order (end > start)
SELECT 'Phase 9c Test 22: ID BETWEEN 3 and 7' AS test_name, id, name 
FROM zxczzx.test_schema.products 
WHERE id BETWEEN 3 AND 7;

-- Test 23: IN with homogeneous integer list
SELECT 'Phase 9c Test 23: ID IN multiple values' AS test_name, id, name, quantity 
FROM zxczzx.test_schema.products 
WHERE id IN (2, 4, 6, 8, 10);

-- Test 24: Combining BETWEEN with other predicates (price BETWEEN 100 AND 300 AND quantity IS NOT NULL)
SELECT 'Phase 9c Test 24: BETWEEN combined' AS test_name, id, name, price, quantity 
FROM zxczzx.test_schema.products 
WHERE price BETWEEN 100 AND 300 AND quantity IS NOT NULL;

-- ============================================================================
-- Advanced: Combined Phase 9 Scenarios
-- ============================================================================

-- Test 25: Complex query mixing all three phases
SELECT 'Phase 9 Combined Test 25' AS test_name, id, name, price, category, quantity, description
FROM zxczzx.test_schema.products
WHERE 
    name IS NOT NULL
    AND price BETWEEN 50 AND 300
    AND category IN ('Electronics', 'Furniture')
    AND quantity IS NOT NULL
ORDER BY price DESC;

-- Test 26: NULL handling with IN (should NOT match NULLs in list context)
SELECT 'Phase 9 Combined Test 26' AS test_name, id, name, category
FROM zxczzx.test_schema.products
WHERE category NOT IN ('Electronics', 'Furniture') AND category IS NOT NULL;

-- Test 27: Complex filter with all predicate types
SELECT 'Phase 9 Combined Test 27' AS test_name, id, name, price, quantity, discount_rate
FROM zxczzx.test_schema.products
WHERE 
    (price BETWEEN 20 AND 400 OR price IS NULL)
    AND (id IN (1, 3, 5, 7, 9) OR id > 8)
    AND name IS NOT NULL;

-- Test 28: Validation scenario - verify type safety works
SELECT COUNT(*) as total_rows FROM zxczzx.test_schema.products WHERE price > 0 AND price < 1000;

-- ============================================================================
-- Cleanup (optional - uncomment to reset)
-- ============================================================================
-- DROP TABLE zxczzx.test_schema.products;
-- DROP SCHEMA zxczzx.test_schema;
-- DROP DATABASE zxczzx;
