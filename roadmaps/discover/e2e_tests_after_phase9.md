This is the end-to-end test plan for validating the implementation of BETWEEN and IN predicates after completing Phase 9c.
 As we can see some tests failed. Lets create a discovery document to investigate the failures and determine if they are expected based on the current implementation state or if they indicate regressions that need to be addressed before phase signoff. 
dont code anything yet, just create the document and outline the investigation steps.


## Latest Test Results
Running 25 SQL statements from file: ./scripts/test-scripts/phase9_simple_test.sql
\n--- Statement 1/25 ---
USE WAREHOUSE kionas-worker1
Query executed successfully
statement 1: SUCCESS
\n--- Statement 2/25 ---
CREATE DATABASE IF NOT EXISTS phase9_simple
Query executed successfully
statement 2: SUCCESS
\n--- Statement 3/25 ---
CREATE SCHEMA IF NOT EXISTS phase9_simple.test
Query executed successfully
statement 3: SUCCESS
\n--- Statement 4/25 ---
DROP TABLE IF EXISTS phase9_simple.test.data
Query failed with status=ERROR error_code=INFRA_GENERIC message=Unsupported statement
statement 4: FAILED (non-success status)
\n--- Statement 5/25 ---
CREATE TABLE IF NOT EXISTS phase9_simple.test.data (
    id INT,
    name VARCHAR(100),
    price INT,
    category VARCHAR(50)
)
Query executed successfully
statement 5: SUCCESS
\n--- Statement 6/25 ---
INSERT INTO phase9_simple.test.data (id, name, price, category) VALUES (1, 'Laptop', 1200, 'Electronics')  
Query executed successfully
statement 6: SUCCESS
\n--- Statement 7/25 ---
INSERT INTO phase9_simple.test.data (id, name, price, category) VALUES (2, 'Mouse', 25, 'Electronics')     
Query executed successfully
statement 7: SUCCESS
\n--- Statement 8/25 ---
INSERT INTO phase9_simple.test.data (id, name, price, category) VALUES (3, 'Monitor', 350, 'Electronics')  
Query executed successfully
statement 8: SUCCESS
\n--- Statement 9/25 ---
INSERT INTO phase9_simple.test.data (id, name, price, category) VALUES (4, 'Desk', 200, 'Furniture')       
Query executed successfully
statement 9: SUCCESS
\n--- Statement 10/25 ---
INSERT INTO phase9_simple.test.data (id, name, price, category) VALUES (5, 'Chair', 180, 'Furniture')      
Query executed successfully
statement 10: SUCCESS
\n--- Statement 11/25 ---
SELECT id, name, price FROM phase9_simple.test.data WHERE id = 1
+----+----------+-------+
| id | name     | price |
+----+----------+-------+
| 1  | 'Laptop' | 1200  |
| 1  | 'Laptop' | 1200  |
| 1  | 'Laptop' | 1200  |
| 1  | 'Laptop' | 1200  |
| 1  | 'Laptop' | 1200  |
+----+----------+-------+
rows=5 columns=3 batches=1
Query executed successfully
statement 11: SUCCESS
\n--- Statement 12/25 ---
SELECT id, name, price FROM phase9_simple.test.data WHERE id != 1
Query failed with status=ERROR error_code=INFRA_WORKER_QUERY_FAILED message=worker query dispatch failed: stage 0 partition 0 dispatch failed: unsupported filter column expression 'id <': only simple column references are supported
statement 12: FAILED (non-success status)
\n--- Statement 13/25 ---
SELECT id, name, price FROM phase9_simple.test.data WHERE price > 100
+----+-----------+-------+
| id | name      | price |
+----+-----------+-------+
| 3  | 'Monitor' | 350   |
| 4  | 'Desk'    | 200   |
| 1  | 'Laptop'  | 1200  |
| 3  | 'Monitor' | 350   |
| 5  | 'Chair'   | 180   |
| 3  | 'Monitor' | 350   |
| 1  | 'Laptop'  | 1200  |
| 1  | 'Laptop'  | 1200  |
| 3  | 'Monitor' | 350   |
| 4  | 'Desk'    | 200   |
| 3  | 'Monitor' | 350   |
| 1  | 'Laptop'  | 1200  |
| 4  | 'Desk'    | 200   |
| 5  | 'Chair'   | 180   |
| 5  | 'Chair'   | 180   |
| 5  | 'Chair'   | 180   |
| 4  | 'Desk'    | 200   |
| 4  | 'Desk'    | 200   |
| 5  | 'Chair'   | 180   |
| 1  | 'Laptop'  | 1200  |
+----+-----------+-------+
rows=20 columns=3 batches=1
Query executed successfully
statement 13: SUCCESS
\n--- Statement 14/25 ---
SELECT id, name, price FROM phase9_simple.test.data WHERE price >= 200
+----+-----------+-------+
| id | name      | price |
+----+-----------+-------+
| 3  | 'Monitor' | 350   |
| 4  | 'Desk'    | 200   |
| 1  | 'Laptop'  | 1200  |
| 3  | 'Monitor' | 350   |
| 3  | 'Monitor' | 350   |
| 1  | 'Laptop'  | 1200  |
| 1  | 'Laptop'  | 1200  |
| 3  | 'Monitor' | 350   |
| 4  | 'Desk'    | 200   |
| 3  | 'Monitor' | 350   |
| 1  | 'Laptop'  | 1200  |
| 4  | 'Desk'    | 200   |
| 4  | 'Desk'    | 200   |
| 4  | 'Desk'    | 200   |
| 1  | 'Laptop'  | 1200  |
+----+-----------+-------+
rows=15 columns=3 batches=1
Query executed successfully
statement 14: SUCCESS
\n--- Statement 15/25 ---
SELECT id, name, price FROM phase9_simple.test.data WHERE price < 100
+----+---------+-------+
| id | name    | price |
+----+---------+-------+
| 2  | 'Mouse' | 25    |
| 2  | 'Mouse' | 25    |
| 2  | 'Mouse' | 25    |
| 2  | 'Mouse' | 25    |
| 2  | 'Mouse' | 25    |
+----+---------+-------+
rows=5 columns=3 batches=1
Query executed successfully
statement 15: SUCCESS
\n--- Statement 16/25 ---
SELECT id, name, price FROM phase9_simple.test.data WHERE price <= 200
+----+---------+-------+
| id | name    | price |
+----+---------+-------+
| 4  | 'Desk'  | 200   |
| 2  | 'Mouse' | 25    |
| 2  | 'Mouse' | 25    |
| 2  | 'Mouse' | 25    |
| 5  | 'Chair' | 180   |
| 2  | 'Mouse' | 25    |
| 4  | 'Desk'  | 200   |
| 4  | 'Desk'  | 200   |
| 5  | 'Chair' | 180   |
| 5  | 'Chair' | 180   |
| 5  | 'Chair' | 180   |
| 2  | 'Mouse' | 25    |
| 4  | 'Desk'  | 200   |
| 4  | 'Desk'  | 200   |
| 5  | 'Chair' | 180   |
+----+---------+-------+
rows=15 columns=3 batches=1
Query executed successfully
statement 16: SUCCESS
\n--- Statement 17/25 ---
SELECT id, name, category FROM phase9_simple.test.data WHERE category = 'Electronics'
+----+------+----------+
| id | name | category |
+----+------+----------+
+----+------+----------+
rows=0 columns=3 batches=1
Query executed successfully
statement 17: SUCCESS
\n--- Statement 18/25 ---
SELECT id, name, category FROM phase9_simple.test.data WHERE category != 'Furniture'
Query failed with status=ERROR error_code=INFRA_WORKER_QUERY_FAILED message=worker query dispatch failed: stage 0 partition 0 dispatch failed: unsupported filter column expression 'category <': only simple column references are supported
statement 18: FAILED (non-success status)
\n--- Statement 19/25 ---
SELECT id, name, price FROM phase9_simple.test.data WHERE price > 100 AND id <= 3
+----+-----------+-------+
| id | name      | price |
+----+-----------+-------+
| 3  | 'Monitor' | 350   |
| 1  | 'Laptop'  | 1200  |
| 3  | 'Monitor' | 350   |
| 3  | 'Monitor' | 350   |
| 1  | 'Laptop'  | 1200  |
| 1  | 'Laptop'  | 1200  |
| 3  | 'Monitor' | 350   |
| 3  | 'Monitor' | 350   |
| 1  | 'Laptop'  | 1200  |
| 1  | 'Laptop'  | 1200  |
+----+-----------+-------+
rows=10 columns=3 batches=1
Query executed successfully
statement 19: SUCCESS
\n--- Statement 20/25 ---
SELECT id, name, price FROM phase9_simple.test.data WHERE price BETWEEN 100 AND 300
+----+---------+-------+
| id | name    | price |
+----+---------+-------+
| 4  | 'Desk'  | 200   |
| 5  | 'Chair' | 180   |
| 4  | 'Desk'  | 200   |
| 4  | 'Desk'  | 200   |
| 5  | 'Chair' | 180   |
| 5  | 'Chair' | 180   |
| 5  | 'Chair' | 180   |
| 4  | 'Desk'  | 200   |
| 4  | 'Desk'  | 200   |
| 5  | 'Chair' | 180   |
+----+---------+-------+
rows=10 columns=3 batches=1
Query executed successfully
statement 20: SUCCESS
\n--- Statement 21/25 ---
SELECT id, name, category FROM phase9_simple.test.data WHERE category BETWEEN 'E' AND 'G'
+----+------+----------+
| id | name | category |
+----+------+----------+
+----+------+----------+
rows=0 columns=3 batches=1
Query executed successfully
statement 21: SUCCESS
\n--- Statement 22/25 ---
SELECT id, name FROM phase9_simple.test.data WHERE id IN (1, 3, 5)
+----+-----------+
| id | name      |
+----+-----------+
| 3  | 'Monitor' |
| 1  | 'Laptop'  |
| 3  | 'Monitor' |
| 5  | 'Chair'   |
| 3  | 'Monitor' |
| 1  | 'Laptop'  |
| 1  | 'Laptop'  |
| 3  | 'Monitor' |
| 3  | 'Monitor' |
| 1  | 'Laptop'  |
| 5  | 'Chair'   |
| 5  | 'Chair'   |
| 5  | 'Chair'   |
| 5  | 'Chair'   |
| 1  | 'Laptop'  |
+----+-----------+
rows=15 columns=2 batches=1
Query executed successfully
statement 22: SUCCESS
\n--- Statement 23/25 ---
SELECT id, category FROM phase9_simple.test.data WHERE category IN ('Electronics', 'Furniture')
+----+----------+
| id | category |
+----+----------+
+----+----------+
rows=0 columns=2 batches=1
Query executed successfully
statement 23: SUCCESS
\n--- Statement 24/25 ---
SELECT id, name, price FROM phase9_simple.test.data WHERE price NOT BETWEEN 100 AND 300
+----+-----------+-------+
| id | name      | price |
+----+-----------+-------+
| 3  | 'Monitor' | 350   |
| 1  | 'Laptop'  | 1200  |
| 2  | 'Mouse'   | 25    |
| 2  | 'Mouse'   | 25    |
| 2  | 'Mouse'   | 25    |
| 3  | 'Monitor' | 350   |
| 3  | 'Monitor' | 350   |
| 2  | 'Mouse'   | 25    |
| 1  | 'Laptop'  | 1200  |
| 1  | 'Laptop'  | 1200  |
| 3  | 'Monitor' | 350   |
| 3  | 'Monitor' | 350   |
| 1  | 'Laptop'  | 1200  |
| 2  | 'Mouse'   | 25    |
| 1  | 'Laptop'  | 1200  |
+----+-----------+-------+
rows=15 columns=3 batches=1
Query executed successfully
statement 24: SUCCESS
\n--- Statement 25/25 ---
SELECT id, name, price, category FROM phase9_simple.test.data
WHERE price BETWEEN 50 AND 500 AND category IN ('Electronics', 'Furniture')
Query failed with status=ERROR error_code=INFRA_WORKER_QUERY_FAILED message=worker query dispatch failed: stage 0 partition 0 dispatch failed: unsupported filter literal '500 AND category IN ('Electronics', 'Furniture')': expected int, bool, or quoted string
statement 25: FAILED (non-success status)
\nExecution summary: total=25 success=21 failed=4
query-file error: failed statements at indexes: [4, 12, 18, 25]
root@ab92486f2179:/workspace# 


## Discovery Document: Phase 9c End-to-End Test Analysis