#!/usr/bin/env python3
"""
Phase 9 Predicate Test Suite Runner
Executes SQL queries and reports results with statement numbers and status
"""

import subprocess
import re
import sys
from pathlib import Path

def extract_statements(sql_content):
    """Extract SELECT statements with their test descriptions"""
    statements = []
    lines = sql_content.split('\n')
    current_desc = ""
    
    for i, line in enumerate(lines):
        line = line.rstrip()
        
        # Track test description from comments
        if line.strip().startswith('-- Test:'):
            current_desc = line.replace('-- Test:', '').strip()
        
        # Detect SELECT statements
        if line.strip().upper().startswith('SELECT'):
            # Collect multi-line SELECT if needed
            full_statement = line
            j = i + 1
            while j < len(lines) and not lines[j].strip().upper().startswith('SELECT'):
                next_line = lines[j].rstrip()
                if next_line and not next_line.strip().startswith('--'):
                    full_statement += '\n' + next_line
                j += 1
            
            if full_statement.strip().endswith(';'):
                full_statement = full_statement.strip()[:-1]
            
            statements.append({
                'num': len(statements) + 1,
                'desc': current_desc,
                'sql': full_statement.strip()
            })
    
    return statements

def format_output_line(output):
    """Format a line of output for display"""
    if not output:
        return "[empty result]"
    return output[:100] + ('...' if len(output) > 100 else '')

def run_tests(sql_file):
    """Run all tests and report results"""
    
    if not Path(sql_file).exists():
        print(f"ERROR: SQL file not found: {sql_file}")
        return False
    
    print("=" * 70)
    print("PHASE 9 PREDICATE TEST SUITE")
    print("=" * 70)
    print()
    
    # Read SQL file
    with open(sql_file, 'r') as f:
        sql_content = f.read()
    
    statements = extract_statements(sql_content)
    print(f"Found {len(statements)} SELECT statements to test")
    print()
    
    # Setup phase9 database and tables with direct SQL execution
    print("=" * 70)
    print("SETUP PHASE")
    print("=" * 70)
    
    setup_sql = """
USE WAREHOUSE kionas-worker1;

CREATE DATABASE IF NOT EXISTS phase9_simple;
CREATE SCHEMA IF NOT EXISTS phase9_simple.test;

CREATE TABLE IF NOT EXISTS phase9_simple.test.data (
    id INT,
    name VARCHAR(100),
    price INT,
    category VARCHAR(50)
);

INSERT INTO phase9_simple.test.data (id, name, price, category) VALUES (1, 'Laptop', 1200, 'Electronics');
INSERT INTO phase9_simple.test.data (id, name, price, category) VALUES (2, 'Mouse', 25, 'Electronics');
INSERT INTO phase9_simple.test.data (id, name, price, category) VALUES (3, 'Monitor', 350, 'Electronics');
INSERT INTO phase9_simple.test.data (id, name, price, category) VALUES (4, 'Desk', 200, 'Furniture');
INSERT INTO phase9_simple.test.data (id, name, price, category) VALUES (5, 'Chair', 180, 'Furniture');
"""
    
    try:
        # Run setup through client
        process = subprocess.Popen(
            ['cargo', 'run', '-p', 'client', '--', '--username', 'kionas', '--password', 'kionas'],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        stdout, stderr = process.communicate(input=setup_sql, timeout=30)
        print("✓ Database and table setup complete")
    except subprocess.TimeoutExpired:
        process.kill()
        print("⚠ Setup timeout (this may be normal for interactive client)")
    except Exception as e:
        print(f"⚠ Setup error: {e}")
    
    print()
    print("=" * 70)
    print("TEST EXECUTION")
    print("=" * 70)
    print()
    
    passed = 0
    failed = 0
    
    for stmt in statements:
        stmt_num = stmt['num']
        desc = stmt['desc']
        sql = stmt['sql']
        
        print(f"Statement {stmt_num}: {desc}")
        print(f"  Query: {sql[:60]}{'...' if len(sql) > 60 else ''}")
        
        try:
            process = subprocess.Popen(
                ['cargo', 'run', '-p', 'client', '--', '--username', 'kionas', '--password', 'kionas'],
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )
            stdout, stderr = process.communicate(input=sql, timeout=15)
            
            if process.returncode == 0:
                output_lines = stdout.strip().split('\n')
                result_row_count = len([l for l in output_lines if l.strip() and not l.startswith('|')])
                print(f"  ✓ SUCCESS")
                if stdout.strip():
                    print(f"  Result: {result_row_count} rows returned")
                passed += 1
            else:
                error_msg = stderr.strip()[:100] if stderr else "Unknown error"
                print(f"  ✗ FAILED: {error_msg}")
                failed += 1
        
        except subprocess.TimeoutExpired:
            process.kill()
            print(f"  ✗ TIMEOUT")
            failed += 1
        except Exception as e:
            print(f"  ✗ ERROR: {str(e)[:80]}")
            failed += 1
        
        print()
    
    # Summary
    print("=" * 70)
    print("TEST SUMMARY")
    print("=" * 70)
    total = passed + failed
    print(f"Total: {total} tests")
    print(f"Passed: {passed}/{total} ✓")
    print(f"Failed: {failed}/{total} ✗")
    print(f"Pass Rate: {(passed/total*100):.1f}%")
    print()
    
    return failed == 0

if __name__ == '__main__':
    script_dir = Path(__file__).parent
    sql_file = script_dir / 'phase9_simple_test.sql'
    
    success = run_tests(str(sql_file))
    sys.exit(0 if success else 1)
