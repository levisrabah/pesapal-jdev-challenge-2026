
"""
Comprehensive RDBMS Verification Test Suite

This script verifies all core functionality of the Custom RDBMS:
- Table creation
- Data insertion with constraint validation
- SELECT queries
- INNER JOIN operations
- Soft delete functionality
- Error handling
"""

import os
import json
from core.storage import Storage
from core.indexing import IndexManager
from core.parser import SQLParser
from core.engine import DatabaseEngine

def print_pass(message: str):
    """Print test pass message."""
    pass  # Silent pass

def print_fail(message: str):
    """Print test fail message."""
    pass  # Silent fail

def cleanup_test_tables():
    """Remove test tables if they exist."""
    storage = Storage()
    for table in ['users', 'transactions']:
        if storage.table_exists(table):
            storage.drop_table(table)

def test_table_creation():
    """Test 1: Create tables with proper schema."""
    storage = Storage()
    index_manager = IndexManager()
    parser = SQLParser()
    engine = DatabaseEngine(storage, index_manager)
    
    try:
        create_users = parser.parse(
            "CREATE TABLE users (id INT PRIMARY KEY, name TEXT, email TEXT UNIQUE)"
        )
        engine.create_table(
            create_users['table_name'],
            create_users['schema'],
            create_users.get('primary_key'),
            create_users.get('unique_keys', [])
        )
        
        assert storage.table_exists('users'), "Users table should exist"
        table_data = storage.load_table('users')
        assert table_data['metadata']['primary_key'] == 'id'
        assert 'email' in table_data['metadata']['unique_keys']
        assert table_data['metadata']['created_at'] is not None
        
        create_transactions = parser.parse(
            "CREATE TABLE transactions (id INT PRIMARY KEY, user_id INT, amount FLOAT)"
        )
        engine.create_table(
            create_transactions['table_name'],
            create_transactions['schema'],
            create_transactions.get('primary_key'),
            create_transactions.get('unique_keys', [])
        )
        
        assert storage.table_exists('transactions'), "Transactions table should exist"
        return True
        
    except Exception:
        return False
    
def test_insert_and_constraints():
    """Test 2: Insert valid data and test constraint violations."""
    storage = Storage()
    index_manager = IndexManager()
    parser = SQLParser()
    engine = DatabaseEngine(storage, index_manager)
    
    try:
        users = [
            (1, 'Sammy Sammy', 'ss@example.com'),
            (2, 'Bobuu GG', 'bg@example.com'),
            (3, 'Owade Charles', 'cowade@example.com')
        ]
        
        for user_id, name, email in users:
            engine.insert('users', {'id': user_id, 'name': name, 'email': email})
        
        all_users = engine.select('users')
        assert len(all_users) == 3
        for user in all_users:
            assert 'created_at' in user
            assert user['created_at'] is not None
        
        try:
            engine.insert('users', {'id': 1, 'name': 'Duplicate', 'email': 'dup@example.com'})
            return False
        except ValueError as e:
            assert "already exists" in str(e).lower()
        
        duplicate_key_passed = False
        try:
            engine.insert('users', {'id': 4, 'name': 'Duplicate Email', 'email': 'ss@example.com'})
            return False
        except ValueError as e:
            error_msg = str(e).lower()
            if "unique constraint" in error_msg or "already exists" in error_msg:
                duplicate_key_passed = True
        
        transactions = [
            (1, 1, 100.50),
            (2, 1, 250.75),
            (3, 2, 500.00),
            (4, 3, 75.25)
        ]
        
        for txn_id, user_id, amount in transactions:
            engine.insert('transactions', {'id': txn_id, 'user_id': user_id, 'amount': amount})
        
        all_transactions = engine.select('transactions')
        assert len(all_transactions) == 4
        
        return duplicate_key_passed
        
    except Exception:
        return False

def test_inner_join():
    """Test 3: Perform INNER JOIN and verify correct mapping."""
    storage = Storage()
    index_manager = IndexManager()
    engine = DatabaseEngine(storage, index_manager)
    
    try:
        joined_rows = engine.inner_join(
            'transactions',
            'users',
            'user_id',
            'id',
            None,
            None
        )
        
        assert len(joined_rows) > 0
        
        first_row = joined_rows[0]
        assert 'transactions.id' in first_row or 'transactions.user_id' in first_row
        assert 'users.id' in first_row or 'users.name' in first_row
        
        users_dict = {user['id']: user['name'] for user in engine.select('users')}
        
        for row in joined_rows:
            txn_user_id = row.get('transactions.user_id') or row.get('user_id')
            user_name = row.get('users.name') or row.get('name')
            
            if txn_user_id and user_name:
                expected_name = users_dict.get(txn_user_id)
                assert user_name == expected_name
        
        joined_specific = engine.inner_join(
            'transactions',
            'users',
            'user_id',
            'id',
            ['transactions.amount', 'users.name'],
            None
        )
        
        assert len(joined_specific) > 0
        assert 'transactions.amount' in joined_specific[0]
        assert 'users.name' in joined_specific[0]
        
        return True
        
    except Exception:
        return False
    
def test_soft_delete():
    """Test 4: Verify soft delete functionality."""
    storage = Storage()
    index_manager = IndexManager()
    parser = SQLParser()
    engine = DatabaseEngine(storage, index_manager)
    
    try:
        initial_count = len(engine.select('transactions'))
        assert initial_count > 0
        
        delete_query = parser.parse("DELETE FROM transactions WHERE id = 1")
        deleted_count = engine.delete(
            delete_query['table_name'],
            delete_query.get('where')
        )
        
        assert deleted_count == 1
        
        remaining = engine.select('transactions')
        assert len(remaining) == initial_count - 1
        
        deleted_ids = [row['id'] for row in remaining]
        assert 1 not in deleted_ids
        
        table_data = storage.load_table('transactions')
        all_rows = table_data['rows']
        
        deleted_row = None
        for row in all_rows:
            if row['id'] == 1:
                deleted_row = row
                break
        
        assert deleted_row is not None
        assert deleted_row.get('is_deleted') == True
        
        update_count = engine.update(
            'transactions',
            {'amount': 999.99},
            {'column': 'id', 'operator': '=', 'value': 1}
        )
        assert update_count == 0
        
        return True
        
    except Exception:
        return False

def test_error_handling():
    """Test 5: Verify error handling for malformed SQL."""
    parser = SQLParser()
    
    test_cases = [
        ("INVALID COMMAND", "Unsupported SQL command"),
        ("SELECT FROM", "Invalid SELECT syntax"),
        ("CREATE TABLE", "Invalid CREATE TABLE syntax"),
        ("INSERT INTO users VALUES", "Invalid INSERT syntax"),
        ("UPDATE users SET", "Invalid UPDATE syntax"),
        ("DELETE FROM", "Invalid DELETE syntax"),
    ]
    
    for sql, expected_error in test_cases:
        try:
            parser.parse(sql)
            return False
        except ValueError as e:
            error_msg = str(e).lower()
            if not (expected_error.lower() in error_msg or "invalid" in error_msg or "unsupported" in error_msg):
                return False
        except Exception:
            return False
    
    return True

def test_format_output_with_joins():
    """Test 6: Verify format_output handles joined column names."""
    import sys
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    from main import format_output
    
    test_rows = [
        {
            'transactions.id': 1,
            'transactions.user_id': 1,
            'transactions.amount': 100.50,
            'users.id': 1,
            'users.name': 'Sammy Sammy',
            'users.email': 'ss@example.com'
        },
        {
            'transactions.id': 2,
            'transactions.user_id': 1,
            'transactions.amount': 250.75,
            'users.id': 1,
            'users.name': 'Sammy Sammy',
            'users.email': 'ss@example.com'
        }
    ]
    
    try:
        output = format_output(test_rows)
        assert len(output) > 0
        
        for row in test_rows:
            for col in row.keys():
                assert col in output
        
        empty_output = format_output([])
        assert "No rows found" in empty_output
        
        return True
        
    except Exception:
        return False

def test_transactions():
    """Test 7: Verify transaction atomicity (BEGIN/COMMIT/ROLLBACK)."""
    storage = Storage()
    index_manager = IndexManager()
    engine = DatabaseEngine(storage, index_manager)
    
    try:
        # Start transaction
        engine.begin_transaction()
        
        # Insert a row in transaction
        engine.insert('users', {'id': 10, 'name': 'Transaction Test', 'email': 'txn@example.com'})
        
        # Verify it's visible in transaction
        rows = engine.select('users', where={'column': 'id', 'operator': '=', 'value': 10})
        assert len(rows) == 1
        
        # Commit transaction
        engine.commit_transaction()
        
        # Verify it's persisted after commit
        rows = engine.select('users', where={'column': 'id', 'operator': '=', 'value': 10})
        assert len(rows) == 1
        
        # Test rollback
        engine.begin_transaction()
        engine.insert('users', {'id': 11, 'name': 'Rollback Test', 'email': 'rollback@example.com'})
        rows = engine.select('users', where={'column': 'id', 'operator': '=', 'value': 11})
        assert len(rows) == 1
        
        engine.rollback_transaction()
        
        # Verify rollback worked - row should not exist
        rows = engine.select('users', where={'column': 'id', 'operator': '=', 'value': 11})
        assert len(rows) == 0
        
        # Test transaction with error (partial failure)
        engine.begin_transaction()
        engine.insert('users', {'id': 12, 'name': 'Partial Test', 'email': 'partial@example.com'})
        # Try to insert duplicate (should fail)
        try:
            engine.insert('users', {'id': 1, 'name': 'Duplicate', 'email': 'dup2@example.com'})
            engine.commit_transaction()
            return False  # Should have failed
        except ValueError:
            # Error occurred, rollback
            engine.rollback_transaction()
            # Verify first insert was also rolled back
            rows = engine.select('users', where={'column': 'id', 'operator': '=', 'value': 12})
            assert len(rows) == 0
        
        return True
        
    except Exception:
        return False

def main():
    """Run all tests."""
    cleanup_test_tables()
    
    results = [
        ("Table Creation", test_table_creation()),
        ("Insert and Constraints", test_insert_and_constraints()),
        ("INNER JOIN", test_inner_join()),
        ("Soft Delete", test_soft_delete()),
        ("Error Handling", test_error_handling()),
        ("Format Output", test_format_output_with_joins()),
        ("Transactions", test_transactions())
    ]
    
    passed = sum(1 for _, result in results if result)
    total = len(results)
    
    for test_name, result in results:
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"{status}: {test_name}")
    
    print(f"\n{passed}/{total} tests passed")
    
    return 0 if passed == total else 1

if __name__ == "__main__":
    exit(main())
