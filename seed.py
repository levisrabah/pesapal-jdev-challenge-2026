"""
Quick Demo Script

This script demonstrates the RDBMS capabilities with a fintech example.
Run it to see the system in action without using the REPL.
"""
from core.storage import Storage
from core.indexing import IndexManager
from core.parser import SQLParser
from core.engine import DatabaseEngine

def main():
    print("🚀 Pesapal RDBMS Demo - Fintech Transaction Ledger\n")    
    # Initialize components
    storage = Storage()
    index_manager = IndexManager()
    parser = SQLParser()
    engine = DatabaseEngine(storage, index_manager)

    # Create Users table
    print("1. Creating 'users' table...")
    create_users = parser.parse(
        "CREATE TABLE users (id INT PRIMARY KEY, name TEXT, email TEXT UNIQUE)"
    )
    engine.create_table(
        create_users['table_name'],
        create_users['schema'],
        create_users.get('primary_key'),
        create_users.get('unique_keys', [])
    )
    print("   ✅ Users table created\n")
    
    # Create Transactions table
    print("2. Creating 'transactions' table...")
    create_transactions = parser.parse(
        "CREATE TABLE transactions (id INT PRIMARY KEY, user_id INT, amount FLOAT, description TEXT)"
    )
    engine.create_table(
        create_transactions['table_name'],
        create_transactions['schema'],
        create_transactions.get('primary_key'),
        create_transactions.get('unique_keys', [])
    )
    print("   ✅ Transactions table created\n")
    
    # Insert users
    print("3. Inserting users...")
    insert_user1 = parser.parse("INSERT INTO users VALUES (1, 'Alice Johnson', 'alice@example.com')")
    engine.insert(insert_user1['table_name'], dict(zip(['id', 'name', 'email'], insert_user1['values'])))
    
    insert_user2 = parser.parse("INSERT INTO users VALUES (2, 'Ochibo Mdogo', 'bob@example.com')")
    engine.insert(insert_user2['table_name'], dict(zip(['id', 'name', 'email'], insert_user2['values'])))
    print("   ✅ 2 users inserted\n")
    
    # Insert transactions
    print("4. Inserting transactions...")
    transactions = [
        (1, 1, 1500.50, 'Payment received'),
        (2, 1, -250.00, 'Payment sent'),
        (3, 2, 3000.00, 'Salary deposit'),
        (4, 2, -100.00, 'Service fee'),
    ]
    
    for txn_id, user_id, amount, desc in transactions:
        insert_txn = parser.parse(f"INSERT INTO transactions VALUES ({txn_id}, {user_id}, {amount}, '{desc}')")
        engine.insert(insert_txn['table_name'], dict(zip(['id', 'user_id', 'amount', 'description'], insert_txn['values'])))
    print("   ✅ 4 transactions inserted\n")
    
    # Query all users
    print("5. Querying all users:")
    select_users = parser.parse("SELECT * FROM users")
    users = engine.select(select_users['table_name'], select_users.get('columns'))
    for user in users:
        print(f"   - {user}")
    print()
    
    # Query transactions for user 1
    print("6. Querying transactions for user_id = 1:")
    select_txns = parser.parse("SELECT * FROM transactions WHERE user_id = 1")
    txns = engine.select(select_txns['table_name'], select_txns.get('columns'), select_txns.get('where'))
    for txn in txns:
        print(f"   - {txn}")
    print()
    
    # Perform INNER JOIN
    print("7. Performing INNER JOIN (transactions + users):")
    join_query = parser.parse(
        "SELECT * FROM transactions INNER JOIN users ON transactions.user_id = users.id"
    )
    joined = engine.inner_join(
        join_query['table1'],
        join_query['table2'],
        join_query['join_col1'],
        join_query['join_col2'],
        join_query.get('columns')
    )
    for row in joined[:2]:  # Show first 2
        print(f"   - {row}")
    print(f"   ... (showing 2 of {len(joined)} joined rows)\n")
    
    # Soft delete a transaction
    print("8. Soft-deleting transaction id=2:")
    delete_txn = parser.parse("DELETE FROM transactions WHERE id = 2")
    deleted_count = engine.delete(delete_txn['table_name'], delete_txn.get('where'))
    print(f"   ✅ Soft-deleted {deleted_count} transaction(s)\n")
    
    # Verify soft delete (transaction shouldn't appear in SELECT)
    print("9. Verifying soft delete (transaction id=2 should not appear):")
    select_all = parser.parse("SELECT * FROM transactions")
    all_txns = engine.select(select_all['table_name'], select_all.get('columns'))
    print(f"   Active transactions: {len(all_txns)}")
    for txn in all_txns:
        print(f"   - {txn}")
    print()
    
    print("✅ Seed completed successfully!")
    print("\n💡 Note: The deleted transaction still exists in storage with is_deleted=True")
    print("   This maintains the immutable ledger for fintech compliance.")


if __name__ == "__main__":
    main()

