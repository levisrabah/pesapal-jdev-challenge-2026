"""
Helper script to add description and timestamp fields to transactions table.
Run this once to update the transactions table schema for the dashboard.
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.storage import Storage

def update_transactions_schema():
    """Add description and timestamp fields to transactions table if they don't exist."""
    storage = Storage()
    
    if not storage.table_exists('transactions'):
        print("❌ Transactions table does not exist. Please create it first.")
        return False
    
    table_data = storage.load_table('transactions')
    schema = table_data["schema"]
    
    updated = False
    
    # Add description field if it doesn't exist
    if 'description' not in schema:
        schema['description'] = 'TEXT'
        updated = True
        print("✅ Added 'description' field to transactions table")
    
    # Add timestamp field if it doesn't exist
    if 'timestamp' not in schema:
        schema['timestamp'] = 'TEXT'
        updated = True
        print("✅ Added 'timestamp' field to transactions table")
    
    if updated:
        # Save the updated schema
        table_data["schema"] = schema
        storage.save_table('transactions', table_data)
        print("✅ Transactions table schema updated successfully!")
        return True
    else:
        print("ℹ️  Transactions table already has description and timestamp fields")
        return False

if __name__ == "__main__":
    update_transactions_schema()
