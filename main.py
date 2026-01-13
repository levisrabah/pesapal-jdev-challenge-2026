#!/usr/bin/env python3
"""
Main REPL - Interactive Command Line Interface

This is the main entry point for the custom RDBMS.
Provides an interactive SQL-like REPL for database operations.
"""

import sys
from core.storage import Storage
from core.indexing import IndexManager
from core.parser import SQLParser
from core.engine import DatabaseEngine


def print_welcome():
    """Display ASCII art welcome message."""
    welcome_msg = """
    ╔══════════════════════════════════════════════════════════════╗
    ║                                                              ║
    ║     🏦  PESAPAL CUSTOM RDBMS - IMMUTABLE TRANSACTION LEDGER  ║
    ║                                                              ║
    ║     Welcome to the Fintech Database System!                 ║
    ║     Built from scratch with Python (No SQL libraries)       ║
    ║                                                              ║
    ╚══════════════════════════════════════════════════════════════╝
    
    Commands:
      CREATE TABLE <name> (<columns>)     - Create a new table
      INSERT INTO <table> VALUES (...)    - Insert a row
      SELECT * FROM <table> [WHERE ...]   - Query rows
      UPDATE <table> SET ... [WHERE ...]  - Update rows
      DELETE FROM <table> [WHERE ...]     - Soft delete rows
      SELECT ... FROM <t1> INNER JOIN <t2> ON ... - Join tables
      EXIT or QUIT                        - Exit the REPL
      HELP                                - Show this help
    
    Example:
      CREATE TABLE users (id INT PRIMARY KEY, name TEXT, email TEXT UNIQUE)
      INSERT INTO users VALUES (1, 'John Doe', 'john@example.com')
      SELECT * FROM users
    
    """
    print(welcome_msg)


def print_help():
    """Display help information."""
    help_text = """
    📖 SQL COMMAND REFERENCE:
    
    1. CREATE TABLE
       Syntax: CREATE TABLE <name> (<col1> <type> [PRIMARY KEY|UNIQUE], ...)
       Example: CREATE TABLE users (id INT PRIMARY KEY, name TEXT, email TEXT UNIQUE)
    
    2. INSERT
       Syntax: INSERT INTO <table> [(columns)] VALUES (value1, value2, ...)
       Example: INSERT INTO users VALUES (1, 'John', 'john@example.com')
    
    3. SELECT
       Syntax: SELECT [*|col1, col2] FROM <table> [WHERE col = value]
       Example: SELECT * FROM users WHERE id = 1
    
    4. UPDATE
       Syntax: UPDATE <table> SET col1 = val1, col2 = val2 [WHERE condition]
       Example: UPDATE users SET name = 'Jane' WHERE id = 1
    
    5. DELETE (Soft Delete)
       Syntax: DELETE FROM <table> [WHERE condition]
       Example: DELETE FROM transactions WHERE id = 5
       Note: This sets is_deleted=True (fintech requirement)
    
    6. INNER JOIN
       Syntax: SELECT * FROM <table1> INNER JOIN <table2> ON <t1.col> = <t2.col>
       Example: SELECT * FROM transactions INNER JOIN users ON transactions.user_id = users.id
    
    💡 TIPS:
      - Use single quotes for TEXT values: 'John Doe'
      - Numbers can be INT or FLOAT
      - Primary keys must be unique
      - Soft deletes preserve data (is_deleted flag)
      - Indexes are automatically created for user_id columns
    
    """
    print(help_text)


def format_output(rows: list, limit: int = 100) -> str:
    """
    Format query results as a table for display.
    
    Args:
        rows: List of row dictionaries
        limit: Maximum number of rows to display
        
    Returns:
        Formatted string representation
    """
    if not rows:
        return "No rows found.\n"
    
    if len(rows) > limit:
        rows = rows[:limit]
        print(f"\n  Showing first {limit} rows (total: {len(rows)})\n")
    
    # Get all column names
    columns = set()
    for row in rows:
        columns.update(row.keys())
    columns = sorted(list(columns))
    
    # Calculate column widths
    col_widths = {}
    for col in columns:
        col_widths[col] = max(len(str(col)), 
                             max((len(str(row.get(col, ''))) for row in rows), default=0))
    
    # Build header
    header = " | ".join(str(col).ljust(col_widths[col]) for col in columns)
    separator = "-" * len(header)
    
    # Build rows
    output_lines = [header, separator]
    for row in rows:
        row_str = " | ".join(str(row.get(col, 'NULL')).ljust(col_widths[col]) 
                           for col in columns)
        output_lines.append(row_str)
    
    return "\n".join(output_lines) + "\n"


def main():
    """Main REPL loop."""
    print_welcome()
    
    # Initialize components
    storage = Storage()
    index_manager = IndexManager()
    parser = SQLParser()
    engine = DatabaseEngine(storage, index_manager)
    
    print("✅ Database initialized. Type 'HELP' for commands or 'EXIT' to quit.\n")
    
    while True:
        try:
            # Get user input
            user_input = input("SQL> ").strip()
            
            if not user_input:
                continue
            
            # Handle special commands
            if user_input.upper() in ['EXIT', 'QUIT']:
                print("\n👋 Thank you for using Pesapal RDBMS! Goodbye!\n")
                break
            
            if user_input.upper() == 'HELP':
                print_help()
                continue
            
            # Parse and execute SQL
            try:
                parsed = parser.parse(user_input)
                command = parsed['command']
                
                if command == 'CREATE_TABLE':
                    engine.create_table(
                        parsed['table_name'],
                        parsed['schema'],
                        parsed.get('primary_key'),
                        parsed.get('unique_keys', [])
                    )
                    print(f"✅ Table '{parsed['table_name']}' created successfully.\n")
                
                elif command == 'INSERT':
                    # Map columns to values
                    columns = parsed.get('columns')
                    values = parsed['values']
                    
                    if columns:
                        row = dict(zip(columns, values))
                    else:
                        # Need to get schema to map values
                        table_data = storage.load_table(parsed['table_name'])
                        schema_cols = list(table_data['schema'].keys())
                        # Filter out is_deleted from schema columns for insertion
                        schema_cols = [col for col in schema_cols if col != 'is_deleted']
                        row = dict(zip(schema_cols, values))
                    
                    engine.insert(parsed['table_name'], row)
                    print(f"✅ Row inserted into '{parsed['table_name']}'.\n")
                
                elif command == 'SELECT':
                    result = engine.select(
                        parsed['table_name'],
                        parsed.get('columns'),
                        parsed.get('where')
                    )
                    print(format_output(result))
                
                elif command == 'UPDATE':
                    count = engine.update(
                        parsed['table_name'],
                        parsed['updates'],
                        parsed.get('where')
                    )
                    print(f"✅ Updated {count} row(s) in '{parsed['table_name']}'.\n")
                
                elif command == 'DELETE':
                    count = engine.delete(
                        parsed['table_name'],
                        parsed.get('where')
                    )
                    print(f"✅ Soft-deleted {count} row(s) from '{parsed['table_name']}'.\n")
                
                elif command == 'JOIN':
                    result = engine.inner_join(
                        parsed['table1'],
                        parsed['table2'],
                        parsed['join_col1'],
                        parsed['join_col2'],
                        parsed.get('columns'),
                        parsed.get('where')
                    )
                    print(format_output(result))
                
                else:
                    print(f"❌ Unsupported command: {command}\n")
            
            except ValueError as e:
                print(f"❌ Error: {str(e)}\n")
            except FileNotFoundError as e:
                print(f"❌ Error: {str(e)}\n")
            except Exception as e:
                print(f"❌ Unexpected error: {str(e)}\n")
                import traceback
                traceback.print_exc()
        
        except KeyboardInterrupt:
            print("\n\n👋 Interrupted. Type 'EXIT' to quit.\n")
        except EOFError:
            print("\n\n👋 Thank you for using Pesapal RDBMS! Goodbye!\n")
            break


if __name__ == "__main__":
    main()

