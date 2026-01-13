"""
Parser Module - Regex-Based SQL Parsing

This module parses SQL-like commands using regular expressions.
Supports CREATE TABLE, INSERT, SELECT, UPDATE, DELETE, and JOIN operations.
"""

import re
from typing import Dict, List, Tuple, Optional, Any


class SQLParser:
    """
    Parses SQL-like commands using regex patterns.
    Converts SQL strings into structured command dictionaries.
    """
    
    # Regex patterns for different SQL operations
    CREATE_TABLE_PATTERN = re.compile(
        r'CREATE\s+TABLE\s+(\w+)\s*\((.*?)\)',
        re.IGNORECASE | re.DOTALL
    )
    
    INSERT_PATTERN = re.compile(
        r'INSERT\s+INTO\s+(\w+)\s*(?:\(([^)]+)\))?\s*VALUES\s*\(([^)]+)\)',
        re.IGNORECASE
    )
    
    SELECT_PATTERN = re.compile(
        r'SELECT\s+(.+?)\s+FROM\s+(\w+)(?:\s+WHERE\s+(.+?))?(?:\s+ORDER\s+BY\s+(.+?))?$',
        re.IGNORECASE
    )
    
    UPDATE_PATTERN = re.compile(
        r'UPDATE\s+(\w+)\s+SET\s+(.+?)(?:\s+WHERE\s+(.+?))?$',
        re.IGNORECASE
    )
    
    DELETE_PATTERN = re.compile(
        r'DELETE\s+FROM\s+(\w+)(?:\s+WHERE\s+(.+?))?$',
        re.IGNORECASE
    )
    
    JOIN_PATTERN = re.compile(
        r'SELECT\s+(.+?)\s+FROM\s+(\w+)\s+INNER\s+JOIN\s+(\w+)\s+ON\s+(\w+\.\w+)\s*=\s*(\w+\.\w+)(?:\s+WHERE\s+(.+?))?$',
        re.IGNORECASE
    )
    
    def parse(self, sql: str) -> Dict[str, Any]:
        """
        Parse a SQL command and return a structured command dictionary.
        
        Args:
            sql: SQL command string
            
        Returns:
            Dictionary with 'command' type and relevant parameters
            
        Raises:
            ValueError: If SQL syntax is invalid
        """
        sql = sql.strip()
        
        # Try to match different command types
        if sql.upper().startswith('CREATE TABLE'):
            return self._parse_create_table(sql)
        elif sql.upper().startswith('INSERT'):
            return self._parse_insert(sql)
        elif sql.upper().startswith('SELECT'):
            # Check if it's a JOIN first
            if 'INNER JOIN' in sql.upper():
                return self._parse_join(sql)
            else:
                return self._parse_select(sql)
        elif sql.upper().startswith('UPDATE'):
            return self._parse_update(sql)
        elif sql.upper().startswith('DELETE'):
            return self._parse_delete(sql)
        else:
            raise ValueError(f"Unsupported SQL command: {sql}")
    
    def _parse_create_table(self, sql: str) -> Dict[str, Any]:
        """
        Parse CREATE TABLE statement.
        Example: CREATE TABLE users (id INT PRIMARY KEY, name TEXT, email TEXT UNIQUE)
        
        Args:
            sql: CREATE TABLE SQL string
            
        Returns:
            Dictionary with table_name and schema
        """
        match = self.CREATE_TABLE_PATTERN.search(sql)
        if not match:
            raise ValueError("Invalid CREATE TABLE syntax")
        
        table_name = match.group(1)
        columns_str = match.group(2)
        
        schema = {}
        primary_key = None
        unique_keys = []
        
        # Parse column definitions
        # Split by comma, but be careful with nested parentheses
        columns = self._split_columns(columns_str)
        
        for col_def in columns:
            col_def = col_def.strip()
            if not col_def:
                continue
            
            # Extract column name, type, and constraints
            parts = col_def.split()
            col_name = parts[0]
            col_type = None
            is_primary = False
            is_unique = False
            
            for i, part in enumerate(parts[1:], 1):
                part_upper = part.upper()
                if part_upper in ['INT', 'TEXT', 'FLOAT']:
                    col_type = part_upper
                elif part_upper == 'PRIMARY' and i < len(parts) - 1 and parts[i+1].upper() == 'KEY':
                    is_primary = True
                    primary_key = col_name
                elif part_upper == 'UNIQUE':
                    is_unique = True
                    unique_keys.append(col_name)
            
            if col_type:
                schema[col_name] = {
                    'type': col_type,
                    'primary_key': is_primary,
                    'unique': is_unique
                }
        
        return {
            'command': 'CREATE_TABLE',
            'table_name': table_name,
            'schema': schema,
            'primary_key': primary_key,
            'unique_keys': unique_keys
        }
    
    def _split_columns(self, columns_str: str) -> List[str]:
        """
        Split column definitions by comma, handling nested structures.
        
        Args:
            columns_str: String containing column definitions
            
        Returns:
            List of column definition strings
        """
        columns = []
        current = []
        depth = 0
        
        for char in columns_str:
            if char == '(':
                depth += 1
            elif char == ')':
                depth -= 1
            elif char == ',' and depth == 0:
                columns.append(''.join(current))
                current = []
                continue
            current.append(char)
        
        if current:
            columns.append(''.join(current))
        
        return columns
    
    def _parse_insert(self, sql: str) -> Dict[str, Any]:
        """
        Parse INSERT statement.
        Example: INSERT INTO users (id, name) VALUES (1, 'John')
        
        Args:
            sql: INSERT SQL string
            
        Returns:
            Dictionary with table_name, columns, and values
        """
        match = self.INSERT_PATTERN.search(sql)
        if not match:
            raise ValueError("Invalid INSERT syntax")
        
        table_name = match.group(1)
        columns_str = match.group(2)
        values_str = match.group(3)
        
        # Parse columns (optional)
        columns = None
        if columns_str:
            columns = [col.strip() for col in columns_str.split(',')]
        
        # Parse values
        values = self._parse_values(values_str)
        
        return {
            'command': 'INSERT',
            'table_name': table_name,
            'columns': columns,
            'values': values
        }
    
    def _parse_values(self, values_str: str) -> List[Any]:
        """
        Parse VALUES clause, handling strings, numbers, and NULL.
        
        Args:
            values_str: String containing comma-separated values
            
        Returns:
            List of parsed values with appropriate types
        """
        values = []
        current = []
        in_string = False
        quote_char = None
        
        for char in values_str:
            if char in ("'", '"') and not in_string:
                in_string = True
                quote_char = char
                current.append(char)
            elif char == quote_char and in_string:
                in_string = False
                quote_char = None
                current.append(char)
            elif char == ',' and not in_string:
                val_str = ''.join(current).strip()
                values.append(self._parse_value(val_str))
                current = []
            else:
                current.append(char)
        
        if current:
            val_str = ''.join(current).strip()
            values.append(self._parse_value(val_str))
        
        return values
    
    def _parse_value(self, value_str: str) -> Any:
        """
        Parse a single value string into appropriate Python type.
        
        Args:
            value_str: String representation of a value
            
        Returns:
            Parsed value (int, float, str, or None)
        """
        value_str = value_str.strip()
        
        # Handle NULL
        if value_str.upper() == 'NULL':
            return None
        
        # Handle strings (quoted)
        if (value_str.startswith("'") and value_str.endswith("'")) or \
           (value_str.startswith('"') and value_str.endswith('"')):
            return value_str[1:-1]
        
        # Handle numbers
        try:
            if '.' in value_str:
                return float(value_str)
            return int(value_str)
        except ValueError:
            # Return as string if not a number
            return value_str
    
    def _parse_select(self, sql: str) -> Dict[str, Any]:
        """
        Parse SELECT statement.
        Example: SELECT * FROM users WHERE id = 1
        
        Args:
            sql: SELECT SQL string
            
        Returns:
            Dictionary with columns, table_name, and where clause
        """
        match = self.SELECT_PATTERN.search(sql)
        if not match:
            raise ValueError("Invalid SELECT syntax")
        
        columns_str = match.group(1).strip()
        table_name = match.group(2)
        where_clause = match.group(3) if match.group(3) else None
        order_by = match.group(4) if match.group(4) else None
        
        # Parse columns
        if columns_str == '*':
            columns = None  # None means select all
        else:
            columns = [col.strip() for col in columns_str.split(',')]
        
        return {
            'command': 'SELECT',
            'columns': columns,
            'table_name': table_name,
            'where': self._parse_where(where_clause) if where_clause else None,
            'order_by': order_by.strip() if order_by else None
        }
    
    def _parse_join(self, sql: str) -> Dict[str, Any]:
        """
        Parse INNER JOIN statement.
        Example: SELECT * FROM transactions INNER JOIN users ON transactions.user_id = users.id
        
        Args:
            sql: JOIN SQL string
            
        Returns:
            Dictionary with join parameters
        """
        match = self.JOIN_PATTERN.search(sql)
        if not match:
            raise ValueError("Invalid INNER JOIN syntax")
        
        columns_str = match.group(1).strip()
        table1 = match.group(2)
        table2 = match.group(3)
        join_col1 = match.group(4)  # e.g., "transactions.user_id"
        join_col2 = match.group(5)   # e.g., "users.id"
        where_clause = match.group(6) if match.group(6) else None
        
        # Parse columns
        if columns_str == '*':
            columns = None
        else:
            columns = [col.strip() for col in columns_str.split(',')]
        
        # Parse join columns (remove table prefix)
        col1_parts = join_col1.split('.')
        col2_parts = join_col2.split('.')
        
        return {
            'command': 'JOIN',
            'columns': columns,
            'table1': table1,
            'table2': table2,
            'join_col1': col1_parts[1] if len(col1_parts) > 1 else col1_parts[0],
            'join_col2': col2_parts[1] if len(col2_parts) > 1 else col2_parts[0],
            'where': self._parse_where(where_clause) if where_clause else None
        }
    
    def _parse_update(self, sql: str) -> Dict[str, Any]:
        """
        Parse UPDATE statement.
        Example: UPDATE users SET name = 'Jane' WHERE id = 1
        
        Args:
            sql: UPDATE SQL string
            
        Returns:
            Dictionary with table_name, set clause, and where clause
        """
        match = self.UPDATE_PATTERN.search(sql)
        if not match:
            raise ValueError("Invalid UPDATE syntax")
        
        table_name = match.group(1)
        set_clause = match.group(2)
        where_clause = match.group(3) if match.group(3) else None
        
        # Parse SET clause (column = value pairs)
        updates = {}
        for pair in set_clause.split(','):
            if '=' in pair:
                col, val = pair.split('=', 1)
                col = col.strip()
                val = self._parse_value(val.strip())
                updates[col] = val
        
        return {
            'command': 'UPDATE',
            'table_name': table_name,
            'updates': updates,
            'where': self._parse_where(where_clause) if where_clause else None
        }
    
    def _parse_delete(self, sql: str) -> Dict[str, Any]:
        """
        Parse DELETE statement.
        Example: DELETE FROM transactions WHERE id = 1
        
        Args:
            sql: DELETE SQL string
            
        Returns:
            Dictionary with table_name and where clause
        """
        match = self.DELETE_PATTERN.search(sql)
        if not match:
            raise ValueError("Invalid DELETE syntax")
        
        table_name = match.group(1)
        where_clause = match.group(2) if match.group(2) else None
        
        return {
            'command': 'DELETE',
            'table_name': table_name,
            'where': self._parse_where(where_clause) if where_clause else None
        }
    
    def _parse_where(self, where_clause: str) -> Dict[str, Any]:
        """
        Parse WHERE clause into a condition dictionary.
        Supports: column = value, column != value, column > value, etc.
        
        Args:
            where_clause: WHERE clause string
            
        Returns:
            Dictionary with column, operator, and value
        """
        where_clause = where_clause.strip()
        
        # Match operators: =, !=, <, >, <=, >=
        operators = ['!=', '<=', '>=', '=', '<', '>']
        
        for op in operators:
            if op in where_clause:
                parts = where_clause.split(op, 1)
                if len(parts) == 2:
                    column = parts[0].strip()
                    value = self._parse_value(parts[1].strip())
                    return {
                        'column': column,
                        'operator': op,
                        'value': value
                    }
        
        raise ValueError(f"Unsupported WHERE clause operator: {where_clause}")

