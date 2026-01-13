"""
Engine Module - CRUD Operations and Join Logic

This module implements the core database engine with CRUD operations,
constraint validation, and INNER JOIN functionality.
"""

from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime, date
from .storage import Storage
from .indexing import IndexManager


class DatabaseEngine:
    """
    Core database engine implementing CRUD operations and joins.
    Handles data validation, constraints, and soft deletes (fintech requirement).
    """
    
    def __init__(self, storage: Storage, index_manager: IndexManager):
        """
        Initialize the database engine.
        
        Args:
            storage: Storage instance for persistence
            index_manager: IndexManager instance for indexing
        """
        self.storage = storage
        self.index_manager = index_manager
        self._table_cache: Dict[str, Dict[str, Any]] = {}
        # Transaction state
        self._in_transaction: bool = False
        self._transaction_changes: Dict[str, Dict[str, Any]] = {}  # table_name -> table_data
    
    def create_table(self, table_name: str, schema: Dict[str, str], 
                    primary_key: Optional[str] = None, 
                    unique_keys: Optional[List[str]] = None) -> None:
        """
        Create a new table with the specified schema.
        
        Args:
            table_name: Name of the table
            schema: Dictionary mapping column names to data type definitions
            primary_key: Name of the primary key column
            unique_keys: List of unique key column names
            
        Raises:
            ValueError: If table already exists or schema is invalid
        """
        if self.storage.table_exists(table_name):
            raise ValueError(f"Table '{table_name}' already exists")
        
        # Convert schema format for storage
        storage_schema = {}
        for col_name, col_def in schema.items():
            if isinstance(col_def, dict):
                storage_schema[col_name] = col_def['type']
            else:
                storage_schema[col_name] = col_def
        
        self.storage.create_table(table_name, storage_schema, primary_key, unique_keys)
        
        # Create index for user_id if it exists (for transaction lookups)
        # Note: Generic indexes can be created with CREATE INDEX command
        if 'user_id' in storage_schema:
            self.index_manager.create_index(table_name, 'user_id')
        
        # Load table into cache
        self._load_table_cache(table_name)
    
    def insert(self, table_name: str, row: Dict[str, Any]) -> None:
        """
        Insert a new row into a table.
        Validates data types, primary key uniqueness, and unique constraints.
        
        Args:
            table_name: Name of the table
            row: Dictionary representing the row data
            
        Raises:
            ValueError: If validation fails or constraints are violated
        """
        table_data = self.storage.load_table(table_name)
        schema = table_data["schema"]
        
        # Validate and convert data types
        validated_row = self._validate_and_convert_row(row, schema, table_name)
        
        # Check primary key uniqueness
        primary_key = self._get_primary_key(table_name)
        if primary_key and primary_key in validated_row:
            if self._primary_key_exists(table_name, validated_row[primary_key]):
                raise ValueError(f"Primary key '{validated_row[primary_key]}' already exists")
        
        # Check unique constraints
        unique_keys = self._get_unique_keys(table_name)
        for unique_col in unique_keys:
            if unique_col in validated_row and validated_row[unique_col] is not None:
                if self._unique_key_exists(table_name, unique_col, validated_row[unique_col]):
                    raise ValueError(f"Unique constraint violation on '{unique_col}': '{validated_row[unique_col]}' already exists")
        
        # Add soft delete flag (default False)
        validated_row['is_deleted'] = False
        
        # Add created_at timestamp
        validated_row['created_at'] = datetime.now().isoformat()
        
        # Handle transaction: store in transaction_changes if in transaction
        if self._in_transaction:
            if table_name not in self._transaction_changes:
                self._transaction_changes[table_name] = self.storage.load_table(table_name)
            table_data = self._transaction_changes[table_name]
            table_data["rows"].append(validated_row)
            table_data["metadata"]["row_count"] = len(table_data["rows"])
        else:
            # Insert row directly to storage
            self.storage.insert_row(table_name, validated_row)
            
            # Update index
            table_data = self.storage.load_table(table_name)
            rows = table_data["rows"]
            row_index = len(rows) - 1
            
            # Update all indexes for this table
            for col_name in schema.keys():
                if self.index_manager.has_index(table_name, col_name) and col_name in validated_row:
                    index = self.index_manager.get_index(table_name, col_name)
                    if index:
                        index.add(row_index, validated_row[col_name])
            
            # Update cache
            self._load_table_cache(table_name)
    
    def select(self, table_name: str, columns: Optional[List[str]] = None,
              where: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """
        Select rows from a table with optional filtering.
        
        Args:
            table_name: Name of the table
            columns: List of column names to select (None = all columns)
            where: WHERE clause conditions dictionary
            
        Returns:
            List of matching row dictionaries
        """
        # Get rows from transaction or storage
        if self._in_transaction and table_name in self._transaction_changes:
            table_data = self._transaction_changes[table_name]
            rows = [row for row in table_data["rows"] if not row.get("is_deleted", False)]
        else:
            rows = self.storage.get_all_rows(table_name, include_deleted=False)
        
        # Apply WHERE clause filtering
        if where:
            rows = self._apply_where_clause(rows, where)
        
        # Select specific columns
        if columns:
            result = []
            for row in rows:
                filtered_row = {col: row.get(col) for col in columns}
                result.append(filtered_row)
            return result
        
        return rows
    
    def update(self, table_name: str, updates: Dict[str, Any],
              where: Optional[Dict[str, Any]] = None) -> int:
        """
        Update rows in a table.
        
        Args:
            table_name: Name of the table
            updates: Dictionary of column:value pairs to update
            where: WHERE clause conditions dictionary
            
        Returns:
            Number of rows updated
        """
        # Get table data (from transaction or storage)
        if self._in_transaction and table_name in self._transaction_changes:
            table_data = self._transaction_changes[table_name]
        else:
            table_data = self.storage.load_table(table_name)
            if self._in_transaction:
                # Create a copy for transaction
                import copy
                self._transaction_changes[table_name] = copy.deepcopy(table_data)
                table_data = self._transaction_changes[table_name]
        
        rows = table_data["rows"]
        schema = table_data["schema"]
        
        updated_count = 0
        
        for idx, row in enumerate(rows):
            # Skip soft-deleted rows
            if row.get('is_deleted', False):
                continue
            
            # Check if row matches WHERE clause
            if where and not self._row_matches_where(row, where):
                continue
            
            # Update row
            old_values = {}
            for col, val in updates.items():
                if col in schema:
                    old_values[col] = row.get(col)
                    # Validate and convert value
                    validated_val = self._convert_value(val, schema[col])
                    row[col] = validated_val
            
            # Update indexes for changed columns
            if not self._in_transaction:
                for col, old_val in old_values.items():
                    if self.index_manager.has_index(table_name, col) and old_val != row.get(col):
                        index = self.index_manager.get_index(table_name, col)
                        if index:
                            index.update(idx, old_val, row.get(col))
            
            updated_count += 1
        
        # Save updated rows (only if not in transaction)
        if not self._in_transaction:
            self.storage.update_rows(table_name, rows)
            self._load_table_cache(table_name)
        
        return updated_count
    
    def delete(self, table_name: str, where: Optional[Dict[str, Any]] = None) -> int:
        """
        Soft delete rows from a table (fintech requirement).
        Sets is_deleted flag to True instead of removing data.
        
        Args:
            table_name: Name of the table
            where: WHERE clause conditions dictionary
            
        Returns:
            Number of rows soft-deleted
        """
        # Get table data (from transaction or storage)
        if self._in_transaction and table_name in self._transaction_changes:
            table_data = self._transaction_changes[table_name]
        else:
            table_data = self.storage.load_table(table_name)
            if self._in_transaction:
                # Create a copy for transaction
                import copy
                self._transaction_changes[table_name] = copy.deepcopy(table_data)
                table_data = self._transaction_changes[table_name]
        
        rows = table_data["rows"]
        
        deleted_count = 0
        
        for idx, row in enumerate(rows):
            # Skip already deleted rows
            if row.get('is_deleted', False):
                continue
            
            # Check if row matches WHERE clause
            if where and not self._row_matches_where(row, where):
                continue
            
            # Soft delete
            row['is_deleted'] = True
            deleted_count += 1
            
            # Update index (remove from index) - only if not in transaction
            if not self._in_transaction:
                for col_name in table_data["schema"].keys():
                    if self.index_manager.has_index(table_name, col_name):
                        index = self.index_manager.get_index(table_name, col_name)
                        if index:
                            index.remove(idx)
        
        # Save updated rows (only if not in transaction)
        if not self._in_transaction:
            self.storage.update_rows(table_name, rows)
            self._load_table_cache(table_name)
        
        return deleted_count
    
    def inner_join(self, table1_name: str, table2_name: str,
                   join_col1: str, join_col2: str,
                   columns: Optional[List[str]] = None,
                   where: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """
        Perform an INNER JOIN between two tables.
        Uses index for fast lookups when joining on user_id.
        
        Args:
            table1_name: Name of the first table
            table2_name: Name of the second table
            join_col1: Column name in table1 to join on
            join_col2: Column name in table2 to join on
            columns: List of columns to select (None = all)
            where: WHERE clause conditions dictionary
            
        Returns:
            List of joined row dictionaries
        """
        # Load rows from both tables (excluding soft-deleted)
        # Check transaction data first
        if self._in_transaction and table1_name in self._transaction_changes:
            rows1 = [row for row in self._transaction_changes[table1_name]["rows"] 
                    if not row.get("is_deleted", False)]
        else:
            rows1 = self.storage.get_all_rows(table1_name, include_deleted=False)
        
        if self._in_transaction and table2_name in self._transaction_changes:
            rows2 = [row for row in self._transaction_changes[table2_name]["rows"] 
                    if not row.get("is_deleted", False)]
        else:
            rows2 = self.storage.get_all_rows(table2_name, include_deleted=False)
        
        # Use index if joining on indexed column
        use_index = (self.index_manager.has_index(table2_name, join_col2))
        
        joined_rows = []
        
        if use_index:
            # Optimized join using index
            # Note: Index stores indices into the full table rows array, so we need to load it
            if self._in_transaction and table2_name in self._transaction_changes:
                table2_data = self._transaction_changes[table2_name]
            else:
                table2_data = self.storage.load_table(table2_name)
            table2_all_rows = table2_data["rows"]  # Full rows array (for index lookup)
            
            index = self.index_manager.get_index(table2_name, join_col2)
            
            index = self.index_manager.get_index(table2_name, 'user_id')
            for row1 in rows1:
                join_value = row1.get(join_col1)
                if join_value is not None:
                    matching_indices = index.find(join_value)
                    for idx in matching_indices:
                        if idx < len(table2_all_rows):
                            row2 = table2_all_rows[idx]
                            # Skip soft-deleted rows
                            if not row2.get('is_deleted', False):
                                joined_row = self._merge_rows(row1, row2, table1_name, table2_name)
                                joined_rows.append(joined_row)
        else:
            # Standard nested loop join
            for row1 in rows1:
                for row2 in rows2:
                    if row1.get(join_col1) == row2.get(join_col2):
                        joined_row = self._merge_rows(row1, row2, table1_name, table2_name)
                        joined_rows.append(joined_row)
        
        # Apply WHERE clause if provided
        if where:
            joined_rows = self._apply_where_clause(joined_rows, where)
        
        # Select specific columns if provided
        if columns:
            result = []
            for row in joined_rows:
                filtered_row = {col: row.get(col) for col in columns if col in row}
                result.append(filtered_row)
            return result
        
        return joined_rows
    
    def _merge_rows(self, row1: Dict[str, Any], row2: Dict[str, Any],
                   table1_name: str, table2_name: str) -> Dict[str, Any]:
        """
        Merge two rows from different tables, prefixing column names.
        
        Args:
            row1: Row from first table
            row2: Row from second table
            table1_name: Name of first table
            table2_name: Name of second table
            
        Returns:
            Merged row dictionary with prefixed column names
        """
        merged = {}
        
        # Add columns from table1 with prefix
        for col, val in row1.items():
            if col != 'is_deleted':  # Don't include is_deleted in joined results
                merged[f"{table1_name}.{col}"] = val
        
        # Add columns from table2 with prefix
        for col, val in row2.items():
            if col != 'is_deleted':
                merged[f"{table2_name}.{col}"] = val
        
        return merged
    
    def _validate_and_convert_row(self, row: Dict[str, Any], 
                                  schema: Dict[str, str],
                                  table_name: str) -> Dict[str, Any]:
        """
        Validate row data types and convert values to appropriate types.
        
        Args:
            row: Row dictionary to validate
            schema: Table schema dictionary
            table_name: Name of the table (for error messages)
            
        Returns:
            Validated and converted row dictionary
            
        Raises:
            ValueError: If validation fails
        """
        validated = {}
        
        for col_name, col_type in schema.items():
            if col_name in row:
                validated[col_name] = self._convert_value(row[col_name], col_type)
            elif col_name == 'is_deleted':
                # is_deleted is auto-added, skip it
                continue
        
        return validated
    
    def _convert_value(self, value: Any, col_type: str) -> Any:
        """
        Convert a value to the appropriate type based on column type.
        
        Args:
            value: Value to convert
            col_type: Column type (INT, FLOAT, TEXT)
            
        Returns:
            Converted value
            
        Raises:
            ValueError: If conversion fails
        """
        if value is None:
            return None
        
        col_type = col_type.upper()
        
        if col_type == 'INT':
            try:
                return int(value)
            except (ValueError, TypeError):
                raise ValueError(f"Cannot convert '{value}' to INT")
        
        elif col_type == 'FLOAT':
            try:
                return float(value)
            except (ValueError, TypeError):
                raise ValueError(f"Cannot convert '{value}' to FLOAT")
        
        elif col_type == 'TEXT':
            return str(value)
        
        elif col_type == 'BOOLEAN':
            if isinstance(value, bool):
                return value
            if isinstance(value, str):
                value_lower = value.lower().strip()
                if value_lower in ('true', '1', 'yes', 'on'):
                    return True
                elif value_lower in ('false', '0', 'no', 'off'):
                    return False
            if isinstance(value, (int, float)):
                return bool(value)
            raise ValueError(f"Cannot convert '{value}' to BOOLEAN")
        
        elif col_type == 'DATE':
            if isinstance(value, date):
                return value.isoformat()
            if isinstance(value, datetime):
                return value.date().isoformat()
            if isinstance(value, str):
                # Try to parse ISO format (YYYY-MM-DD)
                try:
                    parsed_date = datetime.strptime(value, '%Y-%m-%d').date()
                    return parsed_date.isoformat()
                except ValueError:
                    raise ValueError(f"Cannot convert '{value}' to DATE (expected YYYY-MM-DD format)")
            raise ValueError(f"Cannot convert '{value}' to DATE")
        
        else:
            raise ValueError(f"Unsupported column type: {col_type}")
    
    def _apply_where_clause(self, rows: List[Dict[str, Any]], 
                           where: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Filter rows based on WHERE clause conditions.
        
        Args:
            rows: List of row dictionaries
            where: WHERE clause dictionary with column, operator, value
            
        Returns:
            Filtered list of rows
        """
        filtered = []
        column = where['column']
        operator = where['operator']
        value = where['value']
        
        for row in rows:
            # Handle prefixed column names (from joins)
            row_value = row.get(column)
            if row_value is None and '.' in column:
                # Try without prefix
                col_name = column.split('.')[-1]
                row_value = row.get(col_name)
            
            if self._evaluate_condition(row_value, operator, value):
                filtered.append(row)
        
        return filtered
    
    def _row_matches_where(self, row: Dict[str, Any], where: Dict[str, Any]) -> bool:
        """
        Check if a row matches WHERE clause conditions.
        
        Args:
            row: Row dictionary
            where: WHERE clause dictionary
            
        Returns:
            True if row matches, False otherwise
        """
        column = where['column']
        operator = where['operator']
        value = where['value']
        
        row_value = row.get(column)
        return self._evaluate_condition(row_value, operator, value)
    
    def _evaluate_condition(self, row_value: Any, operator: str, value: Any) -> bool:
        """
        Evaluate a comparison condition.
        
        Args:
            row_value: Value from the row
            operator: Comparison operator (=, !=, <, >, <=, >=)
            value: Value to compare against
            
        Returns:
            True if condition is satisfied, False otherwise
        """
        if operator == '=':
            return row_value == value
        elif operator == '!=':
            return row_value != value
        elif operator == '<':
            return row_value < value
        elif operator == '>':
            return row_value > value
        elif operator == '<=':
            return row_value <= value
        elif operator == '>=':
            return row_value >= value
        else:
            return False
    
    def _get_primary_key(self, table_name: str) -> Optional[str]:
        """Get the primary key column name for a table."""
        table_data = self._load_table_cache(table_name)
        metadata = table_data.get("metadata", {})
        return metadata.get("primary_key")
    
    def _get_unique_keys(self, table_name: str) -> List[str]:
        """Get list of unique key column names for a table."""
        table_data = self._load_table_cache(table_name)
        metadata = table_data.get("metadata", {})
        return metadata.get("unique_keys", [])
    
    def _primary_key_exists(self, table_name: str, value: Any) -> bool:
        """Check if a primary key value already exists."""
        primary_key = self._get_primary_key(table_name)
        if not primary_key:
            return False
        
        # Check transaction data first, then storage
        if self._in_transaction and table_name in self._transaction_changes:
            rows = [row for row in self._transaction_changes[table_name]["rows"] 
                   if not row.get("is_deleted", False)]
        else:
            rows = self.storage.get_all_rows(table_name, include_deleted=False)
        
        for row in rows:
            if row.get(primary_key) == value:
                return True
        return False
    
    def _unique_key_exists(self, table_name: str, column: str, value: Any) -> bool:
        """Check if a unique key value already exists."""
        # Check transaction data first, then storage
        if self._in_transaction and table_name in self._transaction_changes:
            rows = [row for row in self._transaction_changes[table_name]["rows"] 
                   if not row.get("is_deleted", False)]
        else:
            rows = self.storage.get_all_rows(table_name, include_deleted=False)
        
        for row in rows:
            if row.get(column) == value:
                return True
        return False
    
    def _load_table_cache(self, table_name: str) -> Dict[str, Any]:
        """Load table data into cache and rebuild indexes."""
        table_data = self.storage.load_table(table_name)
        self._table_cache[table_name] = table_data
        
        # Rebuild all indexes for this table
        rows = table_data["rows"]
        schema = table_data["schema"]
        for col_name in schema.keys():
            if self.index_manager.has_index(table_name, col_name):
                self.index_manager.build_index(table_name, col_name, rows)
        
        return table_data
    
    def begin_transaction(self) -> None:
        """Begin a transaction. All changes will be kept in memory until COMMIT."""
        if self._in_transaction:
            raise ValueError("Transaction already in progress. Use COMMIT or ROLLBACK first.")
        self._in_transaction = True
        self._transaction_changes = {}
    
    def commit_transaction(self) -> None:
        """Commit the current transaction. All changes are written to storage."""
        if not self._in_transaction:
            raise ValueError("No transaction in progress.")
        
        try:
            # Write all changes to storage
            for table_name, table_data in self._transaction_changes.items():
                # Update row count
                table_data["metadata"]["row_count"] = len(table_data["rows"])
                # Save to storage
                self.storage.save_table(table_name, table_data)
                # Rebuild indexes
                rows = table_data["rows"]
                schema = table_data["schema"]
                for col_name in schema.keys():
                    if self.index_manager.has_index(table_name, col_name):
                        self.index_manager.build_index(table_name, col_name, rows)
                # Update cache
                self._load_table_cache(table_name)
        finally:
            # Always clear transaction state
            self._in_transaction = False
            self._transaction_changes = {}
    
    def rollback_transaction(self) -> None:
        """Rollback the current transaction. All changes are discarded."""
        if not self._in_transaction:
            raise ValueError("No transaction in progress.")
        self._in_transaction = False
        self._transaction_changes = {}
    
    def create_index(self, table_name: str, column_name: str) -> None:
        """
        Create an index on a column for faster lookups.
        
        Args:
            table_name: Name of the table
            column_name: Name of the column to index
            
        Raises:
            ValueError: If table doesn't exist or column doesn't exist
        """
        if not self.storage.table_exists(table_name):
            raise ValueError(f"Table '{table_name}' does not exist")
        
        table_data = self.storage.load_table(table_name)
        if column_name not in table_data["schema"]:
            raise ValueError(f"Column '{column_name}' does not exist in table '{table_name}'")
        
        # Create the index
        self.index_manager.create_index(table_name, column_name)
        
        # Build the index with existing data
        rows = table_data["rows"]
        self.index_manager.build_index(table_name, column_name, rows)

