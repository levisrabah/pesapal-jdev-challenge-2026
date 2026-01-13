"""
Indexing Module - In-Memory Hash Map Index

This module implements a hash map-based index for fast lookups on user_id.
The index maps user_id values to lists of row indices for O(1) lookup performance.
"""

from typing import Dict, List, Set, Any, Optional


class Index:
    """
    In-memory hash map index for fast lookups.
    Maintains a mapping from indexed column values to row positions.
    """
    
    def __init__(self, column_name: str):
        """
        Initialize an index for a specific column.
        
        Args:
            column_name: Name of the column to index
        """
        self.column_name = column_name
        # Hash map: value -> set of row indices
        self._index: Dict[Any, Set[int]] = {}
        # Reverse mapping: row_index -> value (for updates/deletes)
        self._row_to_value: Dict[int, Any] = {}
    
    def build(self, rows: List[Dict[str, Any]]) -> None:
        """
        Build or rebuild the index from scratch using all rows.
        This is called when the table is loaded or after bulk operations.
        
        Args:
            rows: List of all row dictionaries
        """
        self._index.clear()
        self._row_to_value.clear()
        
        for idx, row in enumerate(rows):
            value = row.get(self.column_name)
            if value is not None:
                if value not in self._index:
                    self._index[value] = set()
                self._index[value].add(idx)
                self._row_to_value[idx] = value
    
    def add(self, row_index: int, value: Any) -> None:
        """
        Add a new entry to the index.
        
        Args:
            row_index: Index of the row in the rows array
            value: Value of the indexed column for this row
        """
        if value is not None:
            if value not in self._index:
                self._index[value] = set()
            self._index[value].add(row_index)
            self._row_to_value[row_index] = value
    
    def remove(self, row_index: int) -> None:
        """
        Remove an entry from the index.
        
        Args:
            row_index: Index of the row to remove
        """
        if row_index in self._row_to_value:
            value = self._row_to_value[row_index]
            if value in self._index:
                self._index[value].discard(row_index)
                # Clean up empty sets
                if not self._index[value]:
                    del self._index[value]
            del self._row_to_value[row_index]
    
    def update(self, row_index: int, old_value: Any, new_value: Any) -> None:
        """
        Update an index entry when a row's indexed column value changes.
        
        Args:
            row_index: Index of the row being updated
            old_value: Previous value of the indexed column
            new_value: New value of the indexed column
        """
        # Remove old value
        if old_value is not None and old_value in self._index:
            self._index[old_value].discard(row_index)
            if not self._index[old_value]:
                del self._index[old_value]
        
        # Add new value
        if new_value is not None:
            if new_value not in self._index:
                self._index[new_value] = set()
            self._index[new_value].add(row_index)
        
        self._row_to_value[row_index] = new_value
    
    def find(self, value: Any) -> Set[int]:
        """
        Find all row indices that match the given value.
        Returns O(1) lookup performance.
        
        Args:
            value: Value to search for
            
        Returns:
            Set of row indices matching the value (empty set if not found)
        """
        return self._index.get(value, set())
    
    def contains(self, value: Any) -> bool:
        """
        Check if a value exists in the index.
        
        Args:
            value: Value to check
            
        Returns:
            True if value exists, False otherwise
        """
        return value in self._index and len(self._index[value]) > 0
    
    def clear(self) -> None:
        """Clear all index entries."""
        self._index.clear()
        self._row_to_value.clear()
    
    def get_all_values(self) -> Set[Any]:
        """
        Get all unique values in the index.
        
        Returns:
            Set of all indexed values
        """
        return set(self._index.keys())


class IndexManager:
    """
    Manages multiple indexes for different columns.
    Currently focused on user_id indexing for transaction lookups.
    """
    
    def __init__(self):
        """Initialize the index manager with an empty index registry."""
        self._indexes: Dict[str, Index] = {}
    
    def create_index(self, table_name: str, column_name: str) -> None:
        """
        Create a new index for a column in a table.
        
        Args:
            table_name: Name of the table
            column_name: Name of the column to index
        """
        index_key = f"{table_name}.{column_name}"
        self._indexes[index_key] = Index(column_name)
    
    def get_index(self, table_name: str, column_name: str) -> Optional[Index]:
        """
        Get an index for a specific table and column.
        
        Args:
            table_name: Name of the table
            column_name: Name of the column
            
        Returns:
            Index object or None if not found
        """
        index_key = f"{table_name}.{column_name}"
        return self._indexes.get(index_key)
    
    def build_index(self, table_name: str, column_name: str, rows: List[Dict[str, Any]]) -> None:
        """
        Build or rebuild an index from table rows.
        
        Args:
            table_name: Name of the table
            column_name: Name of the column
            rows: List of all row dictionaries
        """
        index = self.get_index(table_name, column_name)
        if index:
            index.build(rows)
    
    def has_index(self, table_name: str, column_name: str) -> bool:
        """
        Check if an index exists for a column.
        
        Args:
            table_name: Name of the table
            column_name: Name of the column
            
        Returns:
            True if index exists, False otherwise
        """
        index_key = f"{table_name}.{column_name}"
        return index_key in self._indexes

