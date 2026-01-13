"""
Storage Module - JSON Persistence Layer

This module handles all file I/O operations for table data persistence.
It uses JSON files stored in the /data directory, with one file per table.
"""

import json
import os
from datetime import datetime
from typing import Dict, List, Any, Optional


class Storage:
    """
    Handles persistent storage of table data using JSON files.
    Each table is stored as a separate JSON file in the /data directory.
    """
    
    def __init__(self, data_dir: Optional[str] = None):
        """
        Initialize the storage system with an absolute path to the project root.
        """
        if data_dir is None:
            # 1. Get the directory where storage.py lives (core/)
            current_dir = os.path.dirname(os.path.abspath(__file__))
            # 2. Go up one level to the project root
            project_root = os.path.dirname(current_dir)
            # 3. Target the 'data' folder at the root
            self.data_dir = os.path.join(project_root, "data")
        else:
            self.data_dir = data_dir
            
        self._ensure_data_directory()
    
    def _ensure_data_directory(self) -> None:
        """Create the data directory if it doesn't exist."""
        if not os.path.exists(self.data_dir):
            os.makedirs(self.data_dir)
    
    def _get_table_path(self, table_name: str) -> str:
        """
        Get the file path for a table's JSON file.
        
        Args:
            table_name: Name of the table
            
        Returns:
            Full path to the table's JSON file
        """
        return os.path.join(self.data_dir, f"{table_name}.json")
    
    def table_exists(self, table_name: str) -> bool:
        """
        Check if a table file exists.
        
        Args:
            table_name: Name of the table to check
            
        Returns:
            True if table exists, False otherwise
        """
        table_path = self._get_table_path(table_name)
        return os.path.exists(table_path)
    
    def create_table(self, table_name: str, schema: Dict[str, str], 
                    primary_key: Optional[str] = None,
                    unique_keys: Optional[List[str]] = None) -> None:
        """
        Create a new table with the given schema.
        Initializes an empty table structure with metadata.
        
        Args:
            table_name: Name of the table to create
            schema: Dictionary mapping column names to data types
            primary_key: Name of the primary key column (optional)
            unique_keys: List of unique key column names (optional)
        """
        table_path = self._get_table_path(table_name)
        
        table_data = {
            "schema": schema,
            "rows": [],
            "metadata": {
                "row_count": 0,
                "primary_key": primary_key,
                "unique_keys": unique_keys or [],
                "created_at": datetime.now().isoformat()  # Set timestamp when table is created
            }
        }
        
        with open(table_path, 'w') as f:
            json.dump(table_data, f, indent=2)
    
    def load_table(self, table_name: str) -> Dict[str, Any]:
        """
        Load a table's data from its JSON file.
        
        Args:
            table_name: Name of the table to load
            
        Returns:
            Dictionary containing schema, rows, and metadata
            
        Raises:
            FileNotFoundError: If the table doesn't exist
        """
        table_path = self._get_table_path(table_name)
        
        if not os.path.exists(table_path):
            raise FileNotFoundError(f"Table '{table_name}' does not exist")
        
        with open(table_path, 'r') as f:
            return json.load(f)
    
    def save_table(self, table_name: str, table_data: Dict[str, Any]) -> None:
        """
        Save table data to its JSON file.
        Updates metadata like row_count automatically.
        
        Args:
            table_name: Name of the table to save
            table_data: Dictionary containing schema, rows, and metadata
        """
        table_path = self._get_table_path(table_name)
        
        # Update metadata
        table_data["metadata"]["row_count"] = len(table_data["rows"])
        
        with open(table_path, 'w') as f:
            json.dump(table_data, f, indent=2)
    
    def get_all_rows(self, table_name: str, include_deleted: bool = False) -> List[Dict[str, Any]]:
        """
        Get all rows from a table, optionally filtering out soft-deleted rows.
        
        Args:
            table_name: Name of the table
            include_deleted: If True, include rows where is_deleted=True
            
        Returns:
            List of row dictionaries
        """
        table_data = self.load_table(table_name)
        rows = table_data["rows"]
        
        if include_deleted:
            return rows
        
        # Filter out soft-deleted rows (fintech requirement)
        return [row for row in rows if not row.get("is_deleted", False)]
    
    def insert_row(self, table_name: str, row: Dict[str, Any]) -> None:
        """
        Insert a new row into a table.
        
        Args:
            table_name: Name of the table
            row: Dictionary representing the row data
        """
        table_data = self.load_table(table_name)
        table_data["rows"].append(row)
        self.save_table(table_name, table_data)
    
    def update_rows(self, table_name: str, updates: List[Dict[str, Any]]) -> None:
        """
        Update multiple rows in a table.
        This replaces the entire rows array, so it's used for updates/deletes.
        
        Args:
            table_name: Name of the table
            updates: List of updated row dictionaries
        """
        table_data = self.load_table(table_name)
        table_data["rows"] = updates
        self.save_table(table_name, table_data)
    
    def get_table_schema(self, table_name: str) -> Dict[str, str]:
        """
        Get the schema (column definitions) for a table.
        
        Args:
            table_name: Name of the table
            
        Returns:
            Dictionary mapping column names to data types
        """
        table_data = self.load_table(table_name)
        return table_data["schema"]
    
    def drop_table(self, table_name: str) -> None:
        """
        Delete a table's JSON file (permanent deletion).
        
        Args:
            table_name: Name of the table to drop
        """
        table_path = self._get_table_path(table_name)
        if os.path.exists(table_path):
            os.remove(table_path)

