from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from typing import List, Dict, Any, Optional
from datetime import datetime
import sys
import os

# Add parent directory to path to import core modules
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.storage import Storage
from core.indexing import IndexManager
from core.parser import SQLParser
from core.engine import DatabaseEngine


app = FastAPI(
    title="Pesapal Custom RDBMS API",
    description="REST API demonstration of the custom RDBMS built from scratch",
    version="1.0.0"
)

# Initialize database components
storage = Storage()
index_manager = IndexManager()
parser = SQLParser()
engine = DatabaseEngine(storage, index_manager)


# Pydantic models for request/response
class CreateTableRequest(BaseModel):
    table_name: str
    schema: Dict[str, str]
    primary_key: Optional[str] = None
    unique_keys: Optional[List[str]] = None
class InsertRequest(BaseModel):
    table_name: str
    row: Dict[str, Any]
class SelectRequest(BaseModel):
    table_name: str
    columns: Optional[List[str]] = None
    where: Optional[Dict[str, Any]] = None
class UpdateRequest(BaseModel):
    table_name: str
    updates: Dict[str, Any]
    where: Optional[Dict[str, Any]] = None
class DeleteRequest(BaseModel):
    table_name: str
    where: Optional[Dict[str, Any]] = None
class JoinRequest(BaseModel):
    table1: str
    table2: str
    join_col1: str
    join_col2: str
    columns: Optional[List[str]] = None
    where: Optional[Dict[str, Any]] = None
@app.get("/", response_class=HTMLResponse)
def root():
    """Serve the dashboard HTML page."""
    try:
        template_path = os.path.join(os.path.dirname(__file__), "templates", "index.html")
        with open(template_path, "r") as f:
            return f.read()
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="Template file not found")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error loading template: {str(e)}")

@app.get("/tables")
def list_tables():
    """List all tables in the database."""
    try:
        data_dir = storage.data_dir
        tables = []
        if os.path.exists(data_dir):
            for filename in os.listdir(data_dir):
                if filename.endswith('.json'):
                    table_name = filename[:-5]  # Remove .json extension
                    table_data = storage.load_table(table_name)
                    tables.append({
                        "name": table_name,
                        "row_count": table_data["metadata"]["row_count"],
                        "columns": list(table_data["schema"].keys())
                    })
        return {"tables": tables}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/tables")
def create_table(request: CreateTableRequest):
    """Create a new table."""
    try:
        engine.create_table(
            request.table_name,
            request.schema,
            request.primary_key,
            request.unique_keys or []
        )
        return {
            "message": f"Table '{request.table_name}' created successfully",
            "table_name": request.table_name
        }
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
@app.post("/insert")
def insert_row(request: InsertRequest):
    """Insert a new row into a table."""
    try:
        engine.insert(request.table_name, request.row)
        return {
            "message": f"Row inserted into '{request.table_name}'",
            "table_name": request.table_name,
            "row": request.row
        }
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
@app.post("/select")
def select_rows(request: SelectRequest):
    """Select rows from a table."""
    try:
        rows = engine.select(
            request.table_name,
            request.columns,
            request.where
        )
        return {
            "table_name": request.table_name,
            "count": len(rows),
            "rows": rows
        }
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
@app.post("/update")
def update_rows(request: UpdateRequest):
    """Update rows in a table."""
    try:
        count = engine.update(
            request.table_name,
            request.updates,
            request.where
        )
        return {
            "message": f"Updated {count} row(s)",
            "table_name": request.table_name,
            "updated_count": count
        }
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
@app.post("/delete")
def delete_rows(request: DeleteRequest):
    """Soft delete rows from a table."""
    try:
        count = engine.delete(
            request.table_name,
            request.where
        )
        return {
            "message": f"Soft-deleted {count} row(s)",
            "table_name": request.table_name,
            "deleted_count": count
        }
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
@app.post("/join")
def join_tables(request: JoinRequest):
    """Perform an INNER JOIN between two tables."""
    try:
        rows = engine.inner_join(
            request.table1,
            request.table2,
            request.join_col1,
            request.join_col2,
            request.columns,
            request.where
        )
        return {
            "table1": request.table1,
            "table2": request.table2,
            "count": len(rows),
            "rows": rows
        }
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
@app.post("/api/transaction")
def create_transaction(transaction: Dict[str, Any]):
    try:
        # Load the latest data directly from storage to get the REAL next ID
        all_rows = storage.get_all_rows('transactions', include_deleted=True)
        next_id = max([row.get('id', 0) for row in all_rows], default=0) + 1
        
        # Force correct types
        user_id = int(transaction.get("user_id"))
        amount = float(transaction.get("amount"))
        
        row_data = {
            "id": next_id,
            "user_id": user_id,
            "amount": amount,
            "description": str(transaction.get("description", "")),
            "created_at": datetime.now().isoformat(),
            "is_deleted": False
        }
        
        engine.insert('transactions', row_data)
        return {"message": "Transaction created", "id": next_id}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
@app.post("/api/users")
def create_user(user_data: Dict[str, Any]):
    """Specific API to create a user with auto-incrementing ID."""
    try:
        # Load all users to calculate the next ID
        all_users = storage.get_all_rows('users', include_deleted=True)
        next_id = max([u.get('id', 0) for u in all_users], default=0) + 1
        
        row_data = {
            "id": next_id,
            "name": str(user_data.get("name")),
            "email": str(user_data.get("email")),
            "created_at": datetime.now().isoformat(),
            "is_deleted": False
        }
        
        engine.insert('users', row_data)
        return {"message": "User created successfully", "id": next_id}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"User creation failed: {str(e)}")
        
@app.get("/api/transactions_with_users")
def get_transactions_with_users():
    """
    Get all transactions joined with users, excluding soft-deleted transactions.
    Returns transactions with user information.
    """
    try:
        # Check if both tables exist
        if not storage.table_exists('transactions'):
            return {"transactions": [], "message": "Transactions table does not exist"}
        if not storage.table_exists('users'):
            return {"transactions": [], "message": "Users table does not exist"}
        
        # Perform INNER JOIN
        joined_rows = engine.inner_join(
            'transactions',
            'users',
            'user_id',  # transactions.user_id
            'id',       # users.id
            None,       # Select all columns
            None        # No WHERE clause
        )
        
        # Filter out soft-deleted transactions
        active_transactions = [
            row for row in joined_rows 
            if not (row.get('transactions.is_deleted') or row.get('is_deleted', False))
        ]
        
        return {
            "transactions": active_transactions,
            "count": len(active_transactions)
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching transactions: {str(e)}")

@app.get("/health")
def health_check():
    """Health check endpoint."""
    return {"status": "healthy", "service": "Pesapal Custom RDBMS"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)

