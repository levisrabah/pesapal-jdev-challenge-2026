from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from typing import List, Dict, Any, Optional
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
@app.get("/")
def root():
    """Root endpoint with API information."""
    return {
        "message": "Welcome to Pesapal Custom RDBMS API",
        "version": "1.0.0",
        "endpoints": {
            "GET /tables": "List all tables",
            "POST /tables": "Create a new table",
            "POST /insert": "Insert a row",
            "POST /select": "Query rows",
            "POST /update": "Update rows",
            "POST /delete": "Soft delete rows",
            "POST /join": "Perform INNER JOIN"
        }
    }

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
@app.get("/health")
def health_check():
    """Health check endpoint."""
    return {"status": "healthy", "service": "Pesapal Custom RDBMS"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)

