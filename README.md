# Pesapal Custom RDBMS - Immutable Transaction Ledger

A custom Relational Database Management System (RDBMS) built from scratch in Python, designed specifically for fintech applications with immutable transaction ledger capabilities.

## 🎯 Project Overview

This project implements a fully functional RDBMS without using any SQL libraries (no sqlite3, SQLAlchemy, or pandas). It's built using only standard Python libraries (`json`, `os`, `re`, `abc`) and provides:

- **CRUD Operations**: Create, Read, Update, and Delete operations
- **SQL-like Syntax**: Regex-based SQL parser for intuitive queries
- **Data Integrity**: Primary Key and Unique Key constraints
- **Join Operations**: INNER JOIN support for relational queries
- **Indexing**: Hash map-based indexing for O(1) lookups
- **Soft Deletes**: Fintech-compliant immutable ledger (data never truly deleted)

## 📁 Project Structure

```
pesapal-jdev-challenge-2026/
├── core/                    # Core database engine
│   ├── __init__.py
│   ├── engine.py           # CRUD operations & Join logic
│   ├── parser.py           # Regex-based SQL parser
│   ├── storage.py          # JSON persistence layer
│   └── indexing.py         # In-memory hash map index
├── web_app/                 # FastAPI demonstration app
│   ├── __init__.py
│   └── app.py              # REST API endpoints
├── data/                   # JSON table storage directory
├── main.py                 # Interactive REPL (CLI)
├── requirements.txt        # Python dependencies
└── README.md              # This file
```

## 🚀 Quick Start

### Prerequisites

- Python 3.8 or higher
- pip (Python package manager)

### Installation

1. Clone or navigate to the project directory:
```bash
cd pesapal-jdev-challenge-2026
```

2. Install dependencies (for web app):
```bash
pip install -r requirements.txt
```

### Running the REPL (Command Line Interface)

Start the interactive SQL REPL:
```bash
python main.py
```

Example session:
```sql
SQL> CREATE TABLE users (id INT PRIMARY KEY, name TEXT, email TEXT UNIQUE)
✅ Table 'users' created successfully.

SQL> INSERT INTO users VALUES (1, 'Eng Midusa', 'midusa@yahoo.com')
✅ Row inserted into 'users'.

SQL> SELECT * FROM users
id | name      | email
---|-----------|------------------
1  | Levis Rabah  | levisrabah@gmail.com
```

### Running the Web Application

Start the FastAPI server:
```bash
cd web_app
python app.py
```

Or using uvicorn directly:
```bash
uvicorn web_app.app:app --reload
```

The API will be available at `http://localhost:8000`

API Documentation (Swagger UI): `http://localhost:8000/docs`

## 📚 Core Features

### 1. Data Types

The system supports three data types:
- **INT**: Integer numbers
- **FLOAT**: Floating-point numbers
- **TEXT**: String values (use single quotes: `'Wesley Kamanda'`)

### 2. Constraints

- **Primary Key**: Ensures uniqueness and identifies each row uniquely
- **Unique Key**: Ensures column values are unique across all rows

### 3. SQL Operations

#### CREATE TABLE
```sql
CREATE TABLE users (
    id INT PRIMARY KEY,
    name TEXT,
    email TEXT UNIQUE
)
```

#### INSERT
```sql
INSERT INTO users VALUES (1, 'Midusa Apollo', 'apollo@gmail.com')
-- Or with explicit columns:
INSERT INTO users (id, name, email) VALUES (2, 'Sarah Waweru', 'wawesh@gmail.com')
```

#### SELECT
```sql
-- Select all columns
SELECT * FROM users

-- Select specific columns
SELECT name, email FROM users

-- With WHERE clause
SELECT * FROM users WHERE id = 1
SELECT * FROM users WHERE name != 'Midusa'
```

#### UPDATE
```sql
UPDATE users SET name = 'Monica Gichana' WHERE id = 1
UPDATE users SET email = 'mg@gmail.com' WHERE name = 'Levis Rabah'
```

#### DELETE (Soft Delete)
```sql
DELETE FROM transactions WHERE id = 5
-- Sets is_deleted = True (data preserved for audit trail)
```

#### INNER JOIN
```sql
SELECT * FROM transactions 
INNER JOIN users 
ON transactions.user_id = users.id

-- With WHERE clause
SELECT transactions.amount, users.name 
FROM transactions 
INNER JOIN users 
ON transactions.user_id = users.id 
WHERE transactions.amount > 1000
```

## 🔍 Indexing Strategy

### Hash Map Index Implementation

The system uses an in-memory hash map index for fast lookups on `user_id` columns. This provides O(1) average-case lookup performance.

**How it works:**

1. **Index Structure**: 
   - Hash map: `{user_id_value: set(row_indices)}`
   - Reverse mapping: `{row_index: user_id_value}` for efficient updates

2. **Automatic Indexing**:
   - Indexes are automatically created for any column named `user_id`
   - Indexes are rebuilt when tables are loaded or after bulk operations

3. **Join Optimization**:
   - When performing INNER JOIN on `user_id`, the system uses the index
   - Instead of O(n×m) nested loop, joins become O(n) where n is the smaller table

4. **Index Maintenance**:
   - Indexes are updated on INSERT, UPDATE, and DELETE operations
   - Soft-deleted rows are removed from the index (but data remains in storage)

**Example:**
```python
# Index structure for user_id = 1
index = {
    1: {0, 3, 7},  # Rows at indices 0, 3, 7 have user_id = 1
    2: {1, 5},     # Rows at indices 1, 5 have user_id = 2
    ...
}
```

### Performance Benefits

- **Lookup**: O(1) average case vs O(n) linear search
- **Join**: O(n) vs O(n×m) for indexed joins
- **Memory**: Minimal overhead (only stores indices, not data)

## 🏦 Fintech Features

### Immutable Transaction Ledger

Following fintech best practices, the system implements **soft deletes**:

- When a transaction is "deleted", it's not removed from storage
- Instead, an `is_deleted` flag is set to `True`
- This ensures:
  - **Audit Trail**: Complete history of all transactions
  - **Compliance**: Meets regulatory requirements for financial data
  - **Recovery**: Ability to "undelete" if needed
  - **Analytics**: Historical data remains available

**Example:**
```sql
-- "Delete" a transaction
DELETE FROM transactions WHERE id = 123

-- The row still exists in storage:
-- {id: 123, amount: 1000, user_id: 1, is_deleted: True}

-- SELECT queries automatically filter out soft-deleted rows
SELECT * FROM transactions  -- Only shows non-deleted rows
```

## 🧪 Example: Users and Transactions

Here's a complete example demonstrating the fintech use case:

```sql
-- Create Users table
CREATE TABLE users (
    id INT PRIMARY KEY,
    name TEXT,
    email TEXT UNIQUE
)

-- Create Transactions table
CREATE TABLE transactions (
    id INT PRIMARY KEY,
    user_id INT,
    amount FLOAT,
    description TEXT,
    timestamp TEXT
)

-- Insert users
INSERT INTO users VALUES (1, 'Alice Johnson', 'alice@example.com')
INSERT INTO users VALUES (2, 'Bob Smith', 'bob@example.com')

-- Insert transactions
INSERT INTO transactions VALUES (1, 1, 1500.50, 'Payment received', '2024-01-15')
INSERT INTO transactions VALUES (2, 1, -250.00, 'Payment sent', '2024-01-16')
INSERT INTO transactions VALUES (3, 2, 3000.00, 'Salary deposit', '2024-01-15')

-- Query all transactions for a user (uses index)
SELECT * FROM transactions WHERE user_id = 1

-- Join transactions with users
SELECT transactions.amount, transactions.description, users.name 
FROM transactions 
INNER JOIN users 
ON transactions.user_id = users.id

-- Soft delete a transaction (preserves audit trail)
DELETE FROM transactions WHERE id = 2

-- Transaction still exists but won't appear in SELECT queries
```

## 🌐 Web API Endpoints

The FastAPI application provides REST endpoints:

- `GET /` - API information
- `GET /tables` - List all tables
- `POST /tables` - Create a new table
- `POST /insert` - Insert a row
- `POST /select` - Query rows
- `POST /update` - Update rows
- `POST /delete` - Soft delete rows
- `POST /join` - Perform INNER JOIN
- `GET /health` - Health check

**Example API Request:**
```bash
curl -X POST "http://localhost:8000/insert" \
  -H "Content-Type: application/json" \
  -d '{
    "table_name": "users",
    "row": {
      "id": 1,
      "name": "John Doe",
      "email": "levisrabah@gmail.com"
    }
  }'
```

## 🔧 Technical Implementation Details

### Storage Layer (`storage.py`)
- Uses JSON files for persistence (one file per table)
- Each table file contains: schema, rows array, and metadata
- Atomic write operations ensure data consistency

### Parser (`parser.py`)
- Regex-based SQL parsing (no external parsing libraries)
- Supports standard SQL syntax with Python-friendly extensions
- Handles string escaping, NULL values, and type conversion

### Engine (`engine.py`)
- Implements all CRUD operations
- Validates data types and constraints
- Handles soft deletes automatically
- Optimizes joins using indexes

### Indexing (`indexing.py`)
- Hash map-based index for O(1) lookups
- Maintains bidirectional mapping for efficient updates
- Automatically rebuilds on table load

## 📝 Limitations & Future Enhancements

**Current Limitations:**
- Single-file storage (not optimized for very large datasets)
- No transaction support (ACID properties)
- Limited WHERE clause operators
- No foreign key constraints (enforced at application level)

**Potential Enhancements:**
- B-tree indexes for range queries
- Transaction support with rollback
- Additional JOIN types (LEFT, RIGHT, FULL)
- Query optimization and execution plans
- Multi-file storage for large tables
- Backup and recovery utilities

## 🤝 AI Attribution

This project was developed with assistance from **Cursor AI**, an AI-powered coding assistant. Cursor AI helped with:

- Code structure and architecture design
- Implementation of core database components
- SQL parsing logic and regex patterns
- Documentation and README generation
- Best practices for Python code organization

The final codebase represents a collaborative effort between human engineering decisions and AI-assisted development, following the challenge requirements for AI attribution.

## 📄 License

This project is part of the Pesapal Junior Dev Challenge 2026.

## 👨‍💻 Author

Built by Levis Rabah for the Pesapal Junior Dev Challenge 2026 - Custom RDBMS Implementation

---

**Note**: This is a demonstration project built from scratch without SQL libraries. For production use, consider established database systems with full ACID compliance and scalability features.

