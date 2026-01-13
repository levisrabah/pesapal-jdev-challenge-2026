import requests
import json

BASE_URL = "http://127.0.0.1:8000"

def test_workflow():
    print("Starting API Integration Test...")

    # 1. Create a New User
    user_payload = {
        "name": "Ondiek The Great",
        "email": "ondiek@pesapal.com"
    }
    user_res = requests.post(f"{BASE_URL}/api/users", json=user_payload)
    user_id = user_res.json().get("id")
    print(f"✅User Created: ID {user_id}")

    # 2. Post a Transaction for that User
    txn_payload = {
        "user_id": user_id,
        "amount": 1250.75,
        "description": "API Test Deposit"
    }
    txn_res = requests.post(f"{BASE_URL}/api/transaction", json=txn_payload)
    print(f"✅Transaction Posted: {txn_res.json().get('message')}")

    # 3. Verify the Joined Data
    ledger_res = requests.get(f"{BASE_URL}/api/transactions_with_users")
    ledger = ledger_res.json().get("transactions", [])
    
    # Check if our new transaction is in the joined results
    found = any(t for t in ledger if t.get('transactions.id') == txn_res.json().get('id'))
    
    if found:
        print("SUCCESS: Data verified in Ledger with User Join!")
    else:
        print("❌ FAIL: Transaction not found in Join.")

if __name__ == "__main__":
    try:
        test_workflow()
    except Exception as e:
        print(f"❌ Connection Error: Is your app.py running? ({e})")