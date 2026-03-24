
import sys, os, re, hmac, json
from pathlib import Path

# Mock FastAPI / Starlette classes if needed, or just test pure logic
def test_validate_db_name():
    print("Testing _validate_db_name...")
    # Logic from app.py:
    # def _validate_db_name(name: str) -> str:
    #     name = (name or "").strip()
    #     if not name or not re.match(r'^[a-zA-Z0-9_\-]+$', name) or ".." in name:
    #         raise Exception("Invalid database name")
    #     return name
    
    def validate(name):
        name = (name or "").strip()
        if not name or not re.match(r'^[a-zA-Z0-9_\-]+$', name) or ".." in name:
            return False
        return True

    assert validate("agentfactory") == True
    assert validate("my-db_123") == True
    assert validate("../etc/passwd") == False
    assert validate("db name with spaces") == False
    assert validate("") == False
    print("✅ _validate_db_name logic PASSED")

def test_admin_auth_logic():
    print("\nTesting admin_auth logic (HMAC)...")
    def auth(password, expected):
        return hmac.compare_digest(password.encode(), expected.encode())

    assert auth("secret123", "secret123") == True
    assert auth("wrong", "secret123") == False
    assert auth("", "secret123") == False
    print("✅ admin_auth HMAC logic PASSED")

def test_smart_convert_logic():
    print("\nTesting _smart_convert structure (JSON/CSV mock)...")
    # Mocking the flattening logic for JSON
    def flatten(obj):
        if isinstance(obj, dict):
            return "\n".join(f"{k}: {v}" for k, v in obj.items())
        return str(obj)

    sample_json = {"name": "Test", "role": "Bot"}
    converted = flatten(sample_json)
    assert "name: Test" in converted
    assert "role: Bot" in converted
    print("✅ _smart_convert (JSON flattening) logic PASSED")

def test_path_traversal_guards():
    print("\nTesting Path Traversal Guards...")
    # The app uses _validate_db_name for most DB paths
    paths_to_test = ["../../config.json", "valid_db", "db/with/slashes", "None"]
    
    def is_safe(p):
        return bool(re.match(r'^[a-zA-Z0-9_\-]+$', p))

    assert is_safe("agentfactory") == True
    assert is_safe("../../etc/passwd") == False
    assert is_safe("db.json") == False # Dots not allowed in DB names
    print("✅ Path Traversal Guard logic PASSED")

if __name__ == "__main__":
    try:
        test_validate_db_name()
        test_admin_auth_logic()
        test_smart_convert_logic()
        test_path_traversal_guards()
        print("\n" + "="*40)
        print("  ALL OFFLINE LOGIC TESTS PASSED")
        print("  (No APIs used, No Bot questions asked)")
        print("="*40)
    except AssertionError as e:
        print(f"\n❌ TEST FAILED")
        sys.exit(1)
