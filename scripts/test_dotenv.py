"""Quick script to verify .env loading and DB connectivity."""
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from dotenv import load_dotenv

# Simulate what database_functions.py does now
load_dotenv()  # CWD

backend_env = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath('backend_functions/database_functions.py'))),
    'backend', '.env'
)
print(f"backend_env path computed: {backend_env}")
print(f"backend_env exists: {os.path.exists(backend_env)}")

# Also check what __file__ resolves to from the actual module
from backend_functions.database_functions import __file__ as dbf_file
print(f"database_functions.py __file__: {dbf_file}")

backend_env2 = os.path.join(os.path.dirname(os.path.dirname(dbf_file)), 'backend', '.env')
print(f"Corrected backend_env path: {backend_env2}")
print(f"Corrected backend_env exists: {os.path.exists(backend_env2)}")

if os.path.exists(backend_env2):
    load_dotenv(backend_env2)

print(f"PG_HOST: {os.getenv('PG_HOST')}")
print(f"PG_USER: {os.getenv('PG_USER')}")
print(f"PG_PORT: {os.getenv('PG_PORT')}")
print(f"PG_DB: {os.getenv('PG_DB')}")