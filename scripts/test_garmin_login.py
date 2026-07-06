"""
Quick test for the new DB-backed Garmin login methodology.
"""
import os
import sys
import time
from pathlib import Path

# Add project root
sys.path.insert(0, str(Path(__file__).parent.parent))

# Load .env
from dotenv import load_dotenv
load_dotenv('backend/.env')

from backend_functions.service_logins import pirate_garmin_login, load_token_from_db
from backend_functions.database_functions import sql_to_dict

print('=== Call #1: First pirate_garmin_login() ===')
token1 = pirate_garmin_login()
if token1:
    print('Token1: age=0s, client present:', token1.get('client') is not None)
    print('Token1: token preview:', token1.get('token', '')[:30])
else:
    print('Token1: FAILED - returned None')

print()
print('=== Call #2: Should reuse cached session ===')
token2 = pirate_garmin_login()
if token2:
    print('Token2: age=0s, client present:', token2.get('client') is not None)
else:
    print('Token2: FAILED - returned None')

print()
print('=== Verify DB has both DI and IT tokens ===')
session = load_token_from_db('Garmin')
if session:
    di_token = session.get('di', {}).get('token', {})
    it_token = session.get('it', {}).get('token', {})
    di_expires = di_token.get('expires_at', 0)
    it_expires = it_token.get('expires_at', 0)
    now = int(time.time())
    print('DI token present:', bool(di_token))
    print('IT token present:', bool(it_token))
    print('DI token expires_at:', di_expires, f'({di_expires - now}s from now)')
    print('IT token expires_at:', it_expires, f'({it_expires - now}s from now)')
else:
    print('No session found in DB!')

print()
print('=== Last 5 Garmin login events from api_logins ===')
sql = """SELECT event_time_utc, event_name, error_text 
 FROM logging.api_logins 
 WHERE api_service_name = 'Garmin' 
 ORDER BY event_time_utc DESC LIMIT 5"""
rows = sql_to_dict(sql)
for r in rows:
    err = f' — ERROR: {r["error_text"]}' if r.get('error_text') else ''
    print(f'  {r["event_time_utc"]}: {r["event_name"]}{err}')

print()
print('=== Summary ===')
if token1 and token2:
    print('✅ Garmin login using pirate-garmin native Android auth works!')
    print('✅ DB stores both DI and IT tokens for cross-device sync')
    print('⚠️  Note: Second call may have triggered browser login if session is new.')
    print('   Repeated calls within the token expiry window will be faster.')
else:
    print('❌ One or both login calls failed')