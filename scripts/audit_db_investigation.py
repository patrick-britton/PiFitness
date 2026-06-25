#!/usr/bin/env python3
"""
Temporary script to investigate database structure for audit purposes.
This file can be deleted after the audit is complete.
"""
import sys
import os

# Add the project root to Python path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path:
    sys.path.append(project_root)

# Load .env file from backend directory
env_path = os.path.join(project_root, 'backend', '.env')
if os.path.exists(env_path):
    from dotenv import load_dotenv
    load_dotenv(env_path)

from backend_functions.database_functions import sql_to_dict

# 1. Check music.all_tracks columns
print("=" * 60)
print("1. music.all_tracks columns:")
print("=" * 60)
cols = sql_to_dict(
    "SELECT column_name, data_type, is_nullable, character_maximum_length "
    "FROM information_schema.columns "
    "WHERE table_schema = 'music' AND table_name = 'all_tracks' "
    "ORDER BY ordinal_position"
)
for c in cols:
    print(f"  {c['column_name']}: {c['data_type']} nullable={c['is_nullable']}")
print(f"Total: {len(cols)} columns")

# 2. Check music tables/views
print("\n" + "=" * 60)
print("2. music schema tables/views:")
print("=" * 60)
tables = sql_to_dict(
    "SELECT table_name, table_type FROM information_schema.tables "
    "WHERE table_schema = 'music' ORDER BY table_name"
)
for t in tables:
    print(f"  {t['table_name']} ({t['table_type']})")

# 3. Find tables/views with 'track' in name in music schema
print("\n" + "=" * 60)
print("3. Tables/views containing 'track' in music schema:")
print("=" * 60)
track_tables = sql_to_dict(
    "SELECT table_name, table_type FROM information_schema.tables "
    "WHERE table_schema = 'music' AND table_name LIKE '%track%' "
    "ORDER BY table_name"
)
for t in track_tables:
    cols2 = sql_to_dict(
        f"SELECT column_name, data_type FROM information_schema.columns "
        f"WHERE table_schema = 'music' AND table_name = '{t['table_name']}' "
        f"ORDER BY ordinal_position"
    )
    print(f"  {t['table_name']} ({t['table_type']}): {len(cols2)} columns")
    for c in cols2:
        print(f"    - {c['column_name']}: {c['data_type']}")

# 4. Check activities.activities columns to verify start_timestamp_utc type
print("\n" + "=" * 60)
print("4. activities.activities - start_timestamp_utc column details:")
print("=" * 60)
ts_col = sql_to_dict(
    "SELECT column_name, data_type, is_nullable "
    "FROM information_schema.columns "
    "WHERE table_schema = 'activities' AND table_name = 'activities' "
    "AND column_name = 'start_timestamp_utc'"
)
if ts_col:
    print(f"  {ts_col[0]['column_name']}: {ts_col[0]['data_type']} nullable={ts_col[0]['is_nullable']}")

# 5. Check health.sleep_totals for hrv_value type
print("\n" + "=" * 60)
print("5. health.sleep_totals - hrv_value column details:")
print("=" * 60)
hrv_col = sql_to_dict(
    "SELECT column_name, data_type, is_nullable "
    "FROM information_schema.columns "
    "WHERE table_schema = 'health' AND table_name = 'sleep_totals' "
    "AND column_name = 'hrv_value'"
)
if hrv_col:
    print(f"  {hrv_col[0]['column_name']}: {hrv_col[0]['data_type']} nullable={hrv_col[0]['is_nullable']}")

# 6. Count stored procedures in staging schema
print("\n" + "=" * 60)
print("6. Stored procedures in staging schema:")
print("=" * 60)
procs = sql_to_dict(
    "SELECT specific_name, routine_name, routine_type, data_type "
    "FROM information_schema.routines "
    "WHERE specific_schema = 'staging' "
    "ORDER BY routine_name"
)
print(f"Total procedures/functions: {len(procs)}")
for p in procs:
    print(f"  {p['routine_name']} ({p['routine_type']}) -> {p['data_type']}")

# 7. Check the database_functions import works for queries
print("\n" + "=" * 60)
print("7. Testing query function imports:")
print("=" * 60)
try:
    from backend_functions.queries import get_weight_targets, get_rating_eligible_count, get_playlist_config
    print("  ✅ All query function imports successful")
except Exception as e:
    print(f"  ❌ Import error: {e}")

# 8. Test a weight target query
print("\n" + "=" * 60)
print("8. Testing get_weight_targets():")
print("=" * 60)
try:
    from backend_functions.queries import get_weight_targets
    result = get_weight_targets()
    print(f"  Returned {len(result)} records")
    if result:
        print(f"  First record: {result[0]}")
    print("  ✅ Query executed successfully")
except Exception as e:
    print(f"  ❌ Query error: {e}")

# 9. Test get_rating_eligible_count()
print("\n" + "=" * 60)
print("9. Testing get_rating_eligible_count():")
print("=" * 60)
try:
    from backend_functions.queries import get_rating_eligible_count
    count = get_rating_eligible_count()
    print(f"  Rating eligible count: {count}")
    print("  ✅ Query executed successfully")
except Exception as e:
    print(f"  ❌ Query error: {e}")

# 10. Test get_playlist_config with a sample ID
print("\n" + "=" * 60)
print("10. Testing get_playlist_config (fetching first playlist):")
print("=" * 60)
try:
    playlists = sql_to_dict("SELECT playlist_id, playlist_name FROM music.playlist_config LIMIT 5")
    print(f"  Found {len(playlists)} playlists")
    for p in playlists:
        print(f"    {p['playlist_id']}: {p['playlist_name']}")
    
    if playlists:
        test_id = playlists[0]['playlist_id']
        from backend_functions.queries import get_playlist_config
        config = get_playlist_config(test_id)
        print(f"  get_playlist_config returned {len(config)} records")
        if config:
            print(f"  First record keys: {list(config[0].keys())}")
    print("  ✅ Query executed successfully")
except Exception as e:
    print(f"  ❌ Query error: {e}")

# 11. Check git branch
print("\n" + "=" * 60)
print("11. Git branch information:")
print("=" * 60)
import subprocess
try:
    result = subprocess.run(["git", "branch"], capture_output=True, text=True, cwd=project_root)
    print(result.stdout)
    # Check if streamlit-prd has any changes
    result2 = subprocess.run(["git", "log", "--oneline", "-1", "streamlit-prd"], capture_output=True, text=True, cwd=project_root)
    print(f"  streamlit-prd latest: {result2.stdout.strip()}")
    # Check if react-ui has any changes
    result3 = subprocess.run(["git", "log", "--oneline", "-1", "react-ui"], capture_output=True, text=True, cwd=project_root)
    print(f"  react-ui latest: {result3.stdout.strip()}")
except Exception as e:
    print(f"  Git check error: {e}")

print("\n" + "=" * 60)
print("Database investigation complete.")
print("=" * 60)