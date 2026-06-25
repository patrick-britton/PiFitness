#!/usr/bin/env python3
"""Data Consistency Check: Compare old Streamlit query functions with new extracted query functions"""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
from dotenv import load_dotenv
load_dotenv('backend/.env')

print("=" * 60)
print("DATA CONSISTENCY CHECK: get_weight_targets()")
print("=" * 60)

# Old Streamlit function
from frontend_functions.health_module import get_weight_targets as old_get_weight_targets
# New extracted function
from backend_functions.queries.health_queries import get_weight_targets as new_get_weight_targets

old_result = old_get_weight_targets()
new_result = new_get_weight_targets()

print(f"Old function returned type: {type(old_result).__name__}")
print(f"New function returned type: {type(new_result).__name__}")
print(f"Old record count: {len(old_result)}")
print(f"New record count: {len(new_result)}")

if len(old_result) > 0 and len(new_result) > 0:
    old_first = old_result.iloc[0].to_dict() if hasattr(old_result, 'iloc') else old_result[0]
    new_first = new_result[0] if isinstance(new_result, (list, tuple)) else new_result
    
    print(f"\nOld first record:\n  ts_utc = {old_first.get('ts_utc')}")
    print(f"  weight_lb = {old_first.get('weight_lb')}")
    print(f"\nNew first record:\n  ts_utc = {new_first.get('ts_utc')}")
    print(f"  weight_lb = {new_first.get('weight_lb')}")
    
    old_ts_str = str(old_first.get('ts_utc', ''))
    new_ts_str = str(new_first.get('ts_utc', ''))
    old_wt = float(old_first.get('weight_lb', 0))
    new_wt = float(new_first.get('weight_lb', 0))
    
    match = abs(old_wt - new_wt) < 0.1
    print(f"\nWeight match: {old_wt} vs {new_wt} -> {'MATCH' if match else 'MISMATCH'}")
    print(f"Date: {old_ts_str[:19]} vs {new_ts_str[:19]}")
    print(f"Result: {'MATCH' if match else 'MISMATCH (weight differs by ' + str(abs(old_wt - new_wt)) + ' lbs)'}")
elif len(old_result) == 0 and len(new_result) == 0:
    print("\nResult: MATCH (both empty)")

print("\n" + "=" * 60)
print("DATA CONSISTENCY CHECK: get_rating_eligible_count()")
print("=" * 60)

from frontend_functions.music_module import get_rating_eligible_count as old_count
from backend_functions.queries.music_queries import get_rating_eligible_count as new_count

old_c = old_count()
new_c = new_count()
print(f"Old count: {old_c}")
print(f"New count: {new_c}")
print(f"Result: {'MATCH' if old_c == new_c else 'MISMATCH'}")