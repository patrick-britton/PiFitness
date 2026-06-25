#!/usr/bin/env python3
"""Investigate vw_weight_viz columns for test fix"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from dotenv import load_dotenv
load_dotenv('backend/.env')
from backend_functions.database_functions import sql_to_dict

cols = sql_to_dict("SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = 'health' AND table_name = 'vw_weight_viz' ORDER BY ordinal_position")
print('vw_weight_viz columns:')
for c in cols:
    print(f"  {c['column_name']}: {c['data_type']}")
print(f"Total: {len(cols)} columns")