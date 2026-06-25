#!/usr/bin/env python3
"""Smoke test: Start FastAPI server, call weight-targets endpoint, verify JSON response"""
import sys, os, time, json, urllib.request, subprocess

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

# Load .env
from dotenv import load_dotenv
load_dotenv(os.path.join(project_root, 'backend', '.env'))

# Start server
server = subprocess.Popen(
    [sys.executable, "-m", "uvicorn", "backend.main:app", "--host", "127.0.0.1", "--port", "8001"],
    cwd=project_root,
    stdout=subprocess.DEVNULL,
    stderr=subprocess.DEVNULL
)

try:
    time.sleep(3)
    
    # Test health endpoint
    r = urllib.request.urlopen("http://127.0.0.1:8001/api/health")
    health_data = json.loads(r.read())
    print(f"✅ /api/health: status={r.status}, db={health_data['database']}")
    
    # Test weight-targets endpoint
    r = urllib.request.urlopen("http://127.0.0.1:8001/api/health/weight-targets")
    wt_data = json.loads(r.read())
    print(f"✅ /api/health/weight-targets: status={r.status}, count={wt_data['count']}")
    if wt_data['data']:
        print(f"   First record keys: {list(wt_data['data'][0].keys())}")
    
    print("\n✅ Smoke test PASSED - FastAPI serves query function data correctly")
    
finally:
    server.terminate()
    server.wait()