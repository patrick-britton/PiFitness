#!/usr/bin/env python3

import sys
import os
import re

# Add backend to path
sys.path.append(os.path.join(os.path.dirname(__file__), 'backend'))

from main import app
from fastapi.testclient import TestClient

def extract_api_client_endpoints():
    """Extract all endpoints from the frontend API client."""
    api_client_path = 'frontend/pifitness/src/lib/api-client.ts'

    if not os.path.exists(api_client_path):
        print(f"API client file not found: {api_client_path}")
        return []

    with open(api_client_path, 'r') as f:
        content = f.read()

    # Find all API calls in the format fetchAPI("/api/...")
    pattern = r'fetchAPI\(["\'](/api/[^"\']+)["\']'
    endpoints = re.findall(pattern, content)

    # Also find any hardcoded paths
    pattern2 = r'["\'](/api/[^"\']+)["\']'
    hardcoded_endpoints = re.findall(pattern2, content)

    all_endpoints = list(set(endpoints + hardcoded_endpoints))  # Remove duplicates

    # Filter out any that are just parameters or partial matches
    valid_endpoints = []
    for endpoint in all_endpoints:
        if endpoint.startswith('/api/') and not endpoint.endswith('{'):  # Skip template strings
            # Remove query parameters for comparison
            clean_endpoint = endpoint.split('?')[0]
            valid_endpoints.append(clean_endpoint)

    return sorted(set(valid_endpoints))

def get_backend_endpoints():
    """Get all available backend endpoints."""
    endpoints = set()

    # Get all routes from the app
    for route in app.routes:
        if hasattr(route, 'path') and route.path.startswith('/api/'):
            endpoints.add(route.path)
        elif hasattr(route, 'routes'):
            for sub_route in route.routes:
                if hasattr(sub_route, 'path'):
                    # Combine prefix and path
                    full_path = route.prefix + sub_route.path
                    endpoints.add(full_path)

    return sorted(endpoints)

def test_endpoints():
    """Test all frontend-expected endpoints against backend."""
    print("=== API Endpoint Validation ===\n")

    frontend_endpoints = extract_api_client_endpoints()
    backend_endpoints = get_backend_endpoints()

    print(f"Frontend expects {len(frontend_endpoints)} endpoints:")
    for endpoint in frontend_endpoints:
        print(f"  {endpoint}")

    print(f"\nBackend provides {len(backend_endpoints)} endpoints:")
    for endpoint in backend_endpoints:
        print(f"  {endpoint}")

    # Find missing endpoints
    missing_endpoints = []
    for frontend_endpoint in frontend_endpoints:
        if frontend_endpoint not in backend_endpoints:
            missing_endpoints.append(frontend_endpoint)

    if missing_endpoints:
        print(f"\n❌ MISSING ENDPOINTS ({len(missing_endpoints)}):")
        for endpoint in missing_endpoints:
            print(f"  {endpoint}")
    else:
        print(f"\n✅ ALL ENDPOINTS PRESENT!")

    # Test all frontend endpoints
    print(f"\n=== Testing Frontend Endpoints ===")
    client = TestClient(app)

    for endpoint in frontend_endpoints:
        try:
            response = client.get(endpoint)
            status = "✅" if response.status_code == 200 else "❌"
            print(f"{status} {endpoint} - Status: {response.status_code}")
        except Exception as e:
            print(f"❌ {endpoint} - Exception: {str(e)}")

    return missing_endpoints

if __name__ == "__main__":
    missing = test_endpoints()
    if missing:
        print(f"\n⚠️  WARNING: {len(missing)} endpoints are missing!")
        sys.exit(1)
    else:
        print(f"\n🎉 All endpoints are available!")
        sys.exit(0)