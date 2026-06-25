import sys
import os

# Add the project root to Python path so we can import backend
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from fastapi.testclient import TestClient
from backend.main import app

def test_health():
    # Create a test client for FastAPI
    client = TestClient(app)

    # Make request to health endpoint
    response = client.get("/api/health")

    # Assert the response
    assert response.status_code == 200
    response_data = response.json()
    assert response_data["status"] == "ok"
    assert "database" in response_data
    assert "timestamp" in response_data
