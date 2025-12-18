import pytest
from fastapi.testclient import TestClient
from datetime import datetime
import sys
import os

# Add services to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../services/ingestion'))

from main import app

client = TestClient(app)

def test_root_endpoint():
    """Test root endpoint returns correct info"""
    response = client.get("/")
    assert response.status_code == 200
    data = response.json()
    assert data["service"] == "IoT Ingestion API"
    assert data["status"] == "running"

def test_health_check():
    """Test health check endpoint"""
    response = client.get("/health")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "healthy"

def test_valid_sensor_reading():
    """Test ingesting valid sensor reading"""
    reading = {
        "device_id": "test_sensor_001",
        "sensor_type": "temperature",
        "value": 25.5,
        "unit": "C",
        "timestamp": datetime.utcnow().isoformat()
    }
    
    response = client.post("/ingest", json=reading)
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "success"
    assert "reading_id" in data

def test_invalid_sensor_type():
    """Test that invalid sensor type is rejected"""
    reading = {
        "device_id": "test_sensor_001",
        "sensor_type": "invalid_type",  # Invalid
        "value": 25.5,
        "unit": "C",
        "timestamp": datetime.utcnow().isoformat()
    }
    
    response = client.post("/ingest", json=reading)
    assert response.status_code == 422  # Validation error

def test_temperature_out_of_range():
    """Test that out-of-range temperature is rejected"""
    reading = {
        "device_id": "test_sensor_001",
        "sensor_type": "temperature",
        "value": 200,  # Too high
        "unit": "C",
        "timestamp": datetime.utcnow().isoformat()
    }
    
    response = client.post("/ingest", json=reading)
    assert response.status_code == 422

def test_batch_ingest():
    """Test batch ingestion"""
    readings = {
        "readings": [
            {
                "device_id": f"sensor_{i}",
                "sensor_type": "temperature",
                "value": 20 + i,
                "unit": "C",
                "timestamp": datetime.utcnow().isoformat()
            }
            for i in range(5)
        ]
    }
    
    response = client.post("/ingest/batch", json=readings)
    assert response.status_code == 200
    data = response.json()
    assert data["total"] == 5
    assert data["success"] == 5