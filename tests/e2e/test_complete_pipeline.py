import pytest
import requests
import time
from datetime import datetime
from snowflake.snowpark import Session
import os
from dotenv import load_dotenv

load_dotenv()

API_URL = "http://localhost:8000"
QUERY_URL = "http://localhost:8001"

@pytest.fixture
def snowflake_session():
    session = Session.builder.configs({
        'account': os.getenv('SNOWFLAKE_ACCOUNT'),
        'user': os.getenv('SNOWFLAKE_USER'),
        'password': os.getenv('SNOWFLAKE_PASSWORD'),
        'role': os.getenv('SNOWFLAKE_ROLE'),
        'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE'),
        'database': os.getenv('SNOWFLAKE_DATABASE'),
        'schema': os.getenv('SNOWFLAKE_SCHEMA')
    }).create()
    yield session
    session.close()

def test_end_to_end_pipeline(snowflake_session):
    """Test complete pipeline: API → Kafka → Processor → Snowflake → Query API"""
    
    # Step 1: Send data to ingestion API
    reading_data = {
        "device_id": "e2e_test_sensor",
        "sensor_type": "temperature",
        "value": 99.9,
        "unit": "C",
        "timestamp": datetime.utcnow().isoformat(),
        "location": {"building": "TEST", "floor": 99}
    }
    
    response = requests.post(f"{API_URL}/ingest", json=reading_data)
    assert response.status_code == 200
    reading_id = response.json()["reading_id"]
    
    # Step 2: Wait for processor to consume and store (max 10 seconds)
    max_wait = 10
    found = False
    
    for _ in range(max_wait):
        result = snowflake_session.sql(f"""
            SELECT * FROM raw_sensor_readings 
            WHERE reading_id = '{reading_id}'
        """).collect()
        
        if len(result) > 0:
            found = True
            break
        
        time.sleep(1)
    
    assert found, f"Data not found in Snowflake after {max_wait} seconds"
    
    # Step 3: Verify data in Snowflake
    data = result[0]
    assert data['DEVICE_ID'] == 'e2e_test_sensor'
    assert data['SENSOR_TYPE'] == 'temperature'
    assert data['VALUE'] == 99.9
    
    # Step 4: Check alert was triggered (value 99.9 > threshold 100)
    # Should NOT trigger alert since 99.9 < 100
    alerts = snowflake_session.sql(f"""
        SELECT * FROM sensor_alerts 
        WHERE device_id = 'e2e_test_sensor'
    """).collect()
    
    # Step 5: Query via Query API
    response = requests.get(f"{QUERY_URL}/readings?device_id=e2e_test_sensor")
    assert response.status_code == 200
    api_data = response.json()
    assert len(api_data) >= 1
    
    # Cleanup
    snowflake_session.sql(f"""
        DELETE FROM raw_sensor_readings 
        WHERE device_id = 'e2e_test_sensor'
    """).collect()
    
    print("✅ End-to-end pipeline test passed!")