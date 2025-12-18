import pytest
from snowflake.snowpark import Session
import os
from dotenv import load_dotenv

load_dotenv()

@pytest.fixture
def snowflake_session():
    """Create Snowflake session for testing"""
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

def test_snowflake_connection(snowflake_session):
    """Test Snowflake connection works"""
    result = snowflake_session.sql("SELECT 1 as test").collect()
    assert len(result) == 1
    assert result[0]['TEST'] == 1

def test_tables_exist(snowflake_session):
    """Test required tables exist"""
    tables = snowflake_session.sql("SHOW TABLES").collect()
    table_names = [row['name'] for row in tables]
    
    assert 'RAW_SENSOR_READINGS' in table_names
    assert 'SENSOR_AGGREGATES' in table_names
    assert 'SENSOR_ALERTS' in table_names

def test_insert_and_query(snowflake_session):
    """Test insert and query data"""
    # Insert test data
    snowflake_session.sql("""
        INSERT INTO raw_sensor_readings 
        (reading_id, device_id, sensor_type, value, unit, timestamp, location)
        VALUES 
        ('test_integration_001', 'test_device', 'temperature', 25.5, 'C', 
         CURRENT_TIMESTAMP(), NULL)
    """).collect()
    
    # Query it back
    result = snowflake_session.sql("""
        SELECT * FROM raw_sensor_readings 
        WHERE reading_id = 'test_integration_001'
    """).collect()
    
    assert len(result) == 1
    assert result[0]['DEVICE_ID'] == 'test_device'
    assert result[0]['VALUE'] == 25.5
    
    # Cleanup
    snowflake_session.sql("""
        DELETE FROM raw_sensor_readings 
        WHERE reading_id = 'test_integration_001'
    """).collect()