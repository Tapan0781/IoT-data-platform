from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel
from typing import List, Optional
from datetime import datetime, timedelta
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col, avg, min as sf_min, max as sf_max, count
import logging
import os
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="IoT Query API", version="1.0.0")

# Snowflake connection
def get_session():
    return Session.builder.configs({
        'account': os.getenv('SNOWFLAKE_ACCOUNT'),
        'user': os.getenv('SNOWFLAKE_USER'),
        'password': os.getenv('SNOWFLAKE_PASSWORD'),
        'role': os.getenv('SNOWFLAKE_ROLE'),
        'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE'),
        'database': os.getenv('SNOWFLAKE_DATABASE'),
        'schema': os.getenv('SNOWFLAKE_SCHEMA')
    }).create()

# Response models
class SensorReadingResponse(BaseModel):
    reading_id: str
    device_id: str
    sensor_type: str
    value: float
    unit: str
    timestamp: str

# Health check
@app.get('/')
async def root():
    return {
        'service': 'IoT Query API',
        'status': 'running',
        'version': '1.0.0'
    }

@app.get('/health')
async def health_check():
    try:
        session = get_session()
        session.sql('SELECT 1').collect()
        session.close()
        return {'status': 'healthy', 'snowflake': 'connected'}
    except Exception as e:
        return {'status': 'unhealthy', 'error': str(e)}

# Get recent readings
@app.get('/readings')
async def get_readings(
    device_id: Optional[str] = None,
    sensor_type: Optional[str] = None,
    limit: int = Query(100, le=1000)
):
    session = get_session()
    
    try:
        query = "SELECT * FROM raw_sensor_readings WHERE 1=1"
        
        if device_id:
            query += f" AND device_id = '{device_id}'"
        
        if sensor_type:
            query += f" AND sensor_type = '{sensor_type}'"
        
        query += f" ORDER BY timestamp DESC LIMIT {limit}"
        
        df = session.sql(query).to_pandas()
        results = df.to_dict('records')
        
        session.close()
        return results
    
    except Exception as e:
        session.close()
        raise HTTPException(status_code=500, detail=str(e))

# Get device statistics
@app.get('/device/{device_id}/stats')
async def get_device_stats(device_id: str, days: int = Query(7, le=90)):
    session = get_session()
    
    try:
        query = f"""
            SELECT 
                sensor_type,
                AVG(value) as avg_value,
                MIN(value) as min_value,
                MAX(value) as max_value,
                COUNT(*) as reading_count
            FROM raw_sensor_readings
            WHERE device_id = '{device_id}'
            AND timestamp >= DATEADD(day, -{days}, CURRENT_TIMESTAMP())
            GROUP BY sensor_type
        """
        
        df = session.sql(query).to_pandas()
        results = df.to_dict('records')
        
        session.close()
        
        return {
            'device_id': device_id,
            'period_days': days,
            'statistics': results
        }
    
    except Exception as e:
        session.close()
        raise HTTPException(status_code=500, detail=str(e))

# Get active alerts
@app.get('/alerts')
async def get_alerts(
    device_id: Optional[str] = None,
    hours: int = Query(24, le=168)
):
    session = get_session()
    
    try:
        query = f"""
            SELECT *
            FROM sensor_alerts
            WHERE created_at >= DATEADD(hour, -{hours}, CURRENT_TIMESTAMP())
        """
        
        if device_id:
            query += f" AND device_id = '{device_id}'"
        
        query += " ORDER BY created_at DESC"
        
        df = session.sql(query).to_pandas()
        results = df.to_dict('records')
        
        session.close()
        return results
    
    except Exception as e:
        session.close()
        raise HTTPException(status_code=500, detail=str(e))

# Dashboard summary
@app.get('/dashboard/summary')
async def get_dashboard_summary():
    session = get_session()
    
    try:
        # Total readings today
        total = session.sql("""
            SELECT COUNT(*) as count
            FROM raw_sensor_readings
            WHERE DATE(timestamp) = CURRENT_DATE()
        """).to_pandas().iloc[0]['COUNT']
        
        # Active devices
        devices = session.sql("""
            SELECT COUNT(DISTINCT device_id) as count
            FROM raw_sensor_readings
            WHERE timestamp > DATEADD(hour, -1, CURRENT_TIMESTAMP())
        """).to_pandas().iloc[0]['COUNT']
        
        # Recent alerts
        alerts = session.sql("""
            SELECT COUNT(*) as count
            FROM sensor_alerts
            WHERE created_at > DATEADD(hour, -24, CURRENT_TIMESTAMP())
        """).to_pandas().iloc[0]['COUNT']
        
        # Average values
        averages = session.sql("""
            SELECT 
                sensor_type,
                AVG(value) as avg_value
            FROM raw_sensor_readings
            WHERE timestamp > DATEADD(hour, -1, CURRENT_TIMESTAMP())
            GROUP BY sensor_type
        """).to_pandas().to_dict('records')
        
        session.close()
        
        return {
            'total_readings_today': int(total),
            'active_devices': int(devices),
            'alerts_24h': int(alerts),
            'current_averages': averages,
            'timestamp': datetime.utcnow().isoformat()
        }
    
    except Exception as e:
        session.close()
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == '__main__':
    import uvicorn
    uvicorn.run(app, host='0.0.0.0', port=8001)