from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, validator
from typing import List, Optional
from datetime import datetime
import redis
import json
import logging
from kafka import KafkaProducer
import os

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="IoT Ingestion API", version="1.0.0")

# Redis connection (will fail gracefully if Redis not available)
try:
    redis_client = redis.Redis(
        host=os.getenv('REDIS_HOST', 'localhost'),
        port=int(os.getenv('REDIS_PORT', 6379)),
        decode_responses=True,
        socket_connect_timeout=5
    )
    redis_client.ping()
    logger.info("✅ Redis connected")
except Exception as e:
    logger.warning(f"⚠️ Redis not available: {e}")
    redis_client = None

try:
    kafka_producer = KafkaProducer(
        bootstrap_servers=os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092'),
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        acks='all',
        retries=3
    )
    logger.info("✅ Kafka connected")
except Exception as e:
    logger.warning(f"⚠️ Kafka not available: {e}")
    kafka_producer = None

# Data models
class SensorReading(BaseModel):
    device_id: str
    sensor_type: str  # temperature, humidity, pressure
    value: float
    unit: str
    timestamp: datetime
    location: Optional[dict] = None
    
    @validator('sensor_type')
    def validate_sensor_type(cls, v):
        allowed = ['temperature', 'humidity', 'pressure', 'motion']
        if v not in allowed:
            raise ValueError(f'sensor_type must be one of {allowed}')
        return v
    
    @validator('value')
    def validate_value(cls, v, values):
        sensor_type = values.get('sensor_type')
        if sensor_type == 'temperature' and not -50 <= v <= 150:
            raise ValueError('Temperature must be between -50 and 150')
        if sensor_type == 'humidity' and not 0 <= v <= 100:
            raise ValueError('Humidity must be between 0 and 100')
        return v

class BatchSensorReadings(BaseModel):
    readings: List[SensorReading]

# Health check
@app.get('/')
async def root():
    return {
        'service': 'IoT Ingestion API',
        'status': 'running',
        'version': '1.0.0'
    }

@app.get('/health')
async def health_check():
    redis_status = 'connected' if redis_client else 'disconnected'
    
    return {
        'status': 'healthy',
        'timestamp': datetime.utcnow().isoformat(),
        'services': {
            'redis': redis_status
        }
    }

# Ingest single reading
@app.post('/ingest')
async def ingest_reading(reading: SensorReading):
    try:
        reading_id = f"{reading.device_id}_{int(reading.timestamp.timestamp())}"
        
        # Cache in Redis if available
        if redis_client:
            cache_key = f"reading:{reading.device_id}:latest"
            redis_client.setex(
                cache_key,
                3600,
                json.dumps(reading.dict(), default=str)
            )
            redis_client.incr(f"metrics:ingested:{reading.sensor_type}")
        
        # Send to Kafka for processing
        if kafka_producer:
            # Convert reading to dict with string timestamps
            reading_data = reading.dict()
            reading_data['timestamp'] = reading.timestamp.isoformat()
            
            event = {
                'id': reading_id,
                'data': reading_data,
                'event_type': 'sensor_reading',
                'ingestion_time': datetime.utcnow().isoformat()
            }
            future = kafka_producer.send('sensor-readings', event)
            future.get(timeout=10)
            logger.info(f"Sent to Kafka: {reading_id}")
        
        logger.info(f"Ingested reading: {reading_id}")
        
        return {
            'status': 'success',
            'reading_id': reading_id,
            'message': 'Reading received and queued for processing'
        }
    
    except Exception as e:
        logger.error(f"Ingestion error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))
# Batch ingest
@app.post('/ingest/batch')
async def ingest_batch(batch: BatchSensorReadings):
    try:
        success_count = 0
        failed = []
        
        for reading in batch.readings:
            try:
                reading_id = f"{reading.device_id}_{int(reading.timestamp.timestamp())}"
                
                if redis_client:
                    cache_key = f"reading:{reading.device_id}:latest"
                    redis_client.setex(
                        cache_key,
                        3600,
                        json.dumps(reading.dict(), default=str)
                    )
                
                success_count += 1
            except Exception as e:
                failed.append({
                    'device_id': reading.device_id,
                    'error': str(e)
                })
        
        return {
            'status': 'completed',
            'total': len(batch.readings),
            'success': success_count,
            'failed': len(failed),
            'failures': failed
        }
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Get latest reading from cache
@app.get('/device/{device_id}/latest')
async def get_latest_reading(device_id: str):
    if not redis_client:
        raise HTTPException(status_code=503, detail='Redis not available')
    
    cache_key = f"reading:{device_id}:latest"
    cached = redis_client.get(cache_key)
    
    if not cached:
        raise HTTPException(status_code=404, detail='No recent data for device')
    
    return json.loads(cached)

# Metrics endpoint
@app.get('/metrics')
async def get_metrics():
    if not redis_client:
        return {'message': 'Redis not available'}
    
    keys = redis_client.keys('metrics:ingested:*')
    metrics = {}
    
    for key in keys:
        sensor_type = key.split(':')[-1]
        count = redis_client.get(key)
        metrics[sensor_type] = int(count) if count else 0
    
    return {
        'total_ingested': sum(metrics.values()),
        'by_sensor_type': metrics,
        'timestamp': datetime.utcnow().isoformat()
    }

if __name__ == '__main__':
    import uvicorn
    uvicorn.run(app, host='0.0.0.0', port=8000)