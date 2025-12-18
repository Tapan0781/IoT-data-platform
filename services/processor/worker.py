from celery import Celery
from snowflake.snowpark import Session
from snowflake.snowpark.types import StructType, StructField, StringType, DoubleType
import json
import logging
import os
from dotenv import load_dotenv
from kafka import KafkaConsumer
from datetime import datetime
import time

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Celery app
celery_app = Celery(
    'iot_processor',
    broker=f"redis://{os.getenv('REDIS_HOST', 'localhost')}:{os.getenv('REDIS_PORT', 6379)}/0",
    backend=f"redis://{os.getenv('REDIS_HOST', 'localhost')}:{os.getenv('REDIS_PORT', 6379)}/1"
)

celery_app.conf.update(
    task_serializer='json',
    result_serializer='json',
    accept_content=['json'],
    timezone='UTC',
    enable_utc=True,
)

# Snowflake session
def create_snowflake_session():
    return Session.builder.configs({
        'account': os.getenv('SNOWFLAKE_ACCOUNT'),
        'user': os.getenv('SNOWFLAKE_USER'),
        'password': os.getenv('SNOWFLAKE_PASSWORD'),
        'role': os.getenv('SNOWFLAKE_ROLE'),
        'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE'),
        'database': os.getenv('SNOWFLAKE_DATABASE'),
        'schema': os.getenv('SNOWFLAKE_SCHEMA')
    }).create()

# Process reading task
@celery_app.task(bind=True, max_retries=3)
def process_reading(self, event_data):
    """Process sensor reading and store in Snowflake"""
    session = create_snowflake_session()
    
    try:
        reading = event_data['data']
        reading_id = event_data['id']
        
        # Parse timestamp
        ts = reading['timestamp']
        if isinstance(ts, str):
            ts = ts.replace('T', ' ').split('.')[0]
        
        # Insert into Snowflake
        query = f"""
            INSERT INTO raw_sensor_readings 
            (reading_id, device_id, sensor_type, value, unit, timestamp, location)
            VALUES (
                '{reading_id}',
                '{reading['device_id']}',
                '{reading['sensor_type']}',
                {reading['value']},
                '{reading['unit']}',
                '{ts}',
                PARSE_JSON('{json.dumps(reading.get('location', {}))}')
            )
        """
        
        session.sql(query).collect()
        
        # Check for alerts
        alert = check_alert(session, reading, reading_id)
        
        logger.info(f"✅ Processed: {reading_id}, Alert: {alert}")
        return {'status': 'success', 'reading_id': reading_id, 'alert': alert}
    
    except Exception as e:
        logger.error(f"❌ Processing error: {e}")
        raise self.retry(exc=e, countdown=60 * (2 ** self.request.retries))
    
    finally:
        session.close()

def process_reading_sync(event_data):
    """Process sensor reading synchronously"""
    session = create_snowflake_session()
    
    try:
        reading = event_data['data']
        reading_id = event_data['id']
        
        # Parse timestamp
        ts = reading['timestamp']
        if isinstance(ts, str):
            ts = ts.replace('T', ' ').split('.')[0]
        
        # Simpler approach: Just insert without the auto-generated column
        from snowflake.snowpark.functions import parse_json, lit
        
        # Create a simple list
        location_str = json.dumps(reading.get('location')) if reading.get('location') else '{}'
        
        # Use SQL with proper column specification
        query = f"""
            INSERT INTO raw_sensor_readings 
            (reading_id, device_id, sensor_type, value, unit, timestamp, location)
            SELECT 
                '{reading_id}',
                '{reading['device_id']}',
                '{reading['sensor_type']}',
                {reading['value']},
                '{reading['unit']}',
                TO_TIMESTAMP_NTZ('{ts}'),
                PARSE_JSON('{location_str.replace("'", "''")}')
        """
        
        session.sql(query).collect()
        
        # Check for alerts
        alert = check_alert(session, reading, reading_id)
        
        logger.info(f"✅ Processed: {reading_id}, Alert: {alert}")
        return {'status': 'success', 'reading_id': reading_id, 'alert': alert}
    
    except Exception as e:
        logger.error(f"❌ Processing error: {e}")
        logger.error(f"Event data: {event_data}")
        return {'status': 'error', 'error': str(e)}
    
    finally:
        session.close()
def check_alert(session, reading, reading_id):
    """Check if reading triggers an alert"""
    thresholds = {
        'temperature': {'high': 100, 'low': -20},
        'humidity': {'high': 90, 'low': 10},
        'pressure': {'high': 1050, 'low': 950}
    }
    
    sensor_type = reading['sensor_type']
    value = reading['value']
    
    if sensor_type not in thresholds:
        return False
    
    alert_type = None
    threshold = None
    
    if value > thresholds[sensor_type]['high']:
        alert_type = 'HIGH'
        threshold = thresholds[sensor_type]['high']
    elif value < thresholds[sensor_type]['low']:
        alert_type = 'LOW'
        threshold = thresholds[sensor_type]['low']
    
    if alert_type:
        ts = reading['timestamp']
        if isinstance(ts, str):
            ts = ts.replace('T', ' ').split('.')[0]
        
        alert_id = f"{reading['device_id']}_{int(datetime.utcnow().timestamp())}"
        query = f"""
            INSERT INTO sensor_alerts
            (alert_id, device_id, sensor_type, alert_type, message, value, threshold, timestamp)
            VALUES (
                '{alert_id}',
                '{reading['device_id']}',
                '{sensor_type}',
                '{alert_type}',
                '{sensor_type} {alert_type}: {value} {reading["unit"]}',
                {value},
                {threshold},
                '{ts}'
            )
        """
        session.sql(query).collect()
        logger.warning(f"⚠️ Alert triggered: {alert_id}")
        return True
    
    return False

# Kafka consumer function
def start_kafka_consumer():
    """Consume messages from Kafka and trigger Celery tasks"""
    logger.info("Starting Kafka consumer...")
    
    consumer = KafkaConsumer(
        'sensor-readings',
        bootstrap_servers=os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092'),
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        group_id='processor-group',
        auto_offset_reset='earliest',
        enable_auto_commit=True
    )
    
    logger.info("✅ Kafka consumer started")
    
    for message in consumer:
        try:
            event = message.value
            logger.info(f"Received: {event['id']}")
            
            # Trigger Celery task
            result = process_reading_sync(event)
            logger.info(f"Result: {result}")
        
        except Exception as e:
            logger.error(f"Consumer error: {e}")

# Aggregation task (run hourly)
@celery_app.task
def aggregate_hourly_data():
    """Aggregate sensor data by hour"""
    session = create_snowflake_session()
    
    try:
        query = """
            INSERT INTO sensor_aggregates 
            (device_id, sensor_type, date, hour, avg_value, min_value, max_value, reading_count)
            SELECT 
                device_id,
                sensor_type,
                DATE(timestamp) as date,
                HOUR(timestamp) as hour,
                AVG(value) as avg_value,
                MIN(value) as min_value,
                MAX(value) as max_value,
                COUNT(*) as reading_count
            FROM raw_sensor_readings
            WHERE timestamp >= DATEADD(hour, -1, CURRENT_TIMESTAMP())
            GROUP BY device_id, sensor_type, DATE(timestamp), HOUR(timestamp)
        """
        
        result = session.sql(query).collect()
        logger.info(f"✅ Aggregated hourly data")
        return {'status': 'success'}
    
    except Exception as e:
        logger.error(f"❌ Aggregation error: {e}")
        raise
    finally:
        session.close()

# Celery Beat schedule
celery_app.conf.beat_schedule = {
    'aggregate-hourly': {
        'task': 'worker.aggregate_hourly_data',
        'schedule': 3600.0,  # Every hour
    },
}

if __name__ == '__main__':
    # Test Snowflake connection
    try:
        session = create_snowflake_session()
        result = session.sql('SELECT CURRENT_DATABASE(), CURRENT_USER()').collect()
        logger.info(f"✅ Snowflake connected: {result[0][0]} as {result[0][1]}")
        session.close()
    except Exception as e:
        logger.error(f"❌ Snowflake connection failed: {e}")
    
    # Start Kafka consumer
    start_kafka_consumer()