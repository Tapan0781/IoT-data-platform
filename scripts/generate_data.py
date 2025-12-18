import requests
import random
import time
from datetime import datetime
import json

# API endpoint
API_URL = 'http://localhost:8000'

# Device configurations
DEVICES = [
    {'id': 'sensor_001', 'type': 'temperature', 'location': {'building': 'A', 'floor': 1}},
    {'id': 'sensor_002', 'type': 'humidity', 'location': {'building': 'A', 'floor': 2}},
    {'id': 'sensor_003', 'type': 'pressure', 'location': {'building': 'B', 'floor': 1}},
    {'id': 'sensor_004', 'type': 'temperature', 'location': {'building': 'B', 'floor': 3}},
]

def generate_reading(device):
    """Generate realistic sensor reading"""
    
    # Base values with realistic ranges
    base_values = {
        'temperature': random.uniform(18, 25),
        'humidity': random.uniform(30, 60),
        'pressure': random.uniform(1000, 1020)
    }
    
    # Add some variation
    value = base_values[device['type']] + random.uniform(-5, 5)
    
    # Occasionally generate anomalies (5% chance)
    if random.random() < 0.05:
        value += random.uniform(50, 100)  # Spike
    
    return {
        'device_id': device['id'],
        'sensor_type': device['type'],
        'value': round(value, 2),
        'unit': 'C' if device['type'] == 'temperature' else '%' if device['type'] == 'humidity' else 'hPa',
        'timestamp': datetime.utcnow().isoformat(),
        'location': device['location']
    }

def send_reading(reading):
    """Send reading to ingestion API"""
    try:
        response = requests.post(
            f'{API_URL}/ingest',
            json=reading,
            timeout=5
        )
        if response.status_code == 200:
            print(f"✓ Sent: {reading['device_id']} - {reading['value']} {reading['unit']}")
        else:
            print(f"✗ Failed: {response.status_code}")
    except Exception as e:
        print(f"✗ Error: {str(e)}")

def continuous_generation(interval=2):
    """Generate data continuously"""
    print('Starting continuous data generation...')
    print(f'Sending readings every {interval} seconds')
    print(f'Devices: {len(DEVICES)}')
    print('Press Ctrl+C to stop')
    print('-' * 50)
    
    try:
        while True:
            for device in DEVICES:
                reading = generate_reading(device)
                send_reading(reading)
            
            time.sleep(interval)
    except KeyboardInterrupt:
        print('\n\nStopped data generation.')

if __name__ == '__main__':
    import sys
    
    # Check if API is running
    try:
        response = requests.get(f'{API_URL}/health', timeout=10)
        print(f'✅ API is running at {API_URL}')
        print('-' * 50)
    except:
        print(f'❌ API is not running at {API_URL}')
        print('Please start the ingestion API first:')
        print('  cd services/ingestion')
        print('  python main.py')
        sys.exit(1)
    
    continuous_generation()