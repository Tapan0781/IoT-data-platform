import requests
import json
from datetime import datetime

# Test data
reading = {
    'device_id': 'sensor_001',
    'sensor_type': 'temperature',
    'value': 25.5,
    'unit': 'C',
    'timestamp': datetime.utcnow().isoformat()
}

# Send request
response = requests.post(
    'http://localhost:8000/ingest',
    json=reading
)

print('Status Code:', response.status_code)
print('Response:', json.dumps(response.json(), indent=2))
