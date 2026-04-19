import pytest
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

@pytest.fixture
def sample_traffic_response():
    return {
        "flowSegmentData": {
            "currentSpeed": 45,
            "freeFlowSpeed": 60,
            "confidence": 0.85,
            "coordinates": {
                "coordinate": [{"latitude": 12.91, "longitude": 77.69}]
            }
        }
    }

@pytest.fixture
def sample_weather_response():
    return {
        "main": {
            "temp": 28.5,
            "humidity": 65,
            "pressure": 1013
        },
        "weather": [{"description": "clear sky"}],
        "wind": {"speed": 3.5},
        "rain": {},
        "visibility": 10000
    }

@pytest.fixture
def sample_aqi_response():
    return {
        "list": [{
            "main": {"aqi": 2},
            "components": {
                "pm2_5": 12.5,
                "pm10": 25.0,
                "no2": 15.0,
                "o3": 80.0,
                "co": 400.0,
                "so2": 5.0
            }
        }]
    }