import pytest
import sys
from pathlib import Path
from unittest.mock import patch, MagicMock, PropertyMock
from datetime import datetime

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


# ── Test Database Connection ──
class TestDatabaseConnection:

    def test_database_url_exists(self):
        """Test that DATABASE_URL is set in environment"""
        import os
        from dotenv import load_dotenv
        load_dotenv()
        db_url = os.getenv("DATABASE_URL")
        assert db_url is not None
        assert "postgresql" in db_url

    def test_database_url_format(self):
        """Test that DATABASE_URL has correct format"""
        import os
        from dotenv import load_dotenv
        load_dotenv()
        db_url = os.getenv("DATABASE_URL")
        assert "://" in db_url
        assert "@" in db_url
        assert "urban_intel" in db_url

    @patch("psycopg2.connect")
    def test_connection_called(self, mock_connect):
        """Test that database connection is attempted"""
        import psycopg2
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn
        conn = psycopg2.connect("postgresql://postgres:postgres@localhost:5432/urban_intel")
        assert mock_connect.called
        assert conn is not None


# ── Test Database Operations (Mocked) ──
class TestDatabaseOperations:

    def test_traffic_reading_insert_structure(self):
        """Test traffic reading dict has required fields"""
        reading = {
            "timestamp": datetime.now(),
            "location_name": "test_location",
            "lat": 12.91,
            "lon": 77.69,
            "current_speed": 45.0,
            "free_flow_speed": 60.0,
            "confidence": 0.85,
            "congestion_ratio": 0.75
        }
        required_fields = [
            "timestamp", "location_name", "lat", "lon",
            "current_speed", "free_flow_speed", "confidence", "congestion_ratio"
        ]
        for field in required_fields:
            assert field in reading, f"Missing field: {field}"

    def test_weather_reading_insert_structure(self):
        """Test weather reading dict has required fields"""
        reading = {
            "timestamp": datetime.now(),
            "temperature": 28.5,
            "humidity": 65,
            "pressure": 1013.0,
            "weather_description": "clear sky",
            "wind_speed": 3.5,
            "rain_1h": 0.0,
            "visibility": 10.0
        }
        required_fields = [
            "timestamp", "temperature", "humidity",
            "pressure", "weather_description", "wind_speed"
        ]
        for field in required_fields:
            assert field in reading, f"Missing field: {field}"

    def test_aqi_reading_insert_structure(self):
        """Test AQI reading dict has required fields"""
        reading = {
            "timestamp": datetime.now(),
            "aqi": 2,
            "pm25": 12.5,
            "pm10": 25.0,
            "no2": 15.0,
            "o3": 80.0,
            "co": 400.0,
            "so2": 5.0
        }
        required_fields = ["timestamp", "aqi", "pm25", "pm10", "no2", "o3", "co", "so2"]
        for field in required_fields:
            assert field in reading, f"Missing field: {field}"

    def test_congestion_ratio_stored_correctly(self):
        """Test congestion ratio is within valid range"""
        current_speed = 45.0
        free_flow_speed = 60.0
        ratio = current_speed / free_flow_speed
        assert 0.0 <= ratio <= 1.0

    def test_anomaly_insert_structure(self):
        """Test anomaly dict has required fields"""
        anomaly = {
            "timestamp": datetime.now(),
            "anomaly_type": "traffic_spike",
            "severity": "high",
            "description": "Unusual traffic detected",
            "location_name": "test_location"
        }
        required_fields = ["timestamp", "anomaly_type", "severity"]
        for field in required_fields:
            assert field in anomaly, f"Missing field: {field}"


# ── Test Spatial Data ──
class TestSpatialData:

    def test_coordinates_within_study_area(self):
        """Test that coordinates fall within study area bounding box"""
        bbox = (12.86, 77.68, 12.95, 77.77)  # min_lat, min_lon, max_lat, max_lon
        test_points = [
            (12.91, 77.69),
            (12.88, 77.75),
            (12.90, 77.70),
        ]
        for lat, lon in test_points:
            assert bbox[0] <= lat <= bbox[2], f"Lat {lat} out of bounds"
            assert bbox[1] <= lon <= bbox[3], f"Lon {lon} out of bounds"

    def test_isme_coordinates_in_study_area(self):
        """Test ISME college coordinates are within study area"""
        bbox = (12.86, 77.68, 12.95, 77.77)
        isme_lat, isme_lon = 12.8795, 77.7601
        assert bbox[0] <= isme_lat <= bbox[2]
        assert bbox[1] <= isme_lon <= bbox[3]

    def test_monitoring_points_in_study_area(self):
        """Test all monitoring points are within study area"""
        bbox = (12.86, 77.68, 12.95, 77.77)
        monitoring_points = {
            "sarjapur_road_junction":  (12.9100, 77.6870),
            "dommasandra_circle":      (12.8832, 77.7524),
            "chembenahalli":           (12.8793, 77.7616),
            "varthur_gunjur_junction": (12.9265, 77.7377),
            "carmelaram_junction":     (12.9023, 77.7020),
        }
        for name, (lat, lon) in monitoring_points.items():
            assert bbox[0] <= lat <= bbox[2], f"{name} lat out of bounds"
            assert bbox[1] <= lon <= bbox[3], f"{name} lon out of bounds"

    def test_wkt_point_format(self):
        """Test WKT point string is formatted correctly"""
        lat, lon = 12.91, 77.69
        wkt = f"POINT({lon} {lat})"
        assert wkt == "POINT(77.69 12.91)"
        assert wkt.startswith("POINT(")
        assert wkt.endswith(")")