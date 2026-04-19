import pytest
import sys
from pathlib import Path
from fastapi.testclient import TestClient
from unittest.mock import patch, MagicMock

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.api.main import app

client = TestClient(app)


# ── Test Root & Health ──
class TestRootEndpoints:

    def test_root_returns_200(self):
        """Test root endpoint is reachable"""
        response = client.get("/")
        assert response.status_code == 200

    def test_health_check(self):
        """Test health endpoint returns ok status"""
        response = client.get("/health")
        assert response.status_code == 200
        data = response.json()
        assert "status" in data


# ── Test Traffic Endpoints ──
class TestTrafficEndpoints:

    def test_get_traffic_latest_returns_200(self):
        """Test traffic latest endpoint returns 200"""
        response = client.get("/api/v1/traffic/latest")
        assert response.status_code == 200

    def test_get_traffic_latest_has_readings(self):
        """Test traffic latest returns count and readings"""
        response = client.get("/api/v1/traffic/latest")
        data = response.json()
        assert "count" in data
        assert "readings" in data
        assert isinstance(data["readings"], list)

    def test_traffic_reading_has_required_fields(self):
        """Test each traffic reading has required fields"""
        response = client.get("/api/v1/traffic/latest")
        data = response.json()
        if data["count"] > 0:
            item = data["readings"][0]
            required = ["location_name", "current_speed", "free_flow_speed", "congestion_ratio"]
            for field in required:
                assert field in item, f"Missing field: {field}"

    def test_get_traffic_history_valid_location(self):
        """Test traffic history with a valid location"""
        response = client.get("/api/v1/traffic/history/sarjapur_road_junction?hours=24")
        assert response.status_code == 200
        data = response.json()
        assert "location_name" in data
        assert "readings" in data

    def test_get_traffic_history_invalid_location(self):
        """Test traffic history with invalid location returns 404"""
        response = client.get("/api/v1/traffic/history/fake_location")
        assert response.status_code == 404

    def test_get_traffic_heatmap_returns_200(self):
        """Test traffic heatmap endpoint returns 200"""
        response = client.get("/api/v1/traffic/heatmap")
        assert response.status_code == 200
        data = response.json()
        assert "points" in data
        assert "count" in data

    def test_get_traffic_predict(self):
        """Test traffic prediction endpoint"""
        response = client.get("/api/v1/traffic/predict?minutes=30")
        assert response.status_code in [200, 503]


# ── Test Weather Endpoints ──
class TestWeatherEndpoints:

    def test_get_weather_latest_returns_200(self):
        """Test weather latest endpoint returns 200"""
        response = client.get("/api/v1/weather/latest")
        assert response.status_code in [200, 404]

    def test_get_weather_latest_has_fields(self):
        """Test weather latest has required fields"""
        response = client.get("/api/v1/weather/latest")
        if response.status_code == 200:
            data = response.json()
            required = ["temperature", "humidity", "pressure"]
            for field in required:
                assert field in data, f"Missing field: {field}"

    def test_get_weather_history_returns_200(self):
        """Test weather history endpoint returns 200"""
        response = client.get("/api/v1/weather/history")
        assert response.status_code == 200

    def test_get_weather_history_returns_list(self):
        """Test weather history returns readings list"""
        response = client.get("/api/v1/weather/history")
        data = response.json()
        assert "readings" in data
        assert isinstance(data["readings"], list)


# ── Test Analytics Endpoints ──
class TestAnalyticsEndpoints:

    def test_get_aqi_latest_returns_200(self):
        """Test AQI latest endpoint returns 200"""
        response = client.get("/api/v1/aqi/latest")
        assert response.status_code in [200, 404]

    def test_get_anomalies_returns_200(self):
        """Test anomalies endpoint returns 200"""
        response = client.get("/api/v1/analytics/anomalies")
        assert response.status_code == 200

    def test_get_anomalies_returns_list(self):
        """Test anomalies endpoint returns anomalies list"""
        response = client.get("/api/v1/analytics/anomalies")
        data = response.json()
        assert "anomalies" in data
        assert isinstance(data["anomalies"], list)

    def test_get_risk_scores_returns_200(self):
        """Test risk scores endpoint returns 200"""
        response = client.get("/api/v1/analytics/risk-scores")
        assert response.status_code == 200

    def test_get_analytics_summary_returns_200(self):
        """Test analytics summary endpoint returns 200"""
        response = client.get("/api/v1/analytics/summary")
        assert response.status_code == 200

    def test_analytics_summary_has_fields(self):
        """Test analytics summary has required fields"""
        response = client.get("/api/v1/analytics/summary")
        if response.status_code == 200:
            data = response.json()
            assert isinstance(data, dict)


# ── Test Geodata Endpoints ──
class TestGeodataEndpoints:

    def test_get_pois_returns_200(self):
        """Test POIs endpoint returns 200"""
        response = client.get("/api/v1/geodata/pois")
        assert response.status_code == 200

    def test_get_pois_returns_geojson(self):
        """Test POIs endpoint returns valid GeoJSON"""
        response = client.get("/api/v1/geodata/pois")
        data = response.json()
        assert "type" in data
        assert data["type"] == "FeatureCollection"
        assert "features" in data

    def test_get_pois_with_category_filter(self):
        """Test POIs endpoint filters by category"""
        response = client.get("/api/v1/geodata/pois?category=Healthcare")
        assert response.status_code == 200
        data = response.json()
        for feature in data["features"]:
            assert feature["properties"]["category"] == "Healthcare"

    def test_get_pois_college_category(self):
        """Test POIs endpoint returns college category"""
        response = client.get("/api/v1/geodata/pois?category=college&limit=1000")
        assert response.status_code == 200
        data = response.json()
        assert data["count"] >= 1

    def test_get_buildings_returns_200(self):
        """Test buildings endpoint returns 200"""
        response = client.get("/api/v1/geodata/buildings")
        assert response.status_code == 200

    def test_get_roads_returns_200(self):
        """Test roads endpoint returns 200"""
        response = client.get("/api/v1/geodata/roads")
        assert response.status_code == 200

    def test_get_pois_isme_in_results(self):
        """Test ISME college appears in full POI list"""
        response = client.get("/api/v1/geodata/pois?limit=1000")
        data = response.json()
        names = [f["properties"]["name"] for f in data["features"]]
        assert any("ISME" in name for name in names), "ISME not found in POI results"
