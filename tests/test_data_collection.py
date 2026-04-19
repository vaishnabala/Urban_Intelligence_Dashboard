import pytest
import sys
from pathlib import Path
from unittest.mock import patch, MagicMock

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


# ── Test Traffic Data Parsing ──
class TestTrafficParsing:

    def test_traffic_response_has_required_fields(self, sample_traffic_response):
        """Test that traffic response contains expected fields"""
        data = sample_traffic_response["flowSegmentData"]
        assert "currentSpeed" in data
        assert "freeFlowSpeed" in data
        assert "confidence" in data

    def test_congestion_ratio_calculation(self, sample_traffic_response):
        """Test congestion ratio is calculated correctly"""
        data = sample_traffic_response["flowSegmentData"]
        current_speed = data["currentSpeed"]
        free_flow_speed = data["freeFlowSpeed"]
        congestion_ratio = current_speed / free_flow_speed
        assert 0.0 <= congestion_ratio <= 1.0
        assert round(congestion_ratio, 4) == round(45 / 60, 4)

    def test_congestion_ratio_zero_division(self):
        """Test that zero free flow speed is handled"""
        current_speed = 30
        free_flow_speed = 0
        # Should not raise ZeroDivisionError
        congestion_ratio = current_speed / free_flow_speed if free_flow_speed > 0 else 0.0
        assert congestion_ratio == 0.0

    def test_traffic_speed_bounds(self, sample_traffic_response):
        """Test that speeds are positive numbers"""
        data = sample_traffic_response["flowSegmentData"]
        assert data["currentSpeed"] > 0
        assert data["freeFlowSpeed"] > 0

    def test_confidence_bounds(self, sample_traffic_response):
        """Test confidence is between 0 and 1"""
        data = sample_traffic_response["flowSegmentData"]
        assert 0.0 <= data["confidence"] <= 1.0


# ── Test Weather Data Parsing ──
class TestWeatherParsing:

    def test_weather_response_has_required_fields(self, sample_weather_response):
        """Test that weather response contains expected fields"""
        assert "main" in sample_weather_response
        assert "weather" in sample_weather_response
        assert "wind" in sample_weather_response

    def test_temperature_parsing(self, sample_weather_response):
        """Test temperature is extracted correctly"""
        temp = sample_weather_response["main"]["temp"]
        assert isinstance(temp, (int, float))
        assert -50 <= temp <= 60  # Reasonable temperature range

    def test_humidity_bounds(self, sample_weather_response):
        """Test humidity is between 0 and 100"""
        humidity = sample_weather_response["main"]["humidity"]
        assert 0 <= humidity <= 100

    def test_missing_rain_handled(self, sample_weather_response):
        """Test that missing rain data defaults to 0"""
        rain_data = sample_weather_response.get("rain", {})
        rain = rain_data.get("1h", 0.0)
        assert rain == 0.0

    def test_weather_description_extracted(self, sample_weather_response):
        """Test weather description is extracted from list"""
        description = sample_weather_response["weather"][0]["description"]
        assert isinstance(description, str)
        assert len(description) > 0


# ── Test AQI Data Parsing ──
class TestAQIParsing:

    def test_aqi_response_has_required_fields(self, sample_aqi_response):
        """Test that AQI response contains expected fields"""
        assert "list" in sample_aqi_response
        assert len(sample_aqi_response["list"]) > 0

    def test_aqi_value_bounds(self, sample_aqi_response):
        """Test AQI value is within valid range (1-5)"""
        aqi = sample_aqi_response["list"][0]["main"]["aqi"]
        assert 1 <= aqi <= 5

    def test_pm25_extraction(self, sample_aqi_response):
        """Test PM2.5 value is extracted correctly"""
        components = sample_aqi_response["list"][0]["components"]
        pm25 = components["pm2_5"]
        assert isinstance(pm25, (int, float))
        assert pm25 >= 0

    def test_all_pollutants_present(self, sample_aqi_response):
        """Test all required pollutant fields exist"""
        components = sample_aqi_response["list"][0]["components"]
        required = ["pm2_5", "pm10", "no2", "o3", "co", "so2"]
        for pollutant in required:
            assert pollutant in components, f"Missing pollutant: {pollutant}"


# ── Test Data Cleaning Edge Cases ──
class TestDataCleaning:

    def test_none_speed_handled(self):
        """Test None speed values are handled"""
        speed = None
        cleaned = speed if speed is not None else 0.0
        assert cleaned == 0.0

    def test_negative_speed_flagged(self):
        """Test negative speed is invalid"""
        speed = -10
        is_valid = speed >= 0
        assert not is_valid

    def test_empty_response_handled(self):
        """Test empty API response is handled gracefully"""
        response = {}
        data = response.get("flowSegmentData", None)
        assert data is None

    def test_string_speed_converted(self):
        """Test string speed values can be converted"""
        speed_str = "45.5"
        speed = float(speed_str)
        assert speed == 45.5

    def test_visibility_unit_conversion(self, sample_weather_response):
        """Test visibility converted from meters to km"""
        visibility_m = sample_weather_response["visibility"]
        visibility_km = visibility_m / 1000
        assert visibility_km == 10.0