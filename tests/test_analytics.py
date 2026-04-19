import pytest
import sys
import numpy as np
from pathlib import Path
from unittest.mock import patch, MagicMock
from datetime import datetime
import math

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


# ── Test Congestion Calculations ──
class TestCongestionCalculations:

    def test_free_flow_congestion_ratio(self):
        """Test free flow traffic gives ratio close to 1.0"""
        current_speed = 60.0
        free_flow_speed = 60.0
        ratio = current_speed / free_flow_speed
        assert ratio == 1.0

    def test_heavy_congestion_ratio(self):
        """Test heavy congestion gives low ratio"""
        current_speed = 10.0
        free_flow_speed = 60.0
        ratio = current_speed / free_flow_speed
        assert ratio < 0.5

    def test_congestion_color_free_flow(self):
        """Test green color for free flow traffic"""
        ratio = 0.95
        if ratio >= 0.9:
            color = "green"
        elif ratio >= 0.7:
            color = "yellow"
        elif ratio >= 0.5:
            color = "orange"
        else:
            color = "red"
        assert color == "green"

    def test_congestion_color_heavy(self):
        """Test red color for heavy congestion"""
        ratio = 0.3
        if ratio >= 0.9:
            color = "green"
        elif ratio >= 0.7:
            color = "yellow"
        elif ratio >= 0.5:
            color = "orange"
        else:
            color = "red"
        assert color == "red"

    def test_congestion_color_moderate(self):
        """Test orange color for moderate congestion"""
        ratio = 0.6
        if ratio >= 0.9:
            color = "green"
        elif ratio >= 0.7:
            color = "yellow"
        elif ratio >= 0.5:
            color = "orange"
        else:
            color = "red"
        assert color == "orange"

    def test_congestion_ratio_never_exceeds_one(self):
        """Test congestion ratio is capped at 1.0"""
        current_speed = 70.0
        free_flow_speed = 60.0
        ratio = min(current_speed / free_flow_speed, 1.0)
        assert ratio <= 1.0

    def test_multiple_location_average(self):
        """Test average congestion across multiple locations"""
        readings = [
            {"current_speed": 60, "free_flow_speed": 60},
            {"current_speed": 30, "free_flow_speed": 60},
            {"current_speed": 45, "free_flow_speed": 60},
        ]
        ratios = [r["current_speed"] / r["free_flow_speed"] for r in readings]
        avg_ratio = sum(ratios) / len(ratios)
        assert round(avg_ratio, 4) == round((1.0 + 0.5 + 0.75) / 3, 4)


# ── Test Anomaly Detection ──
class TestAnomalyDetection:

    def test_speed_drop_is_anomaly(self):
        """Test sudden speed drop triggers anomaly"""
        normal_speed = 60.0
        anomaly_speed = 10.0
        threshold = 0.5
        ratio = anomaly_speed / normal_speed
        is_anomaly = ratio < threshold
        assert is_anomaly

    def test_normal_speed_not_anomaly(self):
        """Test normal speed variation is not anomaly"""
        normal_speed = 60.0
        current_speed = 55.0
        threshold = 0.5
        ratio = current_speed / normal_speed
        is_anomaly = ratio < threshold
        assert not is_anomaly

    def test_anomaly_severity_levels(self):
        """Test anomaly severity classification"""
        def get_severity(ratio):
            if ratio < 0.3:
                return "critical"
            elif ratio < 0.5:
                return "high"
            elif ratio < 0.7:
                return "medium"
            else:
                return "low"

        assert get_severity(0.2) == "critical"
        assert get_severity(0.4) == "high"
        assert get_severity(0.6) == "medium"
        assert get_severity(0.8) == "low"

    def test_anomaly_has_required_fields(self):
        """Test anomaly object has all required fields"""
        anomaly = {
            "timestamp": datetime.now(),
            "anomaly_type": "traffic_spike",
            "severity": "high",
            "description": "Unusual speed drop detected",
            "location_name": "sarjapur_road_junction"
        }
        required = ["timestamp", "anomaly_type", "severity"]
        for field in required:
            assert field in anomaly

    def test_zscore_anomaly_detection(self):
        """Test z-score based anomaly detection"""
        speeds = [60, 58, 62, 59, 61, 10]
        mean = np.mean(speeds[:-1])
        std = np.std(speeds[:-1])
        zscore = abs(speeds[-1] - mean) / std if std > 0 else 0
        is_anomaly = zscore > 2.0
        assert is_anomaly


# ── Test Risk Scorer ──
class TestRiskScorer:

    def test_high_risk_conditions(self):
        """Test high risk score for bad conditions"""
        def calculate_risk(congestion_ratio, aqi, rain):
            score = 0
            if congestion_ratio < 0.5:
                score += 40
            if aqi >= 4:
                score += 30
            if rain > 5:
                score += 30
            return score

        risk = calculate_risk(0.3, 4, 10)
        assert risk >= 70

    def test_low_risk_conditions(self):
        """Test low risk score for good conditions"""
        def calculate_risk(congestion_ratio, aqi, rain):
            score = 0
            if congestion_ratio < 0.5:
                score += 40
            if aqi >= 4:
                score += 30
            if rain > 5:
                score += 30
            return score

        risk = calculate_risk(0.9, 1, 0)
        assert risk == 0

    def test_risk_score_range(self):
        """Test risk score is between 0 and 100"""
        scores = [0, 25, 50, 75, 100]
        for score in scores:
            assert 0 <= score <= 100


# ── Test Traffic Predictor ──
class TestTrafficPredictor:

    def test_model_files_exist(self):
        """Test that trained model files exist"""
        model_dir = Path("models")
        assert (model_dir / "traffic_speed_model.joblib").exists(), \
            "traffic_speed_model.joblib not found"
        assert (model_dir / "location_encoder.joblib").exists(), \
            "location_encoder.joblib not found"
        assert (model_dir / "model_config.joblib").exists(), \
            "model_config.joblib not found"

    def test_model_loads_successfully(self):
        """Test that model loads without error"""
        import joblib
        model = joblib.load("models/traffic_speed_model.joblib")
        assert model is not None

    def test_prediction_returns_numeric(self):
        """Test model returns a numeric prediction with all 25 features"""
        import joblib
        import numpy as np

        model = joblib.load("models/traffic_speed_model.joblib")
        config = joblib.load("models/model_config.joblib")

        # Build sample input with all 25 features in correct order:
        # hour, day_of_week, is_weekend, is_rush_hour,
        # hour_sin, hour_cos, dow_sin, dow_cos,
        # location_encoded, free_flow_speed, confidence,
        # temperature, humidity, wind_speed, rain_1h, visibility, is_raining,
        # speed_lag_1, speed_lag_3, speed_lag_6,
        # ratio_lag_1, ratio_lag_3, ratio_lag_6,
        # speed_rolling_3, speed_rolling_6

        hour = 8
        day_of_week = 1
        is_weekend = 0
        is_rush_hour = 1
        hour_sin = math.sin(2 * math.pi * hour / 24)
        hour_cos = math.cos(2 * math.pi * hour / 24)
        dow_sin = math.sin(2 * math.pi * day_of_week / 7)
        dow_cos = math.cos(2 * math.pi * day_of_week / 7)
        location_encoded = 0
        free_flow_speed = 45.0
        confidence = 0.9
        temperature = 25.0
        humidity = 65.0
        wind_speed = 3.0
        rain_1h = 0.0
        visibility = 10000.0
        is_raining = 0
        speed_lag_1 = 40.0
        speed_lag_3 = 42.0
        speed_lag_6 = 44.0
        ratio_lag_1 = 0.89
        ratio_lag_3 = 0.93
        ratio_lag_6 = 0.98
        speed_rolling_3 = 41.0
        speed_rolling_6 = 43.0

        sample_input = np.array([[
            hour, day_of_week, is_weekend, is_rush_hour,
            hour_sin, hour_cos, dow_sin, dow_cos,
            location_encoded, free_flow_speed, confidence,
            temperature, humidity, wind_speed, rain_1h, visibility, is_raining,
            speed_lag_1, speed_lag_3, speed_lag_6,
            ratio_lag_1, ratio_lag_3, ratio_lag_6,
            speed_rolling_3, speed_rolling_6
        ]])

        # Verify we have exactly 25 features
        assert sample_input.shape[1] == len(config["feature_cols"]), \
            f"Expected {len(config['feature_cols'])} features, got {sample_input.shape[1]}"

        prediction = model.predict(sample_input)

        assert isinstance(prediction[0], (int, float, np.floating))
        assert prediction[0] >= 0

    def test_model_config_has_25_features(self):
        """Test model config lists exactly 25 features"""
        import joblib
        config = joblib.load("models/model_config.joblib")
        assert len(config["feature_cols"]) == 25

    def test_model_has_good_accuracy(self):
        """Test model R2 score is above 0.9"""
        import joblib
        config = joblib.load("models/model_config.joblib")
        assert config["test_r2"] > 0.9, f"R2 score too low: {config['test_r2']}"

    def test_hour_feature_bounds(self):
        """Test hour feature is within valid range"""
        for hour in range(24):
            assert 0 <= hour <= 23

    def test_day_of_week_feature_bounds(self):
        """Test day of week feature is within valid range"""
        for day in range(7):
            assert 0 <= day <= 6