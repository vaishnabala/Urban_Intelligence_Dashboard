"""
Traffic speed predictor using trained ML model.
Loads saved model and makes predictions for future timestamps.
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from datetime import datetime, timezone, timedelta
from dataclasses import dataclass
from typing import Optional

import numpy as np
import pandas as pd
import joblib
from sqlalchemy import text
from loguru import logger

from src.database.connection import engine
from src.config.settings import settings


# ================================================================
# PATHS
# ================================================================
MODELS_DIR = settings.PROJECT_ROOT / "models"
MODEL_PATH = MODELS_DIR / "traffic_speed_model.joblib"
ENCODER_PATH = MODELS_DIR / "location_encoder.joblib"
CONFIG_PATH = MODELS_DIR / "model_config.joblib"


# ================================================================
# DATA CLASS
# ================================================================
@dataclass
class TrafficPrediction:
    """Result from traffic speed prediction."""
    location_name: str
    lat: float
    lon: float
    predicted_speed: float
    free_flow_speed: float
    predicted_congestion_ratio: float
    congestion_level: str
    prediction_time: datetime
    confidence_note: str


# ================================================================
# MODEL LOADER (cached)
# ================================================================
_cached_model = None
_cached_encoder = None
_cached_config = None


def _load_model():
    """Load model, encoder, and config. Caches after first load."""
    global _cached_model, _cached_encoder, _cached_config
    
    if _cached_model is not None:
        return _cached_model, _cached_encoder, _cached_config
    
    if not MODEL_PATH.exists():
        raise FileNotFoundError(
            f"Model not found at {MODEL_PATH}. "
            f"Run the training notebook first (notebooks/02_model_training.ipynb)"
        )
    
    _cached_model = joblib.load(MODEL_PATH)
    _cached_encoder = joblib.load(ENCODER_PATH)
    _cached_config = joblib.load(CONFIG_PATH)
    
    logger.info(f"Model loaded: {_cached_config['model_name']} "
                f"(R²={_cached_config['test_r2']:.4f}, "
                f"MAE={_cached_config['test_mae']:.2f})")
    
    return _cached_model, _cached_encoder, _cached_config


def _get_latest_weather() -> dict:
    """Get the most recent weather reading for feature creation."""
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT temperature, humidity, wind_speed, rain_1h, visibility
            FROM weather_readings
            ORDER BY timestamp DESC
            LIMIT 1
        """))
        row = result.fetchone()
    
    if row:
        return {
            "temperature": row[0],
            "humidity": row[1],
            "wind_speed": row[2],
            "rain_1h": row[3] if row[3] else 0,
            "visibility": row[4] if row[4] else 10000,
        }
    else:
        return {
            "temperature": 25.0,
            "humidity": 60.0,
            "wind_speed": 2.0,
            "rain_1h": 0.0,
            "visibility": 10000.0,
        }


def _get_recent_speeds(location_name: str, n: int = 6) -> dict:
    """Get the most recent N speed readings for lag features."""
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT current_speed, congestion_ratio, free_flow_speed
            FROM traffic_readings
            WHERE location_name = :loc
            ORDER BY timestamp DESC
            LIMIT :n
        """), {"loc": location_name, "n": n})
        rows = result.fetchall()
    
    if not rows:
        return {
            "speed_lag_1": 30.0, "speed_lag_3": 30.0, "speed_lag_6": 30.0,
            "ratio_lag_1": 1.5, "ratio_lag_3": 1.5, "ratio_lag_6": 1.5,
            "speed_rolling_3": 30.0, "speed_rolling_6": 30.0,
            "free_flow_speed": 40.0, "confidence": 0.5,
        }
    
    speeds = [r[0] for r in rows]
    ratios = [r[1] for r in rows]
    ff = rows[0][2]
    
    return {
        "speed_lag_1": speeds[0] if len(speeds) > 0 else 30.0,
        "speed_lag_3": speeds[2] if len(speeds) > 2 else speeds[-1],
        "speed_lag_6": speeds[5] if len(speeds) > 5 else speeds[-1],
        "ratio_lag_1": ratios[0] if len(ratios) > 0 else 1.5,
        "ratio_lag_3": ratios[2] if len(ratios) > 2 else ratios[-1],
        "ratio_lag_6": ratios[5] if len(ratios) > 5 else ratios[-1],
        "speed_rolling_3": np.mean(speeds[:3]) if len(speeds) >= 3 else np.mean(speeds),
        "speed_rolling_6": np.mean(speeds[:6]) if len(speeds) >= 6 else np.mean(speeds),
        "free_flow_speed": ff,
        "confidence": 0.8,
    }


def _speed_to_congestion_level(speed: float, free_flow: float) -> str:
    """Convert predicted speed to congestion level."""
    if free_flow <= 0:
        return "unknown"
    ratio = free_flow / speed if speed > 0 else 10.0
    if ratio <= 1.1:
        return "free_flow"
    elif ratio <= 1.3:
        return "light"
    elif ratio <= 1.6:
        return "moderate"
    elif ratio <= 2.0:
        return "heavy"
    else:
        return "severe"


# ================================================================
# FUNCTION 1: Predict Traffic for One Location
# ================================================================
def predict_traffic(
    location_name: str,
    future_minutes: int = 30,
) -> Optional[TrafficPrediction]:
    """
    Predict traffic speed for a specific location at a future time.
    
    Args:
        location_name: Monitoring point name (e.g., "sarjapur_road_junction")
        future_minutes: How many minutes into the future (default: 30)
    
    Returns:
        TrafficPrediction or None if model not available
    """
    try:
        model, encoder, config = _load_model()
    except FileNotFoundError as e:
        logger.error(str(e))
        return None
    
    # Target time
    future_time = datetime.now(timezone.utc) + timedelta(minutes=future_minutes)
    
    # Get lat/lon
    if location_name not in settings.MONITORING_POINTS:
        logger.error(f"Unknown location: {location_name}")
        return None
    lat, lon = settings.MONITORING_POINTS[location_name]
    
    # ---- Build feature vector ----
    # Time features
    hour = future_time.hour
    day_of_week = future_time.weekday()
    is_weekend = 1 if day_of_week >= 5 else 0
    is_rush_hour = 1 if (8 <= hour < 10) or (17 <= hour < 20) else 0
    hour_sin = np.sin(2 * np.pi * hour / 24)
    hour_cos = np.cos(2 * np.pi * hour / 24)
    dow_sin = np.sin(2 * np.pi * day_of_week / 7)
    dow_cos = np.cos(2 * np.pi * day_of_week / 7)
    
    # Location encoding
    if location_name in encoder.classes_:
        location_encoded = encoder.transform([location_name])[0]
    else:
        logger.warning(f"Location {location_name} not in training data, using 0")
        location_encoded = 0
    
    # Weather
    weather = _get_latest_weather()
    is_raining = 1 if weather["rain_1h"] > 0 else 0
    
    # Lag features (recent speeds)
    recent = _get_recent_speeds(location_name)
    
    # Build feature dict matching training order
    features = {
        "hour": hour,
        "day_of_week": day_of_week,
        "is_weekend": is_weekend,
        "is_rush_hour": is_rush_hour,
        "hour_sin": hour_sin,
        "hour_cos": hour_cos,
        "dow_sin": dow_sin,
        "dow_cos": dow_cos,
        "location_encoded": location_encoded,
        "free_flow_speed": recent["free_flow_speed"],
        "confidence": recent["confidence"],
        "temperature": weather["temperature"],
        "humidity": weather["humidity"],
        "wind_speed": weather["wind_speed"],
        "rain_1h": weather["rain_1h"],
        "visibility": weather["visibility"],
        "is_raining": is_raining,
        "speed_lag_1": recent["speed_lag_1"],
        "speed_lag_3": recent["speed_lag_3"],
        "speed_lag_6": recent["speed_lag_6"],
        "ratio_lag_1": recent["ratio_lag_1"],
        "ratio_lag_3": recent["ratio_lag_3"],
        "ratio_lag_6": recent["ratio_lag_6"],
        "speed_rolling_3": recent["speed_rolling_3"],
        "speed_rolling_6": recent["speed_rolling_6"],
    }
    
    # Create DataFrame with correct column order
    feature_df = pd.DataFrame([features])[config["feature_cols"]]
    
    # Predict
    predicted_speed = float(model.predict(feature_df)[0])
    predicted_speed = max(0, predicted_speed)  # Can't be negative
    
    free_flow = recent["free_flow_speed"]
    congestion_ratio = free_flow / predicted_speed if predicted_speed > 0 else 10.0
    congestion_level = _speed_to_congestion_level(predicted_speed, free_flow)
    
    # Confidence note
    if future_minutes <= 15:
        confidence_note = "High confidence (short-term)"
    elif future_minutes <= 60:
        confidence_note = "Medium confidence (medium-term)"
    else:
        confidence_note = "Low confidence (long-term, conditions may change)"
    
    prediction = TrafficPrediction(
        location_name=location_name,
        lat=lat,
        lon=lon,
        predicted_speed=round(predicted_speed, 1),
        free_flow_speed=round(free_flow, 1),
        predicted_congestion_ratio=round(congestion_ratio, 2),
        congestion_level=congestion_level,
        prediction_time=future_time,
        confidence_note=confidence_note,
    )
    
    return prediction


# ================================================================
# FUNCTION 2: Predict All Locations
# ================================================================
def predict_all_locations(future_minutes: int = 30) -> list[TrafficPrediction]:
    """
    Predict traffic for all 8 monitoring points.
    
    Args:
        future_minutes: Minutes into the future
    
    Returns:
        List of TrafficPrediction for each location
    """
    logger.info(f"Predicting traffic for all locations (+{future_minutes}min)")
    
    predictions = []
    
    for location_name in settings.MONITORING_POINTS:
        pred = predict_traffic(location_name, future_minutes)
        if pred:
            predictions.append(pred)
    
    return predictions


# ================================================================
# MAIN — Test predictions
# ================================================================
if __name__ == "__main__":
    print("\n" + "🔮 " * 20)
    print("TRAFFIC SPEED PREDICTIONS")
    print("🔮 " * 20 + "\n")
    
    for minutes in [15, 30, 60]:
        print(f"\n{'='*75}")
        print(f"⏱️  Predictions for +{minutes} minutes")
        print(f"{'='*75}")
        
        predictions = predict_all_locations(future_minutes=minutes)
        
        if not predictions:
            print("  ⚠️  No predictions available. Train the model first!")
            print(f"  Run: jupyter notebook notebooks/02_model_training.ipynb")
            break
        
        level_emojis = {
            "free_flow": "🟢", "light": "🟡", "moderate": "🟠",
            "heavy": "🔴", "severe": "⛔", "unknown": "⚪"
        }
        
        print(f"\n  {'Location':<30s} {'Predicted':>10s} {'FreeFlow':>10s} {'Ratio':>7s} {'Level':<12s}")
        print(f"  {'-'*30} {'-'*10} {'-'*10} {'-'*7} {'-'*12}")
        
        for p in sorted(predictions, key=lambda x: x.predicted_congestion_ratio, reverse=True):
            display = p.location_name.replace("_", " ").title()
            if len(display) > 28:
                display = display[:28] + ".."
            emoji = level_emojis.get(p.congestion_level, "⚪")
            
            print(f"  {emoji} {display:<28s} {p.predicted_speed:>8.1f}  "
                  f"{p.free_flow_speed:>8.1f}  {p.predicted_congestion_ratio:>6.2f} "
                  f"{p.congestion_level}")
        
        print(f"\n  📝 {predictions[0].confidence_note}")
    
    print(f"\n✅ Predictions complete!")