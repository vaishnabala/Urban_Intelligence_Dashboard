"""
Analytics and Air Quality API routes.
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent.parent))

from fastapi import APIRouter, HTTPException
from sqlalchemy import text
from datetime import datetime, timedelta, timezone
from src.database.connection import get_engine
from src.analytics.risk_scorer import analyze_all_locations
from src.analytics.anomaly_detection import get_active_anomalies

router = APIRouter(tags=["Analytics & AQI"])


# ──────────────────────────────────────
# Air Quality
# ──────────────────────────────────────

@router.get("/api/v1/aqi/latest")
async def get_latest_aqi():
    """Return the most recent air quality reading."""
    engine = get_engine()
    query = text("""
        SELECT id, timestamp, aqi, pm25, pm10, no2, o3, co, so2
        FROM air_quality_readings
        ORDER BY timestamp DESC
        LIMIT 1
    """)
    with engine.connect() as conn:
        row = conn.execute(query).fetchone()

    if not row:
        raise HTTPException(status_code=404, detail="No air quality data found")

    return {
        "id": row.id,
        "timestamp": row.timestamp.isoformat(),
        "aqi": row.aqi,
        "pm25": row.pm25,
        "pm10": row.pm10,
        "no2": row.no2,
        "o3": row.o3,
        "co": row.co,
        "so2": row.so2
    }


# ──────────────────────────────────────
# Anomalies
# ──────────────────────────────────────

@router.get("/api/v1/analytics/anomalies")
async def get_anomalies():
    """Return currently active anomalies."""
    try:
        anomalies = get_active_anomalies()
        results = []
        for a in anomalies:
            results.append({
                "id": a.get("id"),
                "timestamp": a.get("timestamp").isoformat() if a.get("timestamp") else None,
                "anomaly_type": a.get("anomaly_type"),
                "severity": a.get("severity"),
                "description": a.get("description"),
                "location_name": a.get("location_name")
            })
        return {"count": len(results), "anomalies": results}
    except Exception as e:
        return {"count": 0, "anomalies": [], "note": str(e)}


# ──────────────────────────────────────
# Risk Scores
# ──────────────────────────────────────

@router.get("/api/v1/analytics/risk-scores")
async def get_risk_scores():
    """Return risk scores for all monitored locations."""
    try:
        scores = analyze_all_locations(time_window_hours=1)
        return {"count": len(scores), "scores": scores}
    except Exception as e:
        return {"count": 0, "scores": [], "note": str(e)}


# ──────────────────────────────────────
# Dashboard Summary
# ──────────────────────────────────────

@router.get("/api/v1/analytics/summary")
async def get_dashboard_summary():
    """Return overall dashboard summary."""
    engine = get_engine()
    cutoff = datetime.now(timezone.utc) - timedelta(hours=24)

    with engine.connect() as conn:
        # Total monitored points
        traffic_points = conn.execute(text(
            "SELECT COUNT(DISTINCT location_name) FROM traffic_readings"
        )).scalar() or 0

        # Average congestion (last 24h)
        avg_congestion = conn.execute(text("""
            SELECT AVG(congestion_ratio) FROM traffic_readings
            WHERE timestamp >= :cutoff
        """), {"cutoff": cutoff}).scalar()

        # Latest weather
        weather_row = conn.execute(text("""
            SELECT temperature, humidity, weather_description
            FROM weather_readings ORDER BY timestamp DESC LIMIT 1
        """)).fetchone()

        # Latest AQI
        aqi_val = conn.execute(text("""
            SELECT aqi FROM air_quality_readings
            ORDER BY timestamp DESC LIMIT 1
        """)).scalar()

        # Active anomalies count
        anomaly_count = conn.execute(text("""
            SELECT COUNT(*) FROM anomalies
            WHERE timestamp >= :cutoff
        """), {"cutoff": cutoff}).scalar() or 0

    weather_summary = None
    if weather_row:
        weather_summary = {
            "temperature": weather_row.temperature,
            "humidity": weather_row.humidity,
            "description": weather_row.weather_description
        }

    return {
        "monitored_locations": traffic_points,
        "active_anomalies_24h": anomaly_count,
        "avg_congestion_24h": round(avg_congestion, 4) if avg_congestion else None,
        "latest_aqi": aqi_val,
        "weather": weather_summary,
        "generated_at": datetime.now(timezone.utc).isoformat()
    }
