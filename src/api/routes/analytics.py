"""
Analytics and Air Quality API routes.
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent.parent))

from fastapi import APIRouter, HTTPException
from sqlalchemy import text
from datetime import datetime, timedelta, timezone
from src.database.connection import engine
from src.analytics.risk_scorer import analyze_all_locations
from src.analytics.anomaly_detection import get_active_anomalies

router = APIRouter(tags=["Analytics & AQI"])


# ──────────────────────────────────────
# Air Quality
# ──────────────────────────────────────

@router.get("/api/v1/aqi/latest")
async def get_latest_aqi():
    """Return the most recent air quality reading."""
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
        gdf = get_active_anomalies(hours=24)
        results = []
        if len(gdf) > 0:
            for _, row in gdf.iterrows():
                ts = row.get("timestamp")
                results.append({
                    "id": int(row.get("id")) if row.get("id") is not None else None,
                    "timestamp": ts.isoformat() if hasattr(ts, "isoformat") else str(ts),
                    "anomaly_type": row.get("anomaly_type"),
                    "severity": row.get("severity"),
                    "description": row.get("description"),
                    "location_name": row.get("location_name")
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
# ──────────────<span class="ml-2" /><span class="inline-block w-3 h-3 rounded-full bg-neutral-a12 align-middle mb-[0.1rem]" />
# ──────────────────────────────────────
# Dashboard Summary
# ──────────────────────────────────────

@router.get("/api/v1/analytics/summary")
async def get_summary():
    """Return a combined summary for the dashboard."""
    summary = {}

    # Latest AQI
    try:
        query = text("SELECT aqi, timestamp FROM air_quality_readings ORDER BY timestamp DESC LIMIT 1")
        with engine.connect() as conn:
            row = conn.execute(query).fetchone()
        summary["aqi"] = {"value": row.aqi, "timestamp": row.timestamp.isoformat()} if row else None
    except Exception:
        summary["aqi"] = None

    # Latest weather
    try:
        query = text("SELECT temperature, humidity, weather_description, timestamp FROM weather_readings ORDER BY timestamp DESC LIMIT 1")
        with engine.connect() as conn:
            row = conn.execute(query).fetchone()
        summary["weather"] = {
            "temperature": row.temperature,
            "humidity": row.humidity,
            "description": row.weather_description,
            "timestamp": row.timestamp.isoformat()
        } if row else None
    except Exception:
        summary["weather"] = None

    # Traffic count
    try:
        cutoff = datetime.now(timezone.utc) - timedelta(hours=1)
        query = text("SELECT COUNT(*) as cnt FROM traffic_readings WHERE timestamp >= :cutoff")
        with engine.connect() as conn:
            row = conn.execute(query, {"cutoff": cutoff}).fetchone()
        summary["traffic_readings_last_hour"] = row.cnt if row else 0
    except Exception:
        summary["traffic_readings_last_hour"] = 0

    # Active anomalies count
    try:
        anomalies = get_active_anomalies()
        summary["active_anomalies"] = len(anomalies)
    except Exception:
        summary["active_anomalies"] = 0

    return summary