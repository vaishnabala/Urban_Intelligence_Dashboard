"""
Weather API routes.
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent.parent))

from fastapi import APIRouter, Query, HTTPException
from sqlalchemy import text
from datetime import datetime, timedelta, timezone
from src.database.connection import get_engine

router = APIRouter(prefix="/api/v1/weather", tags=["Weather"])


@router.get("/latest")
async def get_latest_weather():
    """Return the most recent weather reading."""
    engine = get_engine()
    query = text("""
        SELECT id, timestamp, temperature, humidity, pressure,
               weather_description, wind_speed, rain_1h, visibility
        FROM weather_readings
        ORDER BY timestamp DESC
        LIMIT 1
    """)
    with engine.connect() as conn:
        row = conn.execute(query).fetchone()

    if not row:
        raise HTTPException(status_code=404, detail="No weather data found")

    return {
        "id": row.id,
        "timestamp": row.timestamp.isoformat(),
        "temperature": row.temperature,
        "humidity": row.humidity,
        "pressure": row.pressure,
        "weather_description": row.weather_description,
        "wind_speed": row.wind_speed,
        "rain_1h": row.rain_1h,
        "visibility": row.visibility
    }


@router.get("/history")
async def get_weather_history(hours: int = Query(default=24, ge=1, le=168)):
    """Return weather readings for the past N hours."""
    engine = get_engine()
    cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
    query = text("""
        SELECT id, timestamp, temperature, humidity, pressure,
               weather_description, wind_speed, rain_1h, visibility
        FROM weather_readings
        WHERE timestamp >= :cutoff
        ORDER BY timestamp DESC
    """)
    with engine.connect() as conn:
        rows = conn.execute(query, {"cutoff": cutoff}).fetchall()

    results = []
    for row in rows:
        results.append({
            "id": row.id,
            "timestamp": row.timestamp.isoformat(),
            "temperature": row.temperature,
            "humidity": row.humidity,
            "pressure": row.pressure,
            "weather_description": row.weather_description,
            "wind_speed": row.wind_speed,
            "rain_1h": row.rain_1h,
            "visibility": row.visibility
        })

    return {"hours": hours, "count": len(results), "readings": results}