"""
Traffic API routes.
Endpoints for live traffic data, history, heatmap, and predictions.
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent.parent))

from datetime import datetime, timezone, timedelta
from typing import Optional

from fastapi import APIRouter, Query, HTTPException
from sqlalchemy import text
from loguru import logger

from src.database.connection import engine
from src.config.settings import settings
from src.api.schemas import (
    TrafficReading,
    TrafficReadingList,
    TrafficHistory,
    TrafficPredictionItem,
    TrafficPredictionList,
    ErrorResponse,
)

router = APIRouter(
    prefix="/api/v1/traffic",
    tags=["Traffic"],
)


# ================================================================
# GET /api/v1/traffic/latest
# ================================================================
@router.get(
    "/latest",
    response_model=TrafficReadingList,
    summary="Latest traffic for all locations",
    description="Returns the most recent traffic reading for each of the 8 monitoring points.",
)
def get_latest_traffic():
    """Get latest traffic reading per location."""
    try:
        with engine.connect() as conn:
            result = conn.execute(text("""
                SELECT DISTINCT ON (location_name)
                    id, timestamp, location_name, lat, lon,
                    current_speed, free_flow_speed, confidence, congestion_ratio
                FROM traffic_readings
                ORDER BY location_name, timestamp DESC
            """))
            rows = result.fetchall()

        readings = [
            TrafficReading(
                id=row[0],
                timestamp=row[1],
                location_name=row[2],
                lat=row[3],
                lon=row[4],
                current_speed=row[5],
                free_flow_speed=row[6],
                confidence=row[7],
                congestion_ratio=row[8],
            )
            for row in rows
        ]

        return TrafficReadingList(count=len(readings), readings=readings)

    except Exception as e:
        logger.error(f"Error fetching latest traffic: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ================================================================
# GET /api/v1/traffic/history/{location}
# ================================================================
@router.get(
    "/history/{location}",
    response_model=TrafficHistory,
    summary="Traffic history for a location",
    description="Returns traffic readings for a specific monitoring point over the given time window.",
    responses={404: {"model": ErrorResponse}},
)
def get_traffic_history(
    location: str,
    hours: int = Query(default=24, ge=1, le=168, description="Hours of history (1-168)"),
):
    """Get traffic history for a specific location."""

    # Validate location
    valid_locations = list(settings.MONITORING_POINTS.keys())
    if location not in valid_locations:
        raise HTTPException(
            status_code=404,
            detail=f"Location '{location}' not found. Valid locations: {valid_locations}",
        )

    cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)

    try:
        with engine.connect() as conn:
            result = conn.execute(text("""
                SELECT 
                    id, timestamp, location_name, lat, lon,
                    current_speed, free_flow_speed, confidence, congestion_ratio
                FROM traffic_readings
                WHERE location_name = :loc
                  AND timestamp >= :cutoff
                ORDER BY timestamp ASC
            """), {"loc": location, "cutoff": cutoff})
            rows = result.fetchall()

        readings = [
            TrafficReading(
                id=row[0],
                timestamp=row[1],
                location_name=row[2],
                lat=row[3],
                lon=row[4],
                current_speed=row[5],
                free_flow_speed=row[6],
                confidence=row[7],
                congestion_ratio=row[8],
            )
            for row in rows
        ]

        return TrafficHistory(
            location_name=location,
            count=len(readings),
            hours=hours,
            readings=readings,
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching traffic history: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ================================================================
# GET /api/v1/traffic/heatmap
# ================================================================
@router.get(
    "/heatmap",
    summary="Traffic heatmap data",
    description="Returns all monitoring points with latest congestion scores, formatted for heatmap visualization.",
)
def get_traffic_heatmap():
    """Get heatmap-ready traffic data for all locations."""
    try:
        with engine.connect() as conn:
            result = conn.execute(text("""
                SELECT DISTINCT ON (location_name)
                    location_name, lat, lon,
                    current_speed, free_flow_speed, congestion_ratio,
                    timestamp
                FROM traffic_readings
                ORDER BY location_name, timestamp DESC
            """))
            rows = result.fetchall()

        points = []
        for row in rows:
            location_name = row[0]
            lat = row[1]
            lon = row[2]
            current_speed = row[3]
            free_flow_speed = row[4]
            congestion_ratio = row[5]
            timestamp = row[6]

            # Congestion score 0-100
            if congestion_ratio <= 1.0:
                score = 0
            elif congestion_ratio >= 3.0:
                score = 100
            else:
                score = round(((congestion_ratio - 1.0) / 2.0) * 100, 1)

            # Level
            if score <= 15:
                level = "free_flow"
                color = "#00cc00"
            elif score <= 35:
                level = "light"
                color = "#ffcc00"
            elif score <= 55:
                level = "moderate"
                color = "#ff8800"
            elif score <= 75:
                level = "heavy"
                color = "#ff0000"
            else:
                level = "severe"
                color = "#990000"

            speed_pct = round((current_speed / free_flow_speed * 100), 1) if free_flow_speed > 0 else 0

            points.append({
                "location_name": location_name,
                "display_name": location_name.replace("_", " ").title(),
                "lat": lat,
                "lon": lon,
                "current_speed": current_speed,
                "free_flow_speed": free_flow_speed,
                "congestion_ratio": congestion_ratio,
                "congestion_score": score,
                "congestion_level": level,
                "speed_pct": speed_pct,
                "color": color,
                "timestamp": str(timestamp),
            })

        # Sort by congestion (worst first)
        points.sort(key=lambda x: x["congestion_score"], reverse=True)

        return {
            "count": len(points),
            "study_area": settings.STUDY_AREA_NAME,
            "center": {"lat": settings.STUDY_AREA_CENTER[0], "lon": settings.STUDY_AREA_CENTER[1]},
            "points": points,
        }

    except Exception as e:
        logger.error(f"Error fetching heatmap data: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ================================================================
# GET /api/v1/traffic/predict
# ================================================================
@router.get(
    "/predict",
    response_model=TrafficPredictionList,
    summary="Predict traffic for all locations",
    description="Uses ML model to predict traffic speed at all monitoring points for a future time.",
    responses={503: {"model": ErrorResponse}},
)
def get_traffic_predictions(
    minutes: int = Query(default=30, ge=5, le=120, description="Minutes into future (5-120)"),
):
    """Predict traffic for all locations."""
    try:
        from src.analytics.traffic_predictor import predict_all_locations

        predictions = predict_all_locations(future_minutes=minutes)

        if not predictions:
            raise HTTPException(
                status_code=503,
                detail="Model not available. Train it first: python scripts/train_model.py",
            )

        items = [
            TrafficPredictionItem(
                location_name=p.location_name,
                lat=p.lat,
                lon=p.lon,
                predicted_speed=p.predicted_speed,
                free_flow_speed=p.free_flow_speed,
                predicted_congestion_ratio=p.predicted_congestion_ratio,
                congestion_level=p.congestion_level,
                prediction_time=p.prediction_time,
                confidence_note=p.confidence_note,
            )
            for p in predictions
        ]

        return TrafficPredictionList(
            future_minutes=minutes,
            count=len(items),
            predictions=items,
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error generating predictions: {e}")
        raise HTTPException(status_code=500, detail=str(e))