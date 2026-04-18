"""
FastAPI application for the Urban Intelligence Dashboard.
Exposes traffic, weather, air quality, analytics, and map data via REST API.
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from datetime import datetime, timezone
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import text
from loguru import logger

from src.database.connection import engine
from src.config.settings import settings
from src.api.schemas import HealthCheckResponse, ErrorResponse


# ================================================================
# LIFESPAN (startup + shutdown)
# ================================================================
@asynccontextmanager
async def lifespan(app: FastAPI):
    """Handle startup and shutdown events."""
    # ---- STARTUP ----
    logger.info("🚀 Starting Urban Intelligence API...")
    
    # Test database connection
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        logger.info("✅ Database connection verified")
    except Exception as e:
        logger.error(f"❌ Database connection failed: {e}")
    
    logger.info(f"📍 Study Area: {settings.STUDY_AREA_NAME}")
    logger.info(f"📍 Monitoring Points: {len(settings.MONITORING_POINTS)}")
    logger.info("✅ API ready")
    
    yield  # App runs here
    
    # ---- SHUTDOWN ----
    logger.info("🛑 Shutting down API...")
    engine.dispose()
    logger.info("👋 Database connections closed. Goodbye!")


# ================================================================
# CREATE APP
# ================================================================
app = FastAPI(
    title="Urban Intelligence Dashboard API",
    description=(
        "REST API for real-time urban monitoring of the "
        "Sarjapur-Dommasandra-Varthur Belt, Bengaluru.\n\n"
        "**Features:**\n"
        "- 🚗 Live traffic data from 8 monitoring points (TomTom)\n"
        "- 🌤️ Weather conditions (OpenWeatherMap)\n"
        "- 🌬️ Air quality index and pollutants\n"
        "- 📊 Congestion analysis and risk scoring\n"
        "- 🔮 ML-based traffic speed predictions\n"
        "- 🚨 Anomaly detection alerts\n"
        "- 🗺️ GeoJSON map layers (roads, buildings, POIs)\n"
    ),
    version="1.0.0",
    lifespan=lifespan,
    responses={
        500: {"model": ErrorResponse, "description": "Internal server error"},
    },
)


# ================================================================
# CORS MIDDLEWARE
# ================================================================
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],        # Allow all origins (development)
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ================================================================
# HEALTH CHECK
# ================================================================
@app.get(
    "/health",
    response_model=HealthCheckResponse,
    tags=["System"],
    summary="Health check",
    description="Check API status, database connection, and table row counts.",
)
def health_check():
    """Health check endpoint."""
    
    db_status = "disconnected"
    tables = {}
    
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
            db_status = "connected"
            
            table_names = [
                "roads", "buildings", "points_of_interest",
                "traffic_readings", "weather_readings",
                "air_quality_readings", "anomalies",
            ]
            for table in table_names:
                result = conn.execute(text(f"SELECT count(*) FROM {table}"))
                tables[table] = result.scalar()
                
    except Exception as e:
        db_status = f"error: {str(e)[:100]}"
    
    return HealthCheckResponse(
        status="healthy" if db_status == "connected" else "degraded",
        timestamp=datetime.now(timezone.utc),
        database=db_status,
        tables=tables,
    )


# ================================================================
# ROOT
# ================================================================
@app.get(
    "/",
    tags=["System"],
    summary="API root",
)
def root():
    """API root — basic info and links."""
    return {
        "name": "Urban Intelligence Dashboard API",
        "version": "1.0.0",
        "study_area": settings.STUDY_AREA_NAME,
        "monitoring_points": len(settings.MONITORING_POINTS),
        "docs": "/docs",
        "health": "/health",
        "endpoints": {
            "traffic": "/api/traffic",
            "weather": "/api/weather",
            "air_quality": "/api/air-quality",
            "analytics": "/api/analytics",
            "predictions": "/api/predictions",
            "anomalies": "/api/anomalies",
            "geo": "/api/geo",
        },
    }


# ================================================================
# INCLUDE ROUTERS (will add as we build them)
# ================================================================
# ================================================================
# INCLUDE ROUTERS
# ================================================================
from src.api.routes import traffic, weather, analytics
app.include_router(traffic.router)
app.include_router(weather.router)
app.include_router(analytics.router)



# ================================================================
# RUN (for development)
# ================================================================
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "src.api.main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        reload_dirs=["src"],
    )