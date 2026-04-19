from pydantic_settings import BaseSettings
from typing import Dict, Tuple
from pathlib import Path


class Settings(BaseSettings):
    """
    Central configuration for the Urban Intelligence Dashboard.
    Loads values from .env file and defines all project constants.
    """

    # ──────────────────────────────────────────────
    # API Keys (loaded from .env)
    # ──────────────────────────────────────────────
    TOMTOM_API_KEY: str
    OWM_API_KEY: str

    # ──────────────────────────────────────────────
    # Database (loaded from .env)
    # ──────────────────────────────────────────────
    DATABASE_URL: str
    REDIS_URL: str

    # ──────────────────────────────────────────────
    # Project Paths
    # ──────────────────────────────────────────────
    PROJECT_ROOT: Path = Path(__file__).resolve().parent.parent.parent
    DATA_DIR: Path = PROJECT_ROOT / "data"
    RAW_DATA_DIR: Path = DATA_DIR / "raw"
    PROCESSED_DATA_DIR: Path = DATA_DIR / "processed"
    GEOJSON_DIR: Path = DATA_DIR / "geojson"

    # ──────────────────────────────────────────────
    # Study Area — Sarjapur-Dommasandra-Varthur Belt
    # Bounding box: (min_lat, min_lon, max_lat, max_lon)
    # ──────────────────────────────────────────────
    STUDY_AREA_NAME: str = "Sarjapur-Dommasandra-Varthur Belt, Bengaluru"
    STUDY_AREA_BBOX: Tuple[float, float, float, float] = (
        12.8600,   # min_lat (south)
        77.6800,   # min_lon (west)
        12.9500,   # max_lat (north)
        77.7700,   # max_lon (east)
    )
    STUDY_AREA_CENTER: Tuple[float, float] = (12.9050, 77.7250)

    # ──────────────────────────────────────────────
    # 8 Monitoring Points (lat, lon)
    # ──────────────────────────────────────────────
    MONITORING_POINTS: Dict[str, Tuple[float, float]] = {
        "sarjapur_road_junction":   (12.9165, 77.6750),  # Near Iblur/Sarjapur Main Road
        "dommasandra_circle":       (12.8832, 77.7524),  # Intersection of SH-35 and Sarjapur Road
        "chembenahalli":            (12.8793, 77.7616),  # Near the main village entrance/bus stop
        "varthur_gunjur_junction":  (12.9265, 77.7377),  # Specifically at Gunjur Village junction
        "sarjapur_town_center":     (12.8600, 77.7860),  # Central bus stand/Police station area
        "carmelaram_junction":      (12.9125, 77.7056),  # Near the railway station/Decathlon intersection
        "harlur_road_junction":     (12.9080, 77.6760),  # Entry to Harlur Road from Sarjapur Road
        "iblur_wipro_junction":     (12.9209, 77.6653),  # Major signal at Sarjapur Road-ORR intersection

    }

    # ──────────────────────────────────────────────
    # API URLs
    # ──────────────────────────────────────────────
    TOMTOM_TRAFFIC_URL: str = "https://api.tomtom.com/traffic/services/4/flowSegmentData/absolute/10/json"
    TOMTOM_INCIDENTS_URL: str = "https://api.tomtom.com/traffic/services/5/incidentDetails"
    OWM_CURRENT_WEATHER_URL: str = "https://api.openweathermap.org/data/2.5/weather"
    OWM_FORECAST_URL: str = "https://api.openweathermap.org/data/2.5/forecast"
    OSM_OVERPASS_URL: str = "https://overpass-api.de/api/interpreter"

    # ──────────────────────────────────────────────
    # Data Collection Settings
    # ──────────────────────────────────────────────
    TRAFFIC_POLL_INTERVAL_SECONDS: int = 300    # every 5 minutes
    WEATHER_POLL_INTERVAL_SECONDS: int = 600    # every 10 minutes
    REQUEST_TIMEOUT_SECONDS: int = 30

    # ──────────────────────────────────────────────
    # Dashboard Settings
    # ──────────────────────────────────────────────
    STREAMLIT_PAGE_TITLE: str = "Urban Intelligence Dashboard"
    MAP_DEFAULT_ZOOM: int = 13

    # ──────────────────────────────────────────────
    # Pydantic Settings Config
    # ──────────────────────────────────────────────
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        extra = "ignore"


# ──────────────────────────────────────────────
# Single instance — import this everywhere:
#   from src.config.settings import settings
# ──────────────────────────────────────────────
settings = Settings()