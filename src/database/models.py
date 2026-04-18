import sys
from pathlib import Path

# Add project root to Python path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from datetime import datetime
from sqlalchemy import (
    Column, Integer, String, Float, DateTime, Index, Text
)
from sqlalchemy.orm import declarative_base
from geoalchemy2 import Geometry

# ──────────────────────────────────────────────
# Base class — all models inherit from this
# ──────────────────────────────────────────────
Base = declarative_base()


# ══════════════════════════════════════════════
# 1. ROADS — Street network from OpenStreetMap
# ══════════════════════════════════════════════
class Road(Base):
    """Road segments in the study area."""
    __tablename__ = "roads"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(255), nullable=True)                # road name (can be unnamed)
    road_type = Column(String(100), nullable=False)          # 'primary', 'secondary', 'residential', etc.
    speed_limit = Column(Integer, nullable=True)             # km/h
    geometry = Column(
        Geometry("LINESTRING", srid=4326, spatial_index=True),  # auto-creates GIST index
        nullable=False,
    )

    # Additional indexes
    __table_args__ = (
        Index("idx_roads_road_type", "road_type"),
        Index("idx_roads_name", "name"),
    )

    def __repr__(self):
        return f"<Road(id={self.id}, name='{self.name}', type='{self.road_type}')>"


# ══════════════════════════════════════════════
# 2. BUILDINGS — Building footprints
# ══════════════════════════════════════════════
class Building(Base):
    """Building footprints in the study area."""
    __tablename__ = "buildings"

    id = Column(Integer, primary_key=True, autoincrement=True)
    building_type = Column(String(100), nullable=True)       # 'residential', 'commercial', 'industrial', etc.
    area_sqm = Column(Float, nullable=True)                  # building area in square meters
    geometry = Column(
        Geometry("POLYGON", srid=4326, spatial_index=True),
        nullable=False,
    )

    # Additional indexes
    __table_args__ = (
        Index("idx_buildings_type", "building_type"),
    )

    def __repr__(self):
        return f"<Building(id={self.id}, type='{self.building_type}', area={self.area_sqm})>"


# ══════════════════════════════════════════════
# 3. POINTS OF INTEREST — Shops, parks, hospitals, etc.
# ══════════════════════════════════════════════
class PointOfInterest(Base):
    """Points of interest (restaurants, schools, hospitals, etc.)."""
    __tablename__ = "points_of_interest"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(255), nullable=True)
    category = Column(String(100), nullable=False)           # 'food', 'education', 'health', etc.
    subcategory = Column(String(100), nullable=True)         # 'restaurant', 'school', 'hospital', etc.
    geometry = Column(
        Geometry("POINT", srid=4326, spatial_index=True),
        nullable=False,
    )

    # Additional indexes
    __table_args__ = (
        Index("idx_poi_category", "category"),
        Index("idx_poi_subcategory", "subcategory"),
        Index("idx_poi_cat_subcat", "category", "subcategory"),  # composite index
    )

    def __repr__(self):
        return f"<POI(id={self.id}, name='{self.name}', category='{self.category}')>"


# ══════════════════════════════════════════════
# 4. WARD BOUNDARIES — Administrative boundaries
# ══════════════════════════════════════════════
class WardBoundary(Base):
    """Administrative ward boundaries."""
    __tablename__ = "ward_boundaries"

    id = Column(Integer, primary_key=True, autoincrement=True)
    ward_name = Column(String(255), nullable=False)
    ward_number = Column(Integer, nullable=True)
    population = Column(Integer, nullable=True)
    geometry = Column(
        Geometry("POLYGON", srid=4326, spatial_index=True),
        nullable=False,
    )

    # Additional indexes
    __table_args__ = (
        Index("idx_ward_name", "ward_name"),
        Index("idx_ward_number", "ward_number"),
    )

    def __repr__(self):
        return f"<Ward(id={self.id}, name='{self.ward_name}', number={self.ward_number})>"


# ══════════════════════════════════════════════
# 5. TRAFFIC READINGS — Time-series traffic data
#    (Many rows — one per reading per location)
# ══════════════════════════════════════════════
class TrafficReading(Base):
    """Real-time traffic readings from TomTom API."""
    __tablename__ = "traffic_readings"

    id = Column(Integer, primary_key=True, autoincrement=True)
    timestamp = Column(DateTime, nullable=False, default=datetime.utcnow)
    location_name = Column(String(255), nullable=False)      # e.g., 'sarjapur_road_junction'
    lat = Column(Float, nullable=False)
    lon = Column(Float, nullable=False)
    current_speed = Column(Float, nullable=True)             # km/h — actual speed right now
    free_flow_speed = Column(Float, nullable=True)           # km/h — speed with no traffic
    confidence = Column(Float, nullable=True)                # 0.0 to 1.0 — data reliability
    congestion_ratio = Column(Float, nullable=True)          # current_speed / free_flow_speed
    geometry = Column(
        Geometry("POINT", srid=4326, spatial_index=True),
        nullable=False,
    )

    # Additional indexes — optimized for time-series queries
    __table_args__ = (
        Index("idx_traffic_timestamp", "timestamp"),
        Index("idx_traffic_location", "location_name"),
        Index("idx_traffic_ts_location", "timestamp", "location_name"),   # composite: filter by time + location
        Index("idx_traffic_congestion", "congestion_ratio"),
    )

    def __repr__(self):
        return f"<Traffic(id={self.id}, location='{self.location_name}', speed={self.current_speed}, time={self.timestamp})>"


# ══════════════════════════════════════════════
# 6. WEATHER READINGS — Time-series weather data
# ══════════════════════════════════════════════
class WeatherReading(Base):
    """Weather readings from OpenWeatherMap API."""
    __tablename__ = "weather_readings"

    id = Column(Integer, primary_key=True, autoincrement=True)
    timestamp = Column(DateTime, nullable=False, default=datetime.utcnow)
    temperature = Column(Float, nullable=True)               # Celsius
    humidity = Column(Float, nullable=True)                  # percentage (0-100)
    pressure = Column(Float, nullable=True)                  # hPa
    weather_description = Column(String(255), nullable=True) # 'clear sky', 'light rain', etc.
    wind_speed = Column(Float, nullable=True)                # m/s
    rain_1h = Column(Float, nullable=True)                   # mm of rain in last 1 hour
    visibility = Column(Float, nullable=True)                # meters
    geometry = Column(
        Geometry("POINT", srid=4326, spatial_index=True),
        nullable=False,
    )

    # Additional indexes
    __table_args__ = (
        Index("idx_weather_timestamp", "timestamp"),
        Index("idx_weather_description", "weather_description"),
    )

    def __repr__(self):
        return f"<Weather(id={self.id}, temp={self.temperature}°C, desc='{self.weather_description}', time={self.timestamp})>"


# ══════════════════════════════════════════════
# 7. AIR QUALITY READINGS — Time-series air data
# ══════════════════════════════════════════════
class AirQualityReading(Base):
    """Air quality readings."""
    __tablename__ = "air_quality_readings"

    id = Column(Integer, primary_key=True, autoincrement=True)
    timestamp = Column(DateTime, nullable=False, default=datetime.utcnow)
    aqi = Column(Integer, nullable=True)                     # Air Quality Index (1-5 scale)
    pm25 = Column(Float, nullable=True)                      # PM2.5 concentration (μg/m³)
    pm10 = Column(Float, nullable=True)                      # PM10 concentration (μg/m³)
    no2 = Column(Float, nullable=True)                       # Nitrogen Dioxide (μg/m³)
    o3 = Column(Float, nullable=True)                        # Ozone (μg/m³)
    co = Column(Float, nullable=True)                        # Carbon Monoxide (μg/m³)
    so2 = Column(Float, nullable=True)                       # Sulfur Dioxide (μg/m³)
    geometry = Column(
        Geometry("POINT", srid=4326, spatial_index=True),
        nullable=False,
    )

    # Additional indexes
    __table_args__ = (
        Index("idx_airquality_timestamp", "timestamp"),
        Index("idx_airquality_aqi", "aqi"),
    )

    def __repr__(self):
        return f"<AirQuality(id={self.id}, aqi={self.aqi}, pm25={self.pm25}, time={self.timestamp})>"


# ══════════════════════════════════════════════
# 8. ANOMALIES — Generated by ML models
# ══════════════════════════════════════════════
class Anomaly(Base):
    """Anomalies detected by ML analysis."""
    __tablename__ = "anomalies"

    id = Column(Integer, primary_key=True, autoincrement=True)
    timestamp = Column(DateTime, nullable=False, default=datetime.utcnow)
    anomaly_type = Column(String(100), nullable=False)       # 'traffic_spike', 'unusual_weather', etc.
    severity = Column(String(50), nullable=False)            # 'low', 'medium', 'high', 'critical'
    description = Column(Text, nullable=True)                # human-readable explanation
    location_name = Column(String(255), nullable=True)       # e.g., 'dommasandra_circle'
    geometry = Column(
        Geometry("POINT", srid=4326, spatial_index=True),
        nullable=False,
    )

    # Additional indexes
    __table_args__ = (
        Index("idx_anomaly_timestamp", "timestamp"),
        Index("idx_anomaly_type", "anomaly_type"),
        Index("idx_anomaly_severity", "severity"),
        Index("idx_anomaly_ts_type", "timestamp", "anomaly_type"),        # composite
        Index("idx_anomaly_ts_severity", "timestamp", "severity"),        # composite
        Index("idx_anomaly_location", "location_name"),
    )

    def __repr__(self):
        return f"<Anomaly(id={self.id}, type='{self.anomaly_type}', severity='{self.severity}', time={self.timestamp})>"
    