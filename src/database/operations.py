"""
Database operations for Urban Intelligence Dashboard.
Provides functions to insert, query, and retrieve spatial data from PostGIS.
"""

import sys
from pathlib import Path

# Fix import path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from datetime import datetime, timedelta, timezone
from typing import Optional

import geopandas as gpd
import pandas as pd
from geoalchemy2.shape import from_shape
from loguru import logger
from shapely.geometry import Point
from shapely import wkt
from sqlalchemy import text

from src.database.connection import engine, SessionLocal
from src.database.models import (
    Road,
    Building,
    PointOfInterest,
    WardBoundary,
    TrafficReading,
    WeatherReading,
    AirQualityReading,
    Anomaly,
)


# ============================================================
# TABLE NAME → MODEL MAPPING
# ============================================================

TABLE_MODEL_MAP = {
    "roads": Road,
    "buildings": Building,
    "points_of_interest": PointOfInterest,
    "ward_boundaries": WardBoundary,
    "traffic_readings": TrafficReading,
    "weather_readings": WeatherReading,
    "air_quality_readings": AirQualityReading,
    "anomalies": Anomaly,
}


# ============================================================
# BULK INSERT (for GeoDataFrames — OSM data, etc.)
# ============================================================

def bulk_insert_geodata(table_name: str, gdf: gpd.GeoDataFrame) -> int:
    """
    Insert a GeoDataFrame into a PostGIS table.

    Args:
        table_name: Name of the target table (e.g., "roads", "buildings")
        gdf: GeoDataFrame with geometry column and matching attribute columns

    Returns:
        Number of rows inserted

    Example:
        count = bulk_insert_geodata("roads", roads_gdf)
    """
    try:
        if gdf is None or gdf.empty:
            logger.warning(f"Empty GeoDataFrame passed for table '{table_name}'. Nothing to insert.")
            return 0

        # Make sure geometry column is set
        if "geometry" not in gdf.columns:
            logger.error(f"GeoDataFrame has no 'geometry' column. Columns: {list(gdf.columns)}")
            return 0

        # Ensure CRS is WGS84 (EPSG:4326) — what our database expects
        if gdf.crs is None:
            logger.warning("GeoDataFrame has no CRS set. Assuming EPSG:4326.")
            gdf = gdf.set_crs(epsg=4326)
        elif gdf.crs.to_epsg() != 4326:
            logger.info(f"Reprojecting from {gdf.crs} to EPSG:4326")
            gdf = gdf.to_crs(epsg=4326)

        # Write to PostGIS using GeoDataFrame's built-in method
        gdf.to_postgis(
            name=table_name,
            con=engine,
            if_exists="append",      # Add to existing data
            index=False,             # Don't write DataFrame index
            chunksize=500,           # Insert 500 rows at a time
        )

        row_count = len(gdf)
        logger.success(f"Inserted {row_count} rows into '{table_name}'")
        return row_count

    except Exception as e:
        logger.error(f"Failed to bulk insert into '{table_name}': {e}")
        return 0


# ============================================================
# SINGLE ROW INSERTS (for real-time data)
# ============================================================

def insert_traffic_reading(data: dict) -> Optional[int]:
    """
    Insert a single traffic reading into the database.

    Args:
        data: Dictionary with keys:
            - location_name (str): e.g., "Sarjapur Road Junction"
            - lat (float): e.g., 12.91
            - lon (float): e.g., 77.687
            - current_speed (float): km/h
            - free_flow_speed (float): km/h
            - confidence (float): 0.0 to 1.0 (optional)
            - congestion_ratio (float): 0.0 to 1.0 (optional)
            - timestamp (datetime): optional, defaults to now

    Returns:
        ID of inserted row, or None if failed

    Example:
        row_id = insert_traffic_reading({
            "location_name": "Sarjapur Road Junction",
            "lat": 12.91,
            "lon": 77.687,
            "current_speed": 25.5,
            "free_flow_speed": 45.0,
            "congestion_ratio": 0.43,
        })
    """
    session = SessionLocal()
    try:
        reading = TrafficReading(
            location_name=data["location_name"],
            lat=data["lat"],
            lon=data["lon"],
            geometry=from_shape(
                Point(data["lon"], data["lat"]),
                srid=4326,
            ),
            current_speed=data.get("current_speed"),
            free_flow_speed=data.get("free_flow_speed"),
            confidence=data.get("confidence"),
            congestion_ratio=data.get("congestion_ratio"),
            timestamp=data.get("timestamp", datetime.now(timezone.utc)),
        )

        session.add(reading)
        session.commit()
        session.refresh(reading)

        logger.debug(f"Traffic reading inserted: {data['location_name']} (ID: {reading.id})")
        return reading.id

    except Exception as e:
        session.rollback()
        logger.error(f"Failed to insert traffic reading for '{data.get('location_name')}': {e}")
        return None

    finally:
        session.close()


def insert_weather_reading(data: dict) -> Optional[int]:
    """
    Insert a single weather reading into the database.

    Args:
        data: Dictionary with keys:
            - lat (float)
            - lon (float)
            - temperature (float): Celsius
            - humidity (float): percentage
            - pressure (float): hPa (optional)
            - weather_description (str): e.g., "light rain" (optional)
            - wind_speed (float): m/s (optional)
            - rain_1h (float): mm (optional)
            - visibility (float): meters (optional)
            - timestamp (datetime): optional

    Returns:
        ID of inserted row, or None if failed
    """
    session = SessionLocal()
    try:
        reading = WeatherReading(
            geometry=from_shape(
                Point(data["lon"], data["lat"]),
                srid=4326,
            ),
            temperature=data.get("temperature"),
            humidity=data.get("humidity"),
            pressure=data.get("pressure"),
            weather_description=data.get("weather_description"),
            wind_speed=data.get("wind_speed"),
            rain_1h=data.get("rain_1h"),
            visibility=data.get("visibility"),
            timestamp=data.get("timestamp", datetime.now(timezone.utc)),
        )

        session.add(reading)
        session.commit()
        session.refresh(reading)

        logger.debug(f"Weather reading inserted (ID: {reading.id})")
        return reading.id

    except Exception as e:
        session.rollback()
        logger.error(f"Failed to insert weather reading: {e}")
        return None

    finally:
        session.close()


def insert_air_quality_reading(data: dict) -> Optional[int]:
    """
    Insert a single air quality reading into the database.

    Args:
        data: Dictionary with keys:
            - lat (float)
            - lon (float)
            - aqi (int): Air Quality Index 1-5
            - pm25 (float): µg/m³ (optional)
            - pm10 (float): µg/m³ (optional)
            - no2 (float): µg/m³ (optional)
            - o3 (float): µg/m³ (optional)
            - co (float): µg/m³ (optional)
            - so2 (float): µg/m³ (optional)
            - timestamp (datetime): optional

    Returns:
        ID of inserted row, or None if failed
    """
    session = SessionLocal()
    try:
        reading = AirQualityReading(
            geometry=from_shape(
                Point(data["lon"], data["lat"]),
                srid=4326,
            ),
            aqi=data.get("aqi"),
            pm25=data.get("pm25"),
            pm10=data.get("pm10"),
            no2=data.get("no2"),
            o3=data.get("o3"),
            co=data.get("co"),
            so2=data.get("so2"),
            timestamp=data.get("timestamp", datetime.now(timezone.utc)),
        )

        session.add(reading)
        session.commit()
        session.refresh(reading)

        logger.debug(f"AQI reading inserted (ID: {reading.id})")
        return reading.id

    except Exception as e:
        session.rollback()
        logger.error(f"Failed to insert AQI reading: {e}")
        return None

    finally:
        session.close()


# ============================================================
# QUERY FUNCTIONS — Get Data Back Out
# ============================================================

def get_latest_traffic() -> list[dict]:
    """
    Get the most recent traffic reading for EACH monitoring location.

    Returns:
        List of dicts, one per location, with latest readings.
    """
    try:
        query = text("""
            SELECT DISTINCT ON (location_name)
                id,
                location_name,
                lat,
                lon,
                current_speed,
                free_flow_speed,
                confidence,
                congestion_ratio,
                timestamp
            FROM traffic_readings
            ORDER BY location_name, timestamp DESC;
        """)

        with engine.connect() as conn:
            results = conn.execute(query).fetchall()

        readings = []
        for row in results:
            readings.append({
                "id": row.id,
                "location_name": row.location_name,
                "lat": row.lat,
                "lon": row.lon,
                "current_speed": row.current_speed,
                "free_flow_speed": row.free_flow_speed,
                "confidence": row.confidence,
                "congestion_ratio": row.congestion_ratio,
                "timestamp": row.timestamp.isoformat() if row.timestamp else None,
            })

        logger.debug(f"Retrieved latest traffic for {len(readings)} locations")
        return readings

    except Exception as e:
        logger.error(f"Failed to get latest traffic: {e}")
        return []


def get_traffic_history(location_name: str, hours: int = 24) -> pd.DataFrame:
    """
    Get historical traffic data for a specific location.

    Args:
        location_name: Exact name of the monitoring point
        hours: How many hours of history to retrieve (default: 24)

    Returns:
        Pandas DataFrame with traffic readings, sorted by time
    """
    try:
        cutoff_time = datetime.now(timezone.utc) - timedelta(hours=hours)

        query = text("""
            SELECT
                id,
                location_name,
                lat,
                lon,
                current_speed,
                free_flow_speed,
                confidence,
                congestion_ratio,
                timestamp
            FROM traffic_readings
            WHERE location_name = :location
              AND timestamp >= :cutoff
            ORDER BY timestamp ASC;
        """)

        with engine.connect() as conn:
            df = pd.read_sql(query, conn, params={
                "location": location_name,
                "cutoff": cutoff_time,
            })

        logger.debug(f"Retrieved {len(df)} traffic readings for '{location_name}' (last {hours}h)")
        return df

    except Exception as e:
        logger.error(f"Failed to get traffic history for '{location_name}': {e}")
        return pd.DataFrame()


def get_latest_weather() -> list[dict]:
    """
    Get the most recent weather reading for EACH location.
    Since weather_readings has no location_name, we group by geometry.

    Returns:
        List of dicts with latest weather data.
    """
    try:
        query = text("""
            SELECT DISTINCT ON (ST_AsText(geometry))
                id,
                ST_Y(geometry) AS lat,
                ST_X(geometry) AS lon,
                temperature,
                humidity,
                pressure,
                weather_description,
                wind_speed,
                rain_1h,
                visibility,
                timestamp
            FROM weather_readings
            ORDER BY ST_AsText(geometry), timestamp DESC;
        """)

        with engine.connect() as conn:
            results = conn.execute(query).fetchall()

        readings = []
        for row in results:
            readings.append({
                "id": row.id,
                "lat": row.lat,
                "lon": row.lon,
                "temperature": row.temperature,
                "humidity": row.humidity,
                "pressure": row.pressure,
                "weather_description": row.weather_description,
                "wind_speed": row.wind_speed,
                "rain_1h": row.rain_1h,
                "visibility": row.visibility,
                "timestamp": row.timestamp.isoformat() if row.timestamp else None,
            })

        logger.debug(f"Retrieved latest weather for {len(readings)} locations")
        return readings

    except Exception as e:
        logger.error(f"Failed to get latest weather: {e}")
        return []


# ============================================================
# SPATIAL QUERIES — The Power of PostGIS!
# ============================================================

def spatial_query_within_radius(
    lat: float,
    lon: float,
    radius_m: float,
    table_name: str,
) -> gpd.GeoDataFrame:
    """
    Find all features within a radius of a point.
    Uses PostGIS ST_DWithin for fast spatial search.

    Args:
        lat: Latitude of center point
        lon: Longitude of center point
        radius_m: Search radius in meters
        table_name: Which table to search (e.g., "roads", "buildings")

    Returns:
        GeoDataFrame of matching features

    Example:
        buildings = spatial_query_within_radius(12.91, 77.687, 500, "buildings")
    """
    try:
        if table_name not in TABLE_MODEL_MAP:
            logger.error(f"Unknown table: '{table_name}'. Valid: {list(TABLE_MODEL_MAP.keys())}")
            return gpd.GeoDataFrame()

        query = text(f"""
            SELECT *,
                   ST_AsText(geometry) AS geom_wkt,
                   ST_Distance(
                       geometry::geography,
                       ST_SetSRID(ST_MakePoint(:lon, :lat), 4326)::geography
                   ) AS distance_m
            FROM {table_name}
            WHERE ST_DWithin(
                geometry::geography,
                ST_SetSRID(ST_MakePoint(:lon, :lat), 4326)::geography,
                :radius
            )
            ORDER BY distance_m ASC;
        """)

        with engine.connect() as conn:
            df = pd.read_sql(query, conn, params={
                "lat": lat,
                "lon": lon,
                "radius": radius_m,
            })

        if df.empty:
            logger.info(f"No {table_name} found within {radius_m}m of ({lat}, {lon})")
            return gpd.GeoDataFrame()

        # Convert WKT geometry back to GeoDataFrame
        df["geometry"] = df["geom_wkt"].apply(wkt.loads)
        gdf = gpd.GeoDataFrame(df, geometry="geometry", crs="EPSG:4326")
        gdf = gdf.drop(columns=["geom_wkt"])

        logger.debug(f"Found {len(gdf)} {table_name} within {radius_m}m of ({lat}, {lon})")
        return gdf

    except Exception as e:
        logger.error(f"Spatial radius query failed on '{table_name}': {e}")
        return gpd.GeoDataFrame()


def spatial_query_within_polygon(
    polygon_wkt: str,
    table_name: str,
) -> gpd.GeoDataFrame:
    """
    Find all features inside a polygon boundary.
    Uses PostGIS ST_Within for spatial containment.

    Args:
        polygon_wkt: Polygon in WKT format, e.g.:
            "POLYGON((77.68 12.86, 77.77 12.86, 77.77 12.95, 77.68 12.95, 77.68 12.86))"
        table_name: Which table to search

    Returns:
        GeoDataFrame of features inside the polygon

    Example:
        bbox_wkt = "POLYGON((77.68 12.86, 77.77 12.86, 77.77 12.95, 77.68 12.95, 77.68 12.86))"
        pois = spatial_query_within_polygon(bbox_wkt, "points_of_interest")
    """
    try:
        if table_name not in TABLE_MODEL_MAP:
            logger.error(f"Unknown table: '{table_name}'. Valid: {list(TABLE_MODEL_MAP.keys())}")
            return gpd.GeoDataFrame()

        query = text(f"""
            SELECT *,
                   ST_AsText(geometry) AS geom_wkt
            FROM {table_name}
            WHERE ST_Within(
                geometry,
                ST_GeomFromText(:polygon_wkt, 4326)
            );
        """)

        with engine.connect() as conn:
            df = pd.read_sql(query, conn, params={
                "polygon_wkt": polygon_wkt,
            })

        if df.empty:
            logger.info(f"No {table_name} found within the given polygon")
            return gpd.GeoDataFrame()

        # Convert WKT geometry back to GeoDataFrame
        df["geometry"] = df["geom_wkt"].apply(wkt.loads)
        gdf = gpd.GeoDataFrame(df, geometry="geometry", crs="EPSG:4326")
        gdf = gdf.drop(columns=["geom_wkt"])

        logger.debug(f"Found {len(gdf)} {table_name} within polygon")
        return gdf

    except Exception as e:
        logger.error(f"Spatial polygon query failed on '{table_name}': {e}")
        return gpd.GeoDataFrame()


# ============================================================
# UTILITY FUNCTIONS
# ============================================================

def get_table_counts() -> dict:
    """
    Get row counts for all tables. Useful for monitoring.

    Returns:
        Dict like {"roads": 1234, "buildings": 5678, ...}
    """
    counts = {}
    try:
        with engine.connect() as conn:
            for table_name in TABLE_MODEL_MAP.keys():
                result = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}")).scalar()
                counts[table_name] = result

        logger.debug(f"Table counts: {counts}")
        return counts

    except Exception as e:
        logger.error(f"Failed to get table counts: {e}")
        return {}


def clear_table(table_name: str) -> bool:
    """
    Delete all rows from a table. Use with caution!

    Args:
        table_name: Table to clear

    Returns:
        True if successful, False otherwise
    """
    try:
        if table_name not in TABLE_MODEL_MAP:
            logger.error(f"Unknown table: '{table_name}'")
            return False

        with engine.connect() as conn:
            result = conn.execute(text(f"DELETE FROM {table_name}"))
            conn.commit()
            logger.warning(f"Cleared {result.rowcount} rows from '{table_name}'")
            return True

    except Exception as e:
        logger.error(f"Failed to clear table '{table_name}': {e}")
        return False