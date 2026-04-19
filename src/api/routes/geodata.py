"""
Geodata API routes — serve static OSM layers as GeoJSON.
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent.parent))

from fastapi import APIRouter, Query
from sqlalchemy import text
from typing import Optional
from src.database.connection import engine

router = APIRouter(prefix="/api/v1/geodata", tags=["Geodata"])


# ──────────────────────────────────────
# Helper: rows to GeoJSON
# ──────────────────────────────────────

def rows_to_geojson(rows, property_columns):
    """Convert DB rows to GeoJSON FeatureCollection."""
    features = []
    for row in rows:
        properties = {}
        for col in property_columns:
            val = getattr(row, col, None)
            properties[col] = val

        feature = {
            "type": "Feature",
            "geometry": None,
            "properties": properties
        }

        if hasattr(row, "geojson") and row.geojson:
            import json
            feature["geometry"] = json.loads(row.geojson)

        features.append(feature)

    return {
        "type": "FeatureCollection",
        "features": features,
        "count": len(features)
    }


# ──────────────────────────────────────
# Roads
# ──────────────────────────────────────

@router.get("/roads")
async def get_roads(
    min_lat: Optional[float] = Query(default=None),
    min_lon: Optional[float] = Query(default=None),
    max_lat: Optional[float] = Query(default=None),
    max_lon: Optional[float] = Query(default=None),
    limit: int = Query(default=500, ge=1, le=10000)
):
    """Return road network as GeoJSON. Optional bbox filter."""
    if all([min_lat, min_lon, max_lat, max_lon]):
        query = text("""
            SELECT id, name, road_type,
                   ST_AsGeoJSON(geometry) as geojson
            FROM roads
            WHERE geometry && ST_MakeEnvelope(:min_lon, :min_lat, :max_lon, :max_lat, 4326)
            LIMIT :limit
        """)
        params = {
            "min_lat": min_lat, "min_lon": min_lon,
            "max_lat": max_lat, "max_lon": max_lon,
            "limit": limit
        }
    else:
        query = text("""
            SELECT id, name, road_type,
                   ST_AsGeoJSON(geometry) as geojson
            FROM roads
            LIMIT :limit
        """)
        params = {"limit": limit}

    with engine.connect() as conn:
        rows = conn.execute(query, params).fetchall()

    return rows_to_geojson(rows, ["id", "name", "road_type"])


# ──────────────────────────────────────
# Buildings
# ──────────────────────────────────────

@router.get("/buildings")
async def get_buildings(
    min_lat: Optional[float] = Query(default=None),
    min_lon: Optional[float] = Query(default=None),
    max_lat: Optional[float] = Query(default=None),
    max_lon: Optional[float] = Query(default=None),
    limit: int = Query(default=500, ge=1, le=5000)
):
    """Return buildings as GeoJSON. Optional bbox filter."""
    if all([min_lat, min_lon, max_lat, max_lon]):
        query = text("""
            SELECT id, building_type, area_sqm,
                   ST_AsGeoJSON(geometry) as geojson
            FROM buildings
            WHERE geometry && ST_MakeEnvelope(:min_lon, :min_lat, :max_lon, :max_lat, 4326)
            LIMIT :limit
        """)
        params = {
            "min_lat": min_lat, "min_lon": min_lon,
            "max_lat": max_lat, "max_lon": max_lon,
            "limit": limit
        }
    else:
        query = text("""
            SELECT id, building_type, area_sqm,
                   ST_AsGeoJSON(geometry) as geojson
            FROM buildings
            LIMIT :limit
        """)
        params = {"limit": limit}

    with engine.connect() as conn:
        rows = conn.execute(query, params).fetchall()

    return rows_to_geojson(rows, ["id", "building_type", "area_sqm"])


# ──────────────────────────────────────
# POIs
# ──────────────────────────────────────

@router.get("/pois")
async def get_pois(
    category: Optional[str] = Query(default=None, description="e.g. hospital, school, restaurant"),
    lat: Optional[float] = Query(default=None),
    lon: Optional[float] = Query(default=None),
    radius: float = Query(default=2000, ge=100, le=10000, description="Radius in meters"),
    limit: int = Query(default=1000, ge=1, le=2000)
):
    """Return POIs as GeoJSON. Optional category and location filter."""
    conditions = []
    params = {"limit": limit}

    if category:
        conditions.append("category ILIKE :category")
        params["category"] = f"%{category}%"

    if lat and lon:
        conditions.append("""
            ST_DWithin(
                geometry::geography,
                ST_SetSRID(ST_MakePoint(:lon, :lat), 4326)::geography,
                :radius
            )
        """)
        params["lat"] = lat
        params["lon"] = lon
        params["radius"] = radius

    where_clause = ""
    if conditions:
        where_clause = "WHERE " + " AND ".join(conditions)

    query = text(f"""
        SELECT id, name, category, subcategory,
               ST_AsGeoJSON(geometry) as geojson
        FROM points_of_interest
        {where_clause}
        LIMIT :limit
    """)

    with engine.connect() as conn:
        rows = conn.execute(query, params).fetchall()

    return rows_to_geojson(rows, ["id", "name", "category", "subcategory"])


# ──────────────────────────────────────
# Nearby (spatial query)
# ──────────────────────────────────────

@router.get("/nearby")
async def get_nearby(
    lat: float = Query(..., description="Latitude"),
    lon: float = Query(..., description="Longitude"),
    radius: float = Query(default=1000, ge=100, le=10000, description="Radius in meters"),
    category: Optional[str] = Query(default=None, description="e.g. hospital, school, restaurant"),
    limit: int = Query(default=50, ge=1, le=500)
):
    """Find POIs near a point, sorted by distance."""
    conditions = ["""
        ST_DWithin(
            geometry::geography,
            ST_SetSRID(ST_MakePoint(:lon, :lat), 4326)::geography,
            :radius
        )
    """]
    params = {
        "lat": lat,
        "lon": lon,
        "radius": radius,
        "limit": limit
    }

    if category:
        conditions.append("category ILIKE :category")
        params["category"] = f"%{category}%"

    where_clause = "WHERE " + " AND ".join(conditions)

    query = text(f"""
        SELECT id, name, category, subcategory,
               ST_AsGeoJSON(geometry) as geojson,
               ST_Distance(
                   geometry::geography,
                   ST_SetSRID(ST_MakePoint(:lon, :lat), 4326)::geography
               ) as distance_m
        FROM points_of_interest
        {where_clause}
        ORDER BY distance_m ASC
        LIMIT :limit
    """)

    with engine.connect() as conn:
        rows = conn.execute(query, params).fetchall()

    features = []
    for row in rows:
        import json
        feature = {
            "type": "Feature",
            "geometry": json.loads(row.geojson) if row.geojson else None,
            "properties": {
                "id": row.id,
                "name": row.name,
                "category": row.category,
                "subcategory": row.subcategory,
                "distance_m": round(row.distance_m, 1)
            }
        }
        features.append(feature)

    return {
        "type": "FeatureCollection",
        "features": features,
        "count": len(features),
        "center": {"lat": lat, "lon": lon},
        "radius_m": radius
    }