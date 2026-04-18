"""
Static Data Collector for Urban Intelligence Dashboard.
Downloads one-time datasets from OpenStreetMap using Overpass API directly.
(OSMnx's auto-subdivision is too slow for this area.)

Data is saved as GeoJSON in data/raw/ and returned as GeoDataFrames.
Files are cached — if they already exist, download is skipped.

Usage:
    python src/data_collection/static_collector.py
"""

import sys
import time
from pathlib import Path

# Fix import path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

import geopandas as gpd
import requests
from loguru import logger
from shapely.geometry import Point, LineString, Polygon

from src.config.settings import settings


# ============================================================
# CONFIGURATION
# ============================================================

# Study area bounding box (south, west, north, east)
BBOX = settings.STUDY_AREA_BBOX  # (12.86, 77.68, 12.95, 77.77)
SOUTH, WEST, NORTH, EAST = BBOX
BBOX_STR = f"{SOUTH},{WEST},{NORTH},{EAST}"

# Output directory
RAW_DIR = settings.RAW_DATA_DIR
RAW_DIR.mkdir(parents=True, exist_ok=True)

# Overpass API URL
OVERPASS_URL = settings.OSM_OVERPASS_URL

# Retry settings
MAX_RETRIES = 3
RETRY_DELAY = 15  # seconds


# ============================================================
# HELPER FUNCTIONS
# ============================================================

def _file_exists(filepath: Path) -> bool:
    """Check if a cached file already exists and has data."""
    if filepath.exists() and filepath.stat().st_size > 100:
        logger.info(f"📁 Cache found: {filepath.name} — skipping download")
        return True
    return False


def _save_geojson(gdf: gpd.GeoDataFrame, filepath: Path) -> None:
    """Save a GeoDataFrame as GeoJSON."""
    gdf.to_file(filepath, driver="GeoJSON")
    size_kb = filepath.stat().st_size / 1024
    logger.success(f"💾 Saved: {filepath.name} ({size_kb:.1f} KB, {len(gdf)} features)")


def _load_geojson(filepath: Path) -> gpd.GeoDataFrame:
    """Load a GeoJSON file as a GeoDataFrame."""
    gdf = gpd.read_file(filepath)
    logger.info(f"📂 Loaded from cache: {filepath.name} ({len(gdf)} features)")
    return gdf


def _overpass_query(query: str) -> dict:
    """
    Send a query to the Overpass API with retries.
    """
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            logger.info(f"   Sending Overpass query (attempt {attempt}/{MAX_RETRIES})...")
            response = requests.post(
                OVERPASS_URL,
                data={"data": query},
                timeout=180,
            )
            response.raise_for_status()
            data = response.json()
            element_count = len(data.get("elements", []))
            logger.info(f"   ✅ Received {element_count} elements")
            return data

        except requests.exceptions.Timeout:
            logger.warning(f"   ⚠️  Timeout on attempt {attempt}. Retrying in {RETRY_DELAY}s...")
            time.sleep(RETRY_DELAY)
        except requests.exceptions.HTTPError as e:
            if response.status_code == 429:
                wait = RETRY_DELAY * attempt
                logger.warning(f"   ⚠️  Rate limited. Waiting {wait}s...")
                time.sleep(wait)
            else:
                logger.error(f"   ❌ HTTP error: {e}")
                if attempt == MAX_RETRIES:
                    raise
                time.sleep(RETRY_DELAY)
        except Exception as e:
            logger.error(f"   ❌ Error: {e}")
            if attempt == MAX_RETRIES:
                raise
            time.sleep(RETRY_DELAY)

    raise RuntimeError("All Overpass API attempts failed")


# ============================================================
# FUNCTION 1: ROAD NETWORK
# ============================================================

def collect_road_network() -> gpd.GeoDataFrame:
    """
    Download road network using Overpass API directly.
    Gets all highway types (motorway, primary, secondary, tertiary, residential, etc.)
    
    Cached in data/raw/roads.geojson.
    """
    filepath = RAW_DIR / "roads.geojson"

    if _file_exists(filepath):
        return _load_geojson(filepath)

    logger.info("🛣️  Downloading road network from OpenStreetMap...")
    logger.info(f"   Bounding box: S={SOUTH}, W={WEST}, N={NORTH}, E={EAST}")

    query = f"""
    [out:json][timeout:180];
    (
      way["highway"~"^(motorway|motorway_link|trunk|trunk_link|primary|primary_link|secondary|secondary_link|tertiary|tertiary_link|residential|unclassified|living_street|service)$"]({BBOX_STR});
    );
    out body geom;
    """

    result = _overpass_query(query)
    elements = result.get("elements", [])

    if not elements:
        logger.warning("   No roads found!")
        return gpd.GeoDataFrame()

    # Convert to GeoDataFrame
    features = []
    for elem in elements:
        if elem["type"] != "way" or "geometry" not in elem:
            continue

        try:
            coords = [(pt["lon"], pt["lat"]) for pt in elem["geometry"]]
            if len(coords) < 2:
                continue

            geom = LineString(coords)
            tags = elem.get("tags", {})

            # Handle 'name' that might be a list
            name = tags.get("name", "")
            if isinstance(name, list):
                name = name[0] if name else ""

            features.append({
                "osm_id": elem["id"],
                "name": name,
                "highway": tags.get("highway", ""),
                "maxspeed": tags.get("maxspeed", ""),
                "lanes": tags.get("lanes", ""),
                "oneway": tags.get("oneway", ""),
                "surface": tags.get("surface", ""),
                "geometry": geom,
            })
        except Exception as e:
            logger.debug(f"   Skipped way {elem.get('id')}: {e}")
            continue

    roads_gdf = gpd.GeoDataFrame(features, crs="EPSG:4326")

    # Add road length in meters
    roads_gdf["length_m"] = roads_gdf.geometry.to_crs(epsg=32643).length

    logger.info(f"   ✅ Processed {len(roads_gdf)} road segments")

    # Print highway type summary
    logger.info("   Road types:")
    for htype, count in roads_gdf["highway"].value_counts().items():
        logger.info(f"      {htype}: {count}")

    _save_geojson(roads_gdf, filepath)
    return roads_gdf


# ============================================================
# FUNCTION 2: BUILDINGS
# ============================================================

def collect_buildings() -> gpd.GeoDataFrame:
    """
    Download building footprints using Overpass API directly.
    
    Cached in data/raw/buildings.geojson.
    """
    filepath = RAW_DIR / "buildings.geojson"

    if _file_exists(filepath):
        return _load_geojson(filepath)

    logger.info("🏢 Downloading building footprints from OpenStreetMap...")
    logger.info(f"   Bounding box: S={SOUTH}, W={WEST}, N={NORTH}, E={EAST}")

    query = f"""
    [out:json][timeout:180];
    (
      way["building"]({BBOX_STR});
    );
    out body geom;
    """

    result = _overpass_query(query)
    elements = result.get("elements", [])

    if not elements:
        logger.warning("   No buildings found!")
        return gpd.GeoDataFrame()

    # Convert to GeoDataFrame
    features = []
    for elem in elements:
        if elem["type"] != "way" or "geometry" not in elem:
            continue

        try:
            coords = [(pt["lon"], pt["lat"]) for pt in elem["geometry"]]

            # Need at least 4 points for a valid polygon (3 + closing)
            if len(coords) < 4:
                continue

            # Close the polygon if not already closed
            if coords[0] != coords[-1]:
                coords.append(coords[0])

            geom = Polygon(coords)

            # Skip invalid or tiny geometries
            if not geom.is_valid or geom.area == 0:
                continue

            tags = elem.get("tags", {})

            name = tags.get("name", "")
            if isinstance(name, list):
                name = name[0] if name else ""

            features.append({
                "osm_id": elem["id"],
                "name": name,
                "building": tags.get("building", "yes"),
                "building_levels": tags.get("building:levels", ""),
                "height": tags.get("height", ""),
                "amenity": tags.get("amenity", ""),
                "geometry": geom,
            })
        except Exception as e:
            logger.debug(f"   Skipped building {elem.get('id')}: {e}")
            continue

    buildings_gdf = gpd.GeoDataFrame(features, crs="EPSG:4326")

    # Add area in square meters
    buildings_gdf["area_sqm"] = buildings_gdf.geometry.to_crs(epsg=32643).area

    logger.info(f"   ✅ Processed {len(buildings_gdf)} buildings")

    # Print building type summary
    logger.info("   Building types:")
    for btype, count in buildings_gdf["building"].value_counts().head(10).items():
        logger.info(f"      {btype}: {count}")

    _save_geojson(buildings_gdf, filepath)
    return buildings_gdf


# ============================================================
# FUNCTION 3: POINTS OF INTEREST
# ============================================================

def collect_pois() -> gpd.GeoDataFrame:
    """
    Download Points of Interest using Overpass API.
    
    Categories: hospitals, schools, restaurants, ATMs, bus stops,
    fuel stations, parks, shopping, pharmacies, places of worship.
    
    Cached in data/raw/pois.geojson.
    """
    filepath = RAW_DIR / "pois.geojson"

    if _file_exists(filepath):
        return _load_geojson(filepath)

    logger.info("📍 Downloading Points of Interest from OpenStreetMap...")

    query = f"""
    [out:json][timeout:120];
    (
      node["amenity"="hospital"]({BBOX_STR});
      node["amenity"="clinic"]({BBOX_STR});
      node["amenity"="school"]({BBOX_STR});
      node["amenity"="college"]({BBOX_STR});
      node["amenity"="restaurant"]({BBOX_STR});
      node["amenity"="cafe"]({BBOX_STR});
      node["amenity"="atm"]({BBOX_STR});
      node["amenity"="bank"]({BBOX_STR});
      node["highway"="bus_stop"]({BBOX_STR});
      node["amenity"="fuel"]({BBOX_STR});
      node["leisure"="park"]({BBOX_STR});
      node["leisure"="garden"]({BBOX_STR});
      node["shop"="mall"]({BBOX_STR});
      node["shop"="supermarket"]({BBOX_STR});
      node["amenity"="pharmacy"]({BBOX_STR});
      node["amenity"="place_of_worship"]({BBOX_STR});
    );
    out body;
    """

    logger.info("   Querying 16 POI categories...")

    result = _overpass_query(query)
    elements = result.get("elements", [])

    if not elements:
        logger.warning("   No POIs found!")
        return gpd.GeoDataFrame()

    # Convert to GeoDataFrame
    features = []
    for elem in elements:
        if elem["type"] != "node":
            continue

        tags = elem.get("tags", {})

        # Determine category
        category = "other"
        amenity = tags.get("amenity", "")
        shop = tags.get("shop", "")
        leisure = tags.get("leisure", "")
        highway = tags.get("highway", "")

        if amenity in ["hospital", "clinic", "pharmacy"]:
            category = "healthcare"
        elif amenity in ["school", "college"]:
            category = "education"
        elif amenity in ["restaurant", "cafe"]:
            category = "food"
        elif amenity in ["atm", "bank"]:
            category = "finance"
        elif highway == "bus_stop":
            category = "transport"
        elif amenity == "fuel":
            category = "fuel"
        elif leisure in ["park", "garden"]:
            category = "recreation"
        elif shop in ["mall", "supermarket"]:
            category = "shopping"
        elif amenity == "place_of_worship":
            category = "worship"

        name = tags.get("name", "Unnamed")
        if isinstance(name, list):
            name = name[0] if name else "Unnamed"

        subcategory = amenity or shop or leisure or highway

        features.append({
            "osm_id": elem["id"],
            "name": name,
            "category": category,
            "subcategory": subcategory,
            "lat": elem["lat"],
            "lon": elem["lon"],
            "geometry": Point(elem["lon"], elem["lat"]),
        })

    pois_gdf = gpd.GeoDataFrame(features, crs="EPSG:4326")

    # Print category summary
    logger.info("   POI Summary by Category:")
    for cat, count in pois_gdf["category"].value_counts().items():
        logger.info(f"      {cat}: {count}")

    logger.info(f"   ✅ Total POIs: {len(pois_gdf)}")

    _save_geojson(pois_gdf, filepath)
    return pois_gdf


# ============================================================
# FUNCTION 4: WATER BODIES
# ============================================================

def collect_water_bodies() -> gpd.GeoDataFrame:
    """
    Download water bodies (lakes, tanks, drains, streams) using Overpass API.
    Important for Bengaluru — many lakes (kere) and storm water drains.
    
    Cached in data/raw/water_bodies.geojson.
    """
    filepath = RAW_DIR / "water_bodies.geojson"

    if _file_exists(filepath):
        return _load_geojson(filepath)

    logger.info("💧 Downloading water bodies from OpenStreetMap...")

    query = f"""
    [out:json][timeout:120];
    (
      way["natural"="water"]({BBOX_STR});
      relation["natural"="water"]({BBOX_STR});
      way["landuse"="reservoir"]({BBOX_STR});
      way["water"="lake"]({BBOX_STR});
      way["water"="reservoir"]({BBOX_STR});
      way["water"="pond"]({BBOX_STR});
      way["waterway"="drain"]({BBOX_STR});
      way["waterway"="stream"]({BBOX_STR});
      way["waterway"="canal"]({BBOX_STR});
      way["waterway"="river"]({BBOX_STR});
    );
    out body geom;
    """

    logger.info("   Querying for water features...")

    result = _overpass_query(query)
    elements = result.get("elements", [])

    if not elements:
        logger.warning("   No water bodies found!")
        return gpd.GeoDataFrame()

    # Convert to GeoDataFrame
    features = []
    for elem in elements:
        if "geometry" not in elem:
            continue

        try:
            coords = [(pt["lon"], pt["lat"]) for pt in elem["geometry"]]
            tags = elem.get("tags", {})

            # Determine geometry type
            waterway = tags.get("waterway", "")
            if waterway in ["drain", "stream", "canal", "river"]:
                # Linear water features
                if len(coords) < 2:
                    continue
                geom = LineString(coords)
                water_type = "waterway"
            else:
                # Area water features (lakes, ponds, etc.)
                if len(coords) < 4:
                    continue
                if coords[0] != coords[-1]:
                    coords.append(coords[0])
                geom = Polygon(coords)
                if not geom.is_valid or geom.area == 0:
                    continue
                water_type = "lake"

                # Refine type
                if tags.get("landuse") == "reservoir":
                    water_type = "reservoir"
                elif tags.get("water") == "pond":
                    water_type = "pond"

            name = tags.get("name", "")
            if isinstance(name, list):
                name = name[0] if name else ""

            features.append({
                "osm_id": elem.get("id"),
                "name": name,
                "water_type": water_type,
                "waterway": waterway,
                "geometry": geom,
            })
        except Exception as e:
            logger.debug(f"   Skipped water element {elem.get('id')}: {e}")
            continue

    if not features:
        logger.warning("   No valid water features extracted")
        return gpd.GeoDataFrame()

    water_gdf = gpd.GeoDataFrame(features, crs="EPSG:4326")

    # Print summary
    logger.info("   Water body summary:")
    for wtype, count in water_gdf["water_type"].value_counts().items():
        logger.info(f"      {wtype}: {count}")

    logger.info(f"   ✅ Total water features: {len(water_gdf)}")

    _save_geojson(water_gdf, filepath)
    return water_gdf


# ============================================================
# COLLECT ALL — Run everything at once
# ============================================================

def collect_all_static_data() -> dict:
    """
    Download all static datasets at once.
    
    Returns:
        Dictionary with keys: roads, buildings, pois, water_bodies
        Each value is a GeoDataFrame.
    """
    logger.info("=" * 60)
    logger.info("🌍 STATIC DATA COLLECTION — Starting")
    logger.info(f"   Study area: {settings.STUDY_AREA_NAME}")
    logger.info(f"   Bounding box: ({SOUTH}, {WEST}, {NORTH}, {EAST})")
    logger.info("=" * 60)

    results = {}

    # 1. Roads
    print("\n" + "-" * 60)
    try:
        results["roads"] = collect_road_network()
    except Exception as e:
        logger.error(f"❌ Road network collection failed: {e}")
        results["roads"] = gpd.GeoDataFrame()

    # Pause between queries to avoid rate limiting
    time.sleep(5)

    # 2. Buildings
    print("\n" + "-" * 60)
    try:
        results["buildings"] = collect_buildings()
    except Exception as e:
        logger.error(f"❌ Building collection failed: {e}")
        results["buildings"] = gpd.GeoDataFrame()

    time.sleep(5)

    # 3. POIs
    print("\n" + "-" * 60)
    try:
        results["pois"] = collect_pois()
    except Exception as e:
        logger.error(f"❌ POI collection failed: {e}")
        results["pois"] = gpd.GeoDataFrame()

    time.sleep(5)

    # 4. Water Bodies
    print("\n" + "-" * 60)
    try:
        results["water_bodies"] = collect_water_bodies()
    except Exception as e:
        logger.error(f"❌ Water body collection failed: {e}")
        results["water_bodies"] = gpd.GeoDataFrame()

    # Summary
    print("\n" + "=" * 60)
    logger.info("📊 COLLECTION SUMMARY")
    logger.info("=" * 60)
    for name, gdf in results.items():
        count = len(gdf) if not gdf.empty else 0
        emoji = "✅" if count > 0 else "❌"
        logger.info(f"   {emoji} {name}: {count} features")

    total = sum(len(gdf) for gdf in results.values() if not gdf.empty)
    logger.info(f"\n   🎯 Total features collected: {total}")
    logger.info(f"   📁 Files saved in: {RAW_DIR}")

    return results


# ============================================================
# MAIN — Run if executed directly
# ============================================================

if __name__ == "__main__":
    collect_all_static_data()