"""
Geo Processor for Urban Intelligence Dashboard.
Cleans, standardizes, and processes raw GeoDataFrames before database ingestion.

Usage:
    from src.processing.geo_processor import (
        clean_road_network,
        clean_buildings,
        clean_pois,
        save_processed,
    )
"""

import sys
from pathlib import Path

# Fix import path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

import geopandas as gpd
import pandas as pd
from loguru import logger

from src.config.settings import settings


# ============================================================
# CONFIGURATION
# ============================================================

PROCESSED_DIR = settings.PROCESSED_DATA_DIR
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

# UTM zone for Bengaluru (Zone 43N) — used for area/length calculations in meters
UTM_CRS = "EPSG:32643"


# ============================================================
# ROAD TYPE STANDARDIZATION MAPPING
# ============================================================

ROAD_TYPE_MAP = {
    # Major roads
    "motorway": "Motorway",
    "motorway_link": "Motorway Link",
    "trunk": "Trunk Road",
    "trunk_link": "Trunk Road Link",
    "primary": "Primary Road",
    "primary_link": "Primary Road Link",
    
    # Medium roads
    "secondary": "Secondary Road",
    "secondary_link": "Secondary Road Link",
    "tertiary": "Tertiary Road",
    "tertiary_link": "Tertiary Road Link",
    
    # Local roads
    "residential": "Residential",
    "unclassified": "Unclassified",
    "living_street": "Living Street",
    "service": "Service Road",
}

# Road hierarchy for sorting/filtering (lower = more important)
ROAD_HIERARCHY = {
    "Motorway": 1,
    "Motorway Link": 2,
    "Trunk Road": 3,
    "Trunk Road Link": 4,
    "Primary Road": 5,
    "Primary Road Link": 6,
    "Secondary Road": 7,
    "Secondary Road Link": 8,
    "Tertiary Road": 9,
    "Tertiary Road Link": 10,
    "Residential": 11,
    "Unclassified": 12,
    "Living Street": 13,
    "Service Road": 14,
}


# ============================================================
# FUNCTION 1: CLEAN ROAD NETWORK
# ============================================================

def clean_road_network(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Clean and standardize road network data.

    Steps:
        1. Keep only useful columns
        2. Standardize road type names
        3. Parse lanes and maxspeed to numeric
        4. Ensure CRS is EPSG:4326
        5. Remove duplicate geometries
        6. Add road hierarchy ranking
        7. Recalculate length in meters

    Args:
        gdf: Raw road network GeoDataFrame

    Returns:
        Cleaned GeoDataFrame
    """
    logger.info("🛣️  Cleaning road network...")
    original_count = len(gdf)

    if gdf.empty:
        logger.warning("   Empty GeoDataFrame — nothing to clean")
        return gdf

    # --- 1. Keep only useful columns ---
    keep_cols = {
        "osm_id": "osm_id",
        "name": "name",
        "highway": "highway",
        "maxspeed": "maxspeed",
        "lanes": "lanes",
        "oneway": "oneway",
        "surface": "surface",
        "length_m": "length_m",
        "geometry": "geometry",
    }

    # Only keep columns that exist
    available = {k: v for k, v in keep_cols.items() if k in gdf.columns}
    gdf = gdf[list(available.keys())].copy()

    # --- 2. Standardize road type names ---
    if "highway" in gdf.columns:
        gdf["road_type"] = gdf["highway"].map(ROAD_TYPE_MAP).fillna("Other")
        gdf["road_hierarchy"] = gdf["road_type"].map(ROAD_HIERARCHY).fillna(99)
    else:
        gdf["road_type"] = "Unknown"
        gdf["road_hierarchy"] = 99

    # --- 3. Parse lanes and maxspeed to numeric ---
    if "lanes" in gdf.columns:
        gdf["lanes"] = pd.to_numeric(gdf["lanes"], errors="coerce")

    if "maxspeed" in gdf.columns:
        # maxspeed can be like "40", "40 mph", "none", etc.
        gdf["maxspeed"] = (
            gdf["maxspeed"]
            .astype(str)
            .str.extract(r"(\d+)", expand=False)
        )
        gdf["maxspeed"] = pd.to_numeric(gdf["maxspeed"], errors="coerce")

    # --- 4. Parse oneway ---
    if "oneway" in gdf.columns:
        gdf["oneway"] = gdf["oneway"].map({
            "yes": True,
            "no": False,
            "True": True,
            "False": False,
            "-1": True,
        }).fillna(False)
    else:
        gdf["oneway"] = False

    # --- 5. Clean name column ---
    if "name" in gdf.columns:
        # Handle list-type names
        gdf["name"] = gdf["name"].apply(
            lambda x: x[0] if isinstance(x, list) else (str(x) if pd.notna(x) else "")
        )
        gdf["name"] = gdf["name"].replace({"": "Unnamed Road", "nan": "Unnamed Road"})
    else:
        gdf["name"] = "Unnamed Road"

    # --- 6. Ensure CRS is EPSG:4326 ---
    if gdf.crs is None:
        gdf = gdf.set_crs(epsg=4326)
    elif gdf.crs.to_epsg() != 4326:
        gdf = gdf.to_crs(epsg=4326)

    # --- 7. Keep only LineString geometries ---
    gdf = gdf[gdf.geometry.geom_type.isin(["LineString", "MultiLineString"])].copy()

    # --- 8. Remove duplicate geometries ---
    gdf["geom_wkt"] = gdf.geometry.apply(lambda g: g.wkt)
    before_dedup = len(gdf)
    gdf = gdf.drop_duplicates(subset="geom_wkt").copy()
    gdf = gdf.drop(columns=["geom_wkt"])
    dupes_removed = before_dedup - len(gdf)

    # --- 9. Recalculate length in meters ---
    gdf["length_m"] = gdf.geometry.to_crs(UTM_CRS).length
    gdf["length_m"] = gdf["length_m"].round(1)

    # --- 10. Reset index ---
    gdf = gdf.reset_index(drop=True)

    # --- Summary ---
    logger.info(f"   Original:    {original_count} roads")
    logger.info(f"   Duplicates:  {dupes_removed} removed")
    logger.info(f"   Final:       {len(gdf)} roads")
    logger.info(f"   Total length: {gdf['length_m'].sum() / 1000:.1f} km")

    logger.info("   Road types:")
    for rtype, count in gdf["road_type"].value_counts().items():
        logger.info(f"      {rtype}: {count}")

    logger.success(f"   ✅ Road network cleaned: {len(gdf)} roads")
    return gdf


# ============================================================
# FUNCTION 2: CLEAN BUILDINGS
# ============================================================

def clean_buildings(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Clean and standardize building footprint data.

    Steps:
        1. Keep only useful columns
        2. Standardize building type
        3. Parse building levels to numeric
        4. Calculate area in square meters (UTM projection)
        5. Ensure CRS is EPSG:4326
        6. Remove invalid/tiny geometries
        7. Remove duplicates

    Args:
        gdf: Raw buildings GeoDataFrame

    Returns:
        Cleaned GeoDataFrame
    """
    logger.info("🏢 Cleaning buildings...")
    original_count = len(gdf)

    if gdf.empty:
        logger.warning("   Empty GeoDataFrame — nothing to clean")
        return gdf

    # --- 1. Keep only useful columns ---
    available_cols = []
    for col in ["osm_id", "name", "building", "building_levels", "height", "amenity", "area_sqm", "geometry"]:
        if col in gdf.columns:
            available_cols.append(col)
    gdf = gdf[available_cols].copy()

    # --- 2. Standardize building type ---
    if "building" in gdf.columns:
        # Map common building types
        building_type_map = {
            "yes": "Unknown",
            "residential": "Residential",
            "house": "Residential",
            "apartments": "Residential",
            "commercial": "Commercial",
            "retail": "Commercial",
            "office": "Commercial",
            "industrial": "Industrial",
            "warehouse": "Industrial",
            "school": "Education",
            "university": "Education",
            "college": "Education",
            "hospital": "Healthcare",
            "clinic": "Healthcare",
            "church": "Religious",
            "temple": "Religious",
            "mosque": "Religious",
            "garage": "Utility",
            "shed": "Utility",
            "roof": "Utility",
        }
        gdf["building_type"] = gdf["building"].map(building_type_map).fillna("Other")
    else:
        gdf["building_type"] = "Unknown"

    # --- 3. Parse building levels ---
    if "building_levels" in gdf.columns:
        gdf["levels"] = pd.to_numeric(gdf["building_levels"], errors="coerce")
    elif "height" in gdf.columns:
        # Estimate levels from height (assume 3m per floor)
        height_numeric = pd.to_numeric(
            gdf["height"].astype(str).str.extract(r"(\d+\.?\d*)", expand=False),
            errors="coerce"
        )
        gdf["levels"] = (height_numeric / 3).round(0)
    else:
        gdf["levels"] = None

    # --- 4. Clean name ---
    if "name" in gdf.columns:
        gdf["name"] = gdf["name"].apply(
            lambda x: x[0] if isinstance(x, list) else (str(x) if pd.notna(x) else "")
        )
        gdf["name"] = gdf["name"].replace({"": "Unnamed", "nan": "Unnamed"})
    else:
        gdf["name"] = "Unnamed"

    # --- 5. Ensure CRS is EPSG:4326 ---
    if gdf.crs is None:
        gdf = gdf.set_crs(epsg=4326)
    elif gdf.crs.to_epsg() != 4326:
        gdf = gdf.to_crs(epsg=4326)

    # --- 6. Keep only Polygon/MultiPolygon geometries ---
    gdf = gdf[gdf.geometry.geom_type.isin(["Polygon", "MultiPolygon"])].copy()

    # --- 7. Remove invalid geometries ---
    gdf = gdf[gdf.geometry.is_valid].copy()

    # --- 8. Calculate area in square meters ---
    gdf["area_sqm"] = gdf.geometry.to_crs(UTM_CRS).area
    gdf["area_sqm"] = gdf["area_sqm"].round(1)

    # Remove tiny buildings (< 5 sq meters — likely errors)
    before_tiny = len(gdf)
    gdf = gdf[gdf["area_sqm"] >= 5].copy()
    tiny_removed = before_tiny - len(gdf)

    # --- 9. Remove duplicate geometries ---
    gdf["geom_wkt"] = gdf.geometry.apply(lambda g: g.wkt)
    before_dedup = len(gdf)
    gdf = gdf.drop_duplicates(subset="geom_wkt").copy()
    gdf = gdf.drop(columns=["geom_wkt"])
    dupes_removed = before_dedup - len(gdf)

    # --- 10. Reset index ---
    gdf = gdf.reset_index(drop=True)

    # --- Summary ---
    logger.info(f"   Original:      {original_count} buildings")
    logger.info(f"   Tiny removed:  {tiny_removed} (< 5 sqm)")
    logger.info(f"   Dupes removed: {dupes_removed}")
    logger.info(f"   Final:         {len(gdf)} buildings")
    logger.info(f"   Total area:    {gdf['area_sqm'].sum() / 1_000_000:.2f} sq km")
    logger.info(f"   Avg area:      {gdf['area_sqm'].mean():.0f} sqm")

    logger.info("   Building types:")
    for btype, count in gdf["building_type"].value_counts().items():
        logger.info(f"      {btype}: {count}")

    logger.success(f"   ✅ Buildings cleaned: {len(gdf)} buildings")
    return gdf


# ============================================================
# FUNCTION 3: CLEAN POIS
# ============================================================

def clean_pois(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Clean and standardize Points of Interest data.

    Steps:
        1. Keep only useful columns
        2. Standardize category names
        3. Convert Polygon geometries to centroids (keep only Points)
        4. Remove POIs without names
        5. Ensure CRS is EPSG:4326
        6. Remove duplicates

    Args:
        gdf: Raw POIs GeoDataFrame

    Returns:
        Cleaned GeoDataFrame
    """
    logger.info("📍 Cleaning Points of Interest...")
    original_count = len(gdf)

    if gdf.empty:
        logger.warning("   Empty GeoDataFrame — nothing to clean")
        return gdf

    # --- 1. Keep only useful columns ---
    available_cols = []
    for col in ["osm_id", "name", "category", "subcategory", "lat", "lon", "geometry"]:
        if col in gdf.columns:
            available_cols.append(col)
    gdf = gdf[available_cols].copy()

    # --- 2. Standardize category names ---
    category_map = {
        "healthcare": "Healthcare",
        "education": "Education",
        "food": "Food & Dining",
        "finance": "Finance",
        "transport": "Transport",
        "fuel": "Fuel Station",
        "recreation": "Recreation",
        "shopping": "Shopping",
        "worship": "Place of Worship",
        "other": "Other",
    }

    if "category" in gdf.columns:
        gdf["category"] = gdf["category"].str.lower().map(category_map).fillna("Other")
    else:
        gdf["category"] = "Other"

    # --- 3. Standardize subcategory ---
    if "subcategory" in gdf.columns:
        subcategory_map = {
            "hospital": "Hospital",
            "clinic": "Clinic",
            "pharmacy": "Pharmacy",
            "school": "School",
            "college": "College",
            "restaurant": "Restaurant",
            "cafe": "Cafe",
            "atm": "ATM",
            "bank": "Bank",
            "bus_stop": "Bus Stop",
            "fuel": "Fuel Station",
            "park": "Park",
            "garden": "Garden",
            "mall": "Mall",
            "supermarket": "Supermarket",
            "place_of_worship": "Place of Worship",
        }
        gdf["subcategory"] = gdf["subcategory"].map(subcategory_map).fillna("Other")
    else:
        gdf["subcategory"] = "Other"

    # --- 4. Convert non-Point geometries to centroids ---
    non_points = gdf[~gdf.geometry.geom_type.isin(["Point"])].index
    if len(non_points) > 0:
        logger.info(f"   Converting {len(non_points)} non-Point geometries to centroids")
        gdf.loc[non_points, "geometry"] = gdf.loc[non_points].geometry.centroid

    # --- 5. Clean name column ---
    if "name" in gdf.columns:
        gdf["name"] = gdf["name"].apply(
            lambda x: x[0] if isinstance(x, list) else (str(x) if pd.notna(x) else "")
        )
        # Remove POIs without names (or named "Unnamed")
        before_name = len(gdf)
        gdf = gdf[
            (gdf["name"] != "") &
            (gdf["name"] != "Unnamed") &
            (gdf["name"] != "nan") &
            (gdf["name"].notna())
        ].copy()
        unnamed_removed = before_name - len(gdf)
        logger.info(f"   Removed {unnamed_removed} unnamed POIs")
    else:
        gdf["name"] = "Unknown"

    # --- 6. Ensure CRS is EPSG:4326 ---
    if gdf.crs is None:
        gdf = gdf.set_crs(epsg=4326)
    elif gdf.crs.to_epsg() != 4326:
        gdf = gdf.to_crs(epsg=4326)

    # --- 7. Update lat/lon from geometry ---
    gdf["lat"] = gdf.geometry.y
    gdf["lon"] = gdf.geometry.x

    # --- 8. Remove duplicate geometries ---
    gdf["geom_wkt"] = gdf.geometry.apply(lambda g: g.wkt)
    before_dedup = len(gdf)
    gdf = gdf.drop_duplicates(subset="geom_wkt").copy()
    gdf = gdf.drop(columns=["geom_wkt"])
    dupes_removed = before_dedup - len(gdf)

    # --- 9. Reset index ---
    gdf = gdf.reset_index(drop=True)

    # --- Summary ---
    logger.info(f"   Original:      {original_count} POIs")
    logger.info(f"   Dupes removed: {dupes_removed}")
    logger.info(f"   Final:         {len(gdf)} POIs")

    logger.info("   Categories:")
    for cat, count in gdf["category"].value_counts().items():
        logger.info(f"      {cat}: {count}")

    logger.success(f"   ✅ POIs cleaned: {len(gdf)} POIs")
    return gdf


# ============================================================
# FUNCTION 4: SAVE PROCESSED DATA
# ============================================================

def save_processed(gdf: gpd.GeoDataFrame, filename: str) -> dict:
    """
    Save a cleaned GeoDataFrame to data/processed/ in multiple formats.

    Saves as:
        - GeoJSON (.geojson) — human-readable, good for web maps
        - GeoParquet (.parquet) — fast, compact, good for analysis

    Args:
        gdf: Cleaned GeoDataFrame
        filename: Base filename without extension (e.g., "roads", "buildings")

    Returns:
        Dict with file paths: {"geojson": Path, "parquet": Path}
    """
    if gdf.empty:
        logger.warning(f"   Empty GeoDataFrame — skipping save for '{filename}'")
        return {}

    paths = {}

    # --- GeoJSON ---
    geojson_path = PROCESSED_DIR / f"{filename}.geojson"
    try:
        gdf.to_file(geojson_path, driver="GeoJSON")
        size_kb = geojson_path.stat().st_size / 1024
        logger.success(f"   💾 Saved: {geojson_path.name} ({size_kb:.1f} KB)")
        paths["geojson"] = geojson_path
    except Exception as e:
        logger.error(f"   ❌ Failed to save GeoJSON: {e}")

    # --- GeoParquet ---
    parquet_path = PROCESSED_DIR / f"{filename}.parquet"
    try:
        gdf.to_parquet(parquet_path)
        size_kb = parquet_path.stat().st_size / 1024
        logger.success(f"   💾 Saved: {filename}.parquet ({size_kb:.1f} KB)")
        paths["parquet"] = parquet_path
    except Exception as e:
        logger.error(f"   ❌ Failed to save Parquet: {e}")

    return paths


# ============================================================
# PROCESS ALL — Run full pipeline
# ============================================================

def process_all_static_data() -> dict:
    """
    Load raw data, clean it, and save processed versions.

    Returns:
        Dictionary with cleaned GeoDataFrames
    """
    RAW_DIR = settings.RAW_DATA_DIR

    logger.info("=" * 60)
    logger.info("🔧 GEO PROCESSING — Starting")
    logger.info("=" * 60)

    results = {}

    # 1. Roads
    print("\n" + "-" * 60)
    roads_path = RAW_DIR / "roads.geojson"
    if roads_path.exists():
        raw_roads = gpd.read_file(roads_path)
        roads = clean_road_network(raw_roads)
        save_processed(roads, "roads")
        results["roads"] = roads
    else:
        logger.warning("   ⚠️  roads.geojson not found in data/raw/")

    # 2. Buildings
    print("\n" + "-" * 60)
    buildings_path = RAW_DIR / "buildings.geojson"
    if buildings_path.exists():
        raw_buildings = gpd.read_file(buildings_path)
        buildings = clean_buildings(raw_buildings)
        save_processed(buildings, "buildings")
        results["buildings"] = buildings
    else:
        logger.warning("   ⚠️  buildings.geojson not found in data/raw/")

    # 3. POIs
    print("\n" + "-" * 60)
    pois_path = RAW_DIR / "pois.geojson"
    if pois_path.exists():
        raw_pois = gpd.read_file(pois_path)
        pois = clean_pois(raw_pois)
        save_processed(pois, "pois")
        results["pois"] = pois
    else:
        logger.warning("   ⚠️  pois.geojson not found in data/raw/")

    # Summary
    print("\n" + "=" * 60)
    logger.info("📊 PROCESSING SUMMARY")
    logger.info("=" * 60)
    for name, gdf in results.items():
        count = len(gdf) if not gdf.empty else 0
        emoji = "✅" if count > 0 else "❌"
        logger.info(f"   {emoji} {name}: {count} features (cleaned)")

    logger.info(f"\n   📁 Processed files saved in: {PROCESSED_DIR}")

    return results


# ============================================================
# MAIN
# ============================================================

if __name__ == "__main__":
    process_all_static_data()
    