"""
Load processed static data into PostGIS database.
Reads cleaned GeoJSON files and bulk-inserts into database tables.

Run from project root: python scripts/load_static_to_db.py
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import geopandas as gpd
from loguru import logger
from sqlalchemy import text

from src.database.connection import engine
from src.database.operations import (
    bulk_insert_geodata,
    get_table_counts,
    spatial_query_within_radius,
    clear_table,
)
from src.config.settings import settings


PROCESSED_DIR = settings.PROCESSED_DATA_DIR


def load_roads() -> int:
    """Load processed roads into the roads table."""
    
    filepath = PROCESSED_DIR / "roads.geojson"
    if not filepath.exists():
        logger.error(f"❌ File not found: {filepath}")
        return 0

    logger.info("🛣️  Loading roads into database...")
    
    gdf = gpd.read_file(filepath)
    logger.info(f"   Read {len(gdf)} roads from {filepath.name}")

    # Map GeoDataFrame columns to database columns
    # DB expects: name, road_type, speed_limit, geometry
    roads_db = gpd.GeoDataFrame({
        "name": gdf["name"] if "name" in gdf.columns else "Unnamed",
        "road_type": gdf["road_type"] if "road_type" in gdf.columns else "Unknown",
        "speed_limit": gdf["maxspeed"].astype("Int64") if "maxspeed" in gdf.columns else None,
        "geometry": gdf["geometry"],
    }, crs=gdf.crs)

    # Truncate name to 255 chars (DB column limit)
    roads_db["name"] = roads_db["name"].astype(str).str[:255]
    roads_db["road_type"] = roads_db["road_type"].astype(str).str[:100]

    count = bulk_insert_geodata("roads", roads_db)
    return count


def load_buildings() -> int:
    """Load processed buildings into the buildings table."""
    
    filepath = PROCESSED_DIR / "buildings.geojson"
    if not filepath.exists():
        logger.error(f"❌ File not found: {filepath}")
        return 0

    logger.info("🏢 Loading buildings into database...")
    
    gdf = gpd.read_file(filepath)
    logger.info(f"   Read {len(gdf)} buildings from {filepath.name}")

    # Map GeoDataFrame columns to database columns
    # DB expects: building_type, area_sqm, geometry
    buildings_db = gpd.GeoDataFrame({
        "building_type": gdf["building_type"] if "building_type" in gdf.columns else "Unknown",
        "area_sqm": gdf["area_sqm"] if "area_sqm" in gdf.columns else 0.0,
        "geometry": gdf["geometry"],
    }, crs=gdf.crs)

    # Truncate to DB column limits
    buildings_db["building_type"] = buildings_db["building_type"].astype(str).str[:100]

    count = bulk_insert_geodata("buildings", buildings_db)
    return count


def load_pois() -> int:
    """Load processed POIs into the points_of_interest table."""
    
    filepath = PROCESSED_DIR / "pois.geojson"
    if not filepath.exists():
        logger.error(f"❌ File not found: {filepath}")
        return 0

    logger.info("📍 Loading POIs into database...")
    
    gdf = gpd.read_file(filepath)
    logger.info(f"   Read {len(gdf)} POIs from {filepath.name}")

    # Map GeoDataFrame columns to database columns
    # DB expects: name, category, subcategory, geometry
    pois_db = gpd.GeoDataFrame({
        "name": gdf["name"] if "name" in gdf.columns else "Unnamed",
        "category": gdf["category"] if "category" in gdf.columns else "Other",
        "subcategory": gdf["subcategory"] if "subcategory" in gdf.columns else "Other",
        "geometry": gdf["geometry"],
    }, crs=gdf.crs)

    # Truncate to DB column limits
    pois_db["name"] = pois_db["name"].astype(str).str[:255]
    pois_db["category"] = pois_db["category"].astype(str).str[:100]
    pois_db["subcategory"] = pois_db["subcategory"].astype(str).str[:100]

    count = bulk_insert_geodata("points_of_interest", pois_db)
    return count


def verify_spatial_query():
    """
    Run a sample spatial query to verify PostGIS is working:
    'Find all hospitals within 2km of Dommasandra Circle'
    """
    logger.info("\n🔍 SPATIAL QUERY TEST")
    logger.info("   Query: Find all Healthcare POIs within 2km of Dommasandra Circle")

    # Dommasandra Circle coordinates
    lat, lon = settings.MONITORING_POINTS["dommasandra_circle"]
    
    gdf = spatial_query_within_radius(lat, lon, 2000, "points_of_interest")

    if gdf.empty:
        logger.info("   No POIs found within 2km (table may have no healthcare POIs nearby)")
        
        # Try a wider search as fallback
        logger.info("\n   Trying wider search: All POIs within 5km...")
        gdf = spatial_query_within_radius(lat, lon, 5000, "points_of_interest")

    if gdf.empty:
        logger.warning("   Still no results. POIs may not have been loaded correctly.")
        return

    # Filter healthcare if we have a category column
    if "category" in gdf.columns:
        healthcare = gdf[gdf["category"] == "Healthcare"]
        logger.info(f"\n   📋 Healthcare POIs found: {len(healthcare)}")
        if not healthcare.empty:
            for _, row in healthcare.iterrows():
                dist = row.get("distance_m", "?")
                name = row.get("name", "Unknown")
                logger.info(f"      🏥 {name} — {float(dist):.0f}m away")

    # Show all categories found
    if "category" in gdf.columns:
        logger.info(f"\n   📋 All POIs within range: {len(gdf)}")
        for cat, count in gdf["category"].value_counts().items():
            logger.info(f"      {cat}: {count}")

    # Show sample rows
    display_cols = [c for c in ["name", "category", "subcategory", "distance_m"] if c in gdf.columns]
    if display_cols:
        logger.info(f"\n   📋 First 10 results:")
        sample = gdf[display_cols].head(10)
        for _, row in sample.iterrows():
            parts = [f"{col}={row[col]}" for col in display_cols]
            logger.info(f"      {', '.join(parts)}")


def main():
    print("\n" + "=" * 65)
    print("📥 LOADING STATIC DATA INTO POSTGIS")
    print("=" * 65)

    # Check if tables already have data
    counts_before = get_table_counts()
    roads_existing = counts_before.get("roads", 0)
    buildings_existing = counts_before.get("buildings", 0)
    pois_existing = counts_before.get("points_of_interest", 0)

    if roads_existing > 0 or buildings_existing > 0 or pois_existing > 0:
        logger.warning(f"\n⚠️  Tables already have data:")
        logger.warning(f"   roads: {roads_existing} rows")
        logger.warning(f"   buildings: {buildings_existing} rows")
        logger.warning(f"   points_of_interest: {pois_existing} rows")
        
        response = input("\n   Clear existing data and reload? (yes/no): ").strip().lower()
        if response == "yes":
            logger.info("   Clearing existing data...")
            clear_table("roads")
            clear_table("buildings")
            clear_table("points_of_interest")
        else:
            logger.info("   Keeping existing data. New data will be appended.")

    # Load each dataset
    print("\n" + "-" * 65)
    roads_count = load_roads()

    print("\n" + "-" * 65)
    buildings_count = load_buildings()

    print("\n" + "-" * 65)
    pois_count = load_pois()

    # Print row counts after insertion
    print("\n" + "=" * 65)
    logger.info("📊 ROW COUNTS AFTER INSERTION")
    print("=" * 65)
    
    counts_after = get_table_counts()
    for table, count in counts_after.items():
        emoji = "📊" if count > 0 else "📭"
        logger.info(f"   {emoji}  {table:25s}  {count:>8,} rows")

    # Run spatial query test
    print("\n" + "-" * 65)
    verify_spatial_query()

    # Final summary
    print("\n" + "=" * 65)
    logger.info("🎉 STATIC DATA LOADING COMPLETE!")
    print("=" * 65)
    logger.info(f"   🛣️  Roads:     {roads_count:,} inserted")
    logger.info(f"   🏢 Buildings: {buildings_count:,} inserted")
    logger.info(f"   📍 POIs:      {pois_count:,} inserted")
    logger.info(f"   Total:       {roads_count + buildings_count + pois_count:,} features in PostGIS")
    print()


if __name__ == "__main__":
    main()