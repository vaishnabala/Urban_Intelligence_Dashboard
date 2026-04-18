"""
Verify PostGIS data loading - run counts, spatial queries, and check indexes.
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from sqlalchemy import text
from src.database.connection import engine


def run_verification():
    """Run all verification queries."""
    
    with engine.connect() as conn:
        
        # ============================================================
        # 1. ROW COUNTS
        # ============================================================
        print("=" * 60)
        print("1. TABLE ROW COUNTS")
        print("=" * 60)
        
        tables = ["roads", "buildings", "points_of_interest", 
                   "traffic_readings", "weather_readings", 
                   "air_quality_readings", "ward_boundaries", "anomalies"]
        
        for table in tables:
            result = conn.execute(text(f"SELECT count(*) FROM {table}"))
            count = result.scalar()
            print(f"  {table:30s} → {count:,} rows")
        
        # ============================================================
        # 2. SAMPLE DATA CHECK
        # ============================================================
        print("\n" + "=" * 60)
        print("2. SAMPLE ROAD TYPES")
        print("=" * 60)
        
        result = conn.execute(text("""
            SELECT road_type, count(*) as cnt 
            FROM roads 
            GROUP BY road_type 
            ORDER BY cnt DESC
        """))
        for row in result:
            print(f"  {row[0]:30s} → {row[1]:,}")
        
        print("\n" + "=" * 60)
        print("3. SAMPLE BUILDING TYPES")
        print("=" * 60)
        
        result = conn.execute(text("""
            SELECT building_type, count(*) as cnt 
            FROM buildings 
            GROUP BY building_type 
            ORDER BY cnt DESC
            LIMIT 10
        """))
        for row in result:
            print(f"  {row[0]:30s} → {row[1]:,}")
        
        print("\n" + "=" * 60)
        print("4. SAMPLE POI CATEGORIES")
        print("=" * 60)
        
        result = conn.execute(text("""
            SELECT category, count(*) as cnt 
            FROM points_of_interest 
            GROUP BY category 
            ORDER BY cnt DESC
        """))
        for row in result:
            print(f"  {row[0]:30s} → {row[1]:,}")
        
        # ============================================================
        # 5. SPATIAL QUERY — POIs within 1km of Dommasandra Circle
        #    Dommasandra Circle: lat=12.9098, lon=77.7601
        #    ST_MakePoint takes (lon, lat)
        # ============================================================
        print("\n" + "=" * 60)
        print("5. SPATIAL QUERY: POIs within 1km of Dommasandra Circle")
        print("   (lat=12.9098, lon=77.7601)")
        print("=" * 60)
        
        result = conn.execute(text("""
            SELECT name, category 
            FROM points_of_interest 
            WHERE ST_DWithin(
                geometry, 
                ST_SetSRID(ST_MakePoint(77.7601, 12.9098), 4326)::geography, 
                1000
            )
            ORDER BY category, name
        """))
        rows = result.fetchall()
        print(f"  Found {len(rows)} POIs within 1km:")
        for row in rows:
            name = row[0] if row[0] else "(unnamed)"
            print(f"    • {name:40s} [{row[1]}]")
        
        if len(rows) == 0:
            print("  (No results — this is okay if there are no POIs in that area)")
        
        # ============================================================
        # 6. CHECK SPATIAL INDEXES
        # ============================================================
        print("\n" + "=" * 60)
        print("6. SPATIAL INDEXES CHECK")
        print("=" * 60)
        
        result = conn.execute(text("""
            SELECT 
                tablename, 
                indexname, 
                indexdef
            FROM pg_indexes 
            WHERE indexdef LIKE '%gist%' 
               OR indexname LIKE '%geometry%'
               OR indexname LIKE '%spatial%'
            ORDER BY tablename, indexname
        """))
        rows = result.fetchall()
        
        if rows:
            print(f"  Found {len(rows)} spatial index(es):")
            for row in rows:
                print(f"    Table: {row[0]}")
                print(f"    Index: {row[1]}")
                print(f"    Definition: {row[2][:100]}...")
                print()
        else:
            print("  ⚠️  No spatial indexes found!")
            print("  We may need to create them manually.")
        
        # ============================================================
        # 7. GEOMETRY VALIDATION
        # ============================================================
        print("=" * 60)
        print("7. GEOMETRY VALIDATION")
        print("=" * 60)
        
        for table in ["roads", "buildings", "points_of_interest"]:
            result = conn.execute(text(f"""
                SELECT count(*) FROM {table} WHERE geometry IS NOT NULL
            """))
            geo_count = result.scalar()
            
            result = conn.execute(text(f"""
                SELECT count(*) FROM {table}
            """))
            total = result.scalar()
            
            status = "✅" if geo_count == total else "⚠️"
            print(f"  {status} {table}: {geo_count}/{total} rows have geometry")
    
    print("\n" + "=" * 60)
    print("VERIFICATION COMPLETE")
    print("=" * 60)


if __name__ == "__main__":
    run_verification()
    