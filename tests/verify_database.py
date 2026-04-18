"""
Verify that all database tables and spatial columns exist in PostGIS.
Run from project root: python tests/verify_database.py
"""

import sys
from pathlib import Path

# Fix import path so we can find src/
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from sqlalchemy import text, inspect
from src.database.connection import engine
from src.database.models import Base


def verify_tables():
    """Check that all 8 expected tables exist in the database."""
    
    expected_tables = [
        "roads",
        "buildings",
        "points_of_interest",
        "ward_boundaries",
        "traffic_readings",
        "weather_readings",
        "air_quality_readings",
        "anomalies",
    ]
    
    inspector = inspect(engine)
    actual_tables = inspector.get_table_names()
    
    print("=" * 60)
    print("TABLE VERIFICATION")
    print("=" * 60)
    
    all_good = True
    for table in expected_tables:
        if table in actual_tables:
            print(f"  ✅  {table}")
        else:
            print(f"  ❌  {table}  — MISSING!")
            all_good = False
    
    print(f"\nExpected: {len(expected_tables)} tables")
    print(f"Found:    {len([t for t in expected_tables if t in actual_tables])} tables")
    
    return all_good


def verify_spatial_columns():
    """Check that geometry columns are registered in PostGIS."""
    
    print("\n" + "=" * 60)
    print("SPATIAL COLUMN VERIFICATION")
    print("=" * 60)
    
    query = text("""
        SELECT 
            f_table_name AS table_name,
            f_geometry_column AS column_name,
            type AS geometry_type,
            srid
        FROM geometry_columns
        WHERE f_table_schema = 'public'
        ORDER BY f_table_name;
    """)
    
    with engine.connect() as conn:
        results = conn.execute(query).fetchall()
    
    if not results:
        print("  ❌  No spatial columns found!")
        return False
    
    for row in results:
        table_name, col_name, geom_type, srid = row
        print(f"  ✅  {table_name}.{col_name}  →  {geom_type} (SRID: {srid})")
    
    print(f"\nTotal spatial columns: {len(results)}")
    return True


def verify_indexes():
    """Check that spatial and time-based indexes exist."""
    
    print("\n" + "=" * 60)
    print("INDEX VERIFICATION")
    print("=" * 60)
    
    query = text("""
        SELECT 
            tablename,
            indexname
        FROM pg_indexes
        WHERE schemaname = 'public'
          AND (
              indexname LIKE 'idx_%'
              OR indexname LIKE 'ix_%'
          )
        ORDER BY tablename, indexname;
    """)
    
    with engine.connect() as conn:
        results = conn.execute(query).fetchall()
    
    if not results:
        print("  ⚠️  No custom indexes found (only primary keys)")
        return False
    
    current_table = ""
    for row in results:
        table_name, index_name = row
        if table_name != current_table:
            current_table = table_name
            print(f"\n  📁 {table_name}:")
        print(f"      ✅  {index_name}")
    
    print(f"\nTotal custom indexes: {len(results)}")
    return True


def verify_row_counts():
    """Show current row counts for each table (should be 0 if fresh)."""
    
    print("\n" + "=" * 60)
    print("ROW COUNTS (should be 0 for fresh database)")
    print("=" * 60)
    
    tables = [
        "roads", "buildings", "points_of_interest", "ward_boundaries",
        "traffic_readings", "weather_readings", "air_quality_readings", "anomalies",
    ]
    
    with engine.connect() as conn:
        for table in tables:
            try:
                result = conn.execute(text(f"SELECT COUNT(*) FROM {table}")).scalar()
                print(f"  📊  {table}: {result} rows")
            except Exception as e:
                print(f"  ❌  {table}: ERROR — {e}")


def main():
    print("\n🔍 URBAN INTELLIGENCE DASHBOARD — DATABASE VERIFICATION\n")
    
    # Test basic connection first
    try:
        with engine.connect() as conn:
            version = conn.execute(text("SELECT version()")).scalar()
            postgis = conn.execute(text("SELECT PostGIS_Version()")).scalar()
            print(f"PostgreSQL: {version[:50]}...")
            print(f"PostGIS:    {postgis}")
    except Exception as e:
        print(f"❌ Cannot connect to database: {e}")
        print("\nMake sure Docker containers are running:")
        print("  cd docker")
        print("  docker-compose up -d")
        return
    
    # Run all checks
    tables_ok = verify_tables()
    spatial_ok = verify_spatial_columns()
    indexes_ok = verify_indexes()
    verify_row_counts()
    
    # Final summary
    print("\n" + "=" * 60)
    print("FINAL SUMMARY")
    print("=" * 60)
    
    if tables_ok and spatial_ok:
        print("  🎉  DATABASE IS FULLY READY!")
        print("  ✅  All tables exist")
        print("  ✅  Spatial columns registered")
        if indexes_ok:
            print("  ✅  Indexes created")
        else:
            print("  ⚠️  Some indexes may be missing (not critical)")
        print("\n  👉  You're ready for DATA COLLECTION phase!")
    else:
        print("  ⚠️  Some issues found. See details above.")
        if not tables_ok:
            print("\n  To recreate tables, run:")
            print("    python src/database/init_db.py")


if __name__ == "__main__":
    main()