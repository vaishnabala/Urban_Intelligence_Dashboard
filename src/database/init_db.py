import sys
from pathlib import Path

# Add project root to Python path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from sqlalchemy import text
from src.database.connection import engine
from src.database.models import Base


def init_database():
    """Create all tables in the database."""
    print("=" * 50)
    print("  DATABASE TABLE CREATION")
    print("=" * 50)

    # Step 1: Ensure PostGIS extension exists
    print("\n🔌 Checking PostGIS extension...")
    with engine.connect() as conn:
        conn.execute(text("CREATE EXTENSION IF NOT EXISTS postgis;"))
        conn.commit()
    print("✅ PostGIS extension ready!")

    # Step 2: Create all tables
    print("\n🏗️  Creating tables...")
    Base.metadata.create_all(bind=engine)

    # Step 3: List created tables
    with engine.connect() as conn:
        result = conn.execute(text(
            "SELECT tablename FROM pg_tables WHERE schemaname = 'public' ORDER BY tablename;"
        ))
        tables = [row[0] for row in result]

    print(f"\n📋 Tables in database ({len(tables)} total):")
    for table in tables:
        print(f"   ✅ {table}")

    # Step 4: Check spatial indexes
    with engine.connect() as conn:
        result = conn.execute(text(
            "SELECT indexname FROM pg_indexes WHERE indexdef LIKE '%gist%' ORDER BY indexname;"
        ))
        spatial_indexes = [row[0] for row in result]

    print(f"\n📍 Spatial (GIST) indexes ({len(spatial_indexes)} total):")
    for idx in spatial_indexes:
        print(f"   📌 {idx}")

    print("\n🎉 Database setup complete!")


if __name__ == "__main__":
    init_database()