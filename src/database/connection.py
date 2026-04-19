import sys
from pathlib import Path

# Add project root to Python path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from src.config.settings import settings


# ──────────────────────────────────────────────
# 1. Create Engine (the main connection to database)
# ──────────────────────────────────────────────
engine = create_engine(
    settings.DATABASE_URL,
    echo=False,              # Set True to see SQL queries in terminal (for debugging)
    pool_size=5,             # Max 5 connections at a time
    max_overflow=10,         # Allow 10 extra connections if needed
    pool_pre_ping=True,      # Check if connection is alive before using it
)

# ──────────────────────────────────────────────
# 2. Create Session Factory
#    A session = one conversation with the database
#    This factory creates new sessions when needed
# ──────────────────────────────────────────────
SessionLocal = sessionmaker(
    bind=engine,
    autocommit=False,        # We control when to save (commit)
    autoflush=False,         # We control when to send data to DB
)


# ──────────────────────────────────────────────
# 3. get_db() — Dependency Injection
#    Used by FastAPI and other parts of the app
#    Opens a session, gives it to you, then closes it
# ──────────────────────────────────────────────
def get_db():
    """
    Creates a database session.
    Automatically closes when done.

    Usage:
        db = next(get_db())
        # do stuff with db
        db.close()

    Or in FastAPI:
        @app.get("/example")
        def example(db: Session = Depends(get_db)):
            ...
    """
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


# ──────────────────────────────────────────────
# 4. Test Connection
# ──────────────────────────────────────────────
def test_connection():
    """Test database connection and PostGIS support."""
    print("=" * 50)
    print("  DATABASE CONNECTION TEST")
    print("=" * 50)

    try:
        # Test basic connection
        with engine.connect() as conn:
            result = conn.execute(text("SELECT version();"))
            version = result.fetchone()[0]
            print("\n✅ PostgreSQL connected!")
            print(f"📊 Version: {version[:60]}...")

            # Check PostGIS
            result = conn.execute(text(
                "SELECT EXISTS(SELECT 1 FROM pg_extension WHERE extname = 'postgis');"
            ))
            postgis_exists = result.fetchone()[0]

            if postgis_exists:
                result = conn.execute(text("SELECT PostGIS_version();"))
                postgis_version = result.fetchone()[0]
                print("✅ PostGIS enabled!")
                print(f"📍 PostGIS Version: {postgis_version}")
            else:
                print("⚠️  PostGIS not found. Installing...")
                conn.execute(text("CREATE EXTENSION IF NOT EXISTS postgis;"))
                conn.commit()
                print("✅ PostGIS installed!")

        # Test session factory
        db = SessionLocal()
        db.execute(text("SELECT 1"))
        db.close()
        print("✅ Session factory working!")

        # Test get_db()
        db = next(get_db())
        db.execute(text("SELECT 1"))
        db.close()
        print("✅ get_db() working!")

        print("\n🎉 All database connections verified!")
        return True

    except Exception as e:
        print(f"\n❌ Connection failed: {e}")
        return False


if __name__ == "__main__":
    test_connection()