import psycopg2
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

def test_database_connection():
    """Test PostgreSQL connection and PostGIS extension"""
    try:
        # Get database URL from .env
        db_url = os.getenv('DATABASE_URL')
        print(f"🔗 Attempting to connect to database...")
        
        # Parse the connection string
        # Format: postgresql://user:password@host:port/database
        conn = psycopg2.connect(db_url)
        cursor = conn.cursor()
        
        # Test basic connection
        cursor.execute("SELECT version();")
        version = cursor.fetchone()[0]
        print(f"✅ PostgreSQL connected successfully!")
        print(f"📊 Version: {version[:50]}...")
        
        # Check if PostGIS extension is installed
        cursor.execute("""
            SELECT EXISTS(
                SELECT 1 FROM pg_extension WHERE extname = 'postgis'
            );
        """)
        postgis_installed = cursor.fetchone()[0]
        
        if postgis_installed:
            print("✅ PostGIS extension is ENABLED!")
            
            # Get PostGIS version
            cursor.execute("SELECT PostGIS_version();")
            postgis_version = cursor.fetchone()[0]
            print(f"📍 PostGIS Version: {postgis_version}")
        else:
            print("❌ PostGIS extension is NOT enabled!")
            print("💡 Installing PostGIS extension now...")
            cursor.execute("CREATE EXTENSION IF NOT EXISTS postgis;")
            conn.commit()
            print("✅ PostGIS extension installed successfully!")
        
        cursor.close()
        conn.close()
        print("\n🎉 Database setup verified successfully!")
        return True
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return False

if __name__ == "__main__":
    test_database_connection()