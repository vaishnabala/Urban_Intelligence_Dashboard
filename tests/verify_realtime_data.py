"""
Verify real-time data collection - check database rows and JSON backups.
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from sqlalchemy import text
from src.database.connection import engine
from src.config.settings import settings


def verify_realtime():
    """Check all real-time data in database and on disk."""
    
    with engine.connect() as conn:
        
        # ============================================================
        # 1. TRAFFIC READINGS
        # ============================================================
        print("=" * 65)
        print("1. TRAFFIC READINGS")
        print("=" * 65)
        
        result = conn.execute(text("SELECT count(*) FROM traffic_readings"))
        count = result.scalar()
        print(f"\n  Total rows: {count}")
        
        print(f"\n  {'Location':<30s} {'Speed':>7s} {'FreeFlow':>9s} {'Congestion':>11s} {'Confidence':>11s}")
        print(f"  {'-'*30} {'-'*7} {'-'*9} {'-'*11} {'-'*11}")
        
        result = conn.execute(text("""
            SELECT 
                location_name,
                current_speed,
                free_flow_speed,
                congestion_ratio,
                confidence,
                timestamp
            FROM traffic_readings
            ORDER BY timestamp DESC
            LIMIT 8
        """))
        
        rows = result.fetchall()
        for row in rows:
            loc = row[0].replace("_", " ").title()
            if len(loc) > 28:
                loc = loc[:28] + ".."
            print(f"  {loc:<30s} {row[1]:>7.1f} {row[2]:>9.1f} {row[3]:>11.3f} {row[4]:>11.2f}")
        
        # Congestion analysis
        print(f"\n  📊 Congestion Analysis (latest readings):")
        result = conn.execute(text("""
            SELECT 
                location_name,
                current_speed,
                free_flow_speed,
                congestion_ratio
            FROM traffic_readings
            ORDER BY timestamp DESC
            LIMIT 8
        """))
        rows = result.fetchall()
        
        for row in rows:
            loc = row[0].replace("_", " ").title()
            ratio = row[3]
            if ratio <= 1.1:
                status = "🟢 Free flow"
            elif ratio <= 1.3:
                status = "🟡 Light traffic"
            elif ratio <= 1.6:
                status = "🟠 Moderate congestion"
            elif ratio <= 2.0:
                status = "🔴 Heavy congestion"
            else:
                status = "⛔ Severe congestion"
            
            speed_pct = round((row[1] / row[2] * 100), 1) if row[2] > 0 else 0
            print(f"    {loc:<30s} → {status} ({speed_pct}% of free flow)")
        
        # ============================================================
        # 2. WEATHER READINGS
        # ============================================================
        print(f"\n{'=' * 65}")
        print("2. WEATHER READINGS")
        print("=" * 65)
        
        result = conn.execute(text("SELECT count(*) FROM weather_readings"))
        count = result.scalar()
        print(f"\n  Total rows: {count}")
        
        result = conn.execute(text("""
            SELECT 
                timestamp,
                temperature,
                humidity,
                pressure,
                weather_description,
                wind_speed,
                rain_1h,
                visibility
            FROM weather_readings
            ORDER BY timestamp DESC
            LIMIT 1
        """))
        
        row = result.fetchone()
        if row:
            print(f"\n  Latest reading:")
            print(f"    🕐 Time:        {row[0]}")
            print(f"    🌡️  Temperature: {row[1]}°C")
            print(f"    💧 Humidity:    {row[2]}%")
            print(f"    🔵 Pressure:    {row[3]} hPa")
            print(f"    ☁️  Description: {row[4]}")
            print(f"    💨 Wind Speed:  {row[5]} m/s")
            print(f"    🌧️  Rain (1h):   {row[6]} mm")
            print(f"    👁️  Visibility:  {row[7]} m")
        else:
            print("  ⚠️  No weather data found!")
        
        # ============================================================
        # 3. AIR QUALITY READINGS
        # ============================================================
        print(f"\n{'=' * 65}")
        print("3. AIR QUALITY READINGS")
        print("=" * 65)
        
        result = conn.execute(text("SELECT count(*) FROM air_quality_readings"))
        count = result.scalar()
        print(f"\n  Total rows: {count}")
        
        result = conn.execute(text("""
            SELECT 
                timestamp,
                aqi,
                pm25,
                pm10,
                no2,
                o3,
                co,
                so2
            FROM air_quality_readings
            ORDER BY timestamp DESC
            LIMIT 1
        """))
        
        row = result.fetchone()
        if row:
            aqi_labels = {1: "Good 🟢", 2: "Fair 🟡", 3: "Moderate 🟠", 4: "Poor 🔴", 5: "Very Poor ⛔"}
            aqi_label = aqi_labels.get(row[1], "Unknown")
            
            print(f"\n  Latest reading:")
            print(f"    🕐 Time:    {row[0]}")
            print(f"    📊 AQI:     {row[1]} — {aqi_label}")
            print(f"    PM2.5:     {row[2]} μg/m³")
            print(f"    PM10:      {row[3]} μg/m³")
            print(f"    NO₂:       {row[4]} μg/m³")
            print(f"    O₃:        {row[5]} μg/m³")
            print(f"    CO:        {row[6]} μg/m³")
            print(f"    SO₂:       {row[7]} μg/m³")
        else:
            print("  ⚠️  No air quality data found!")
        
        # ============================================================
        # 4. GEOMETRY CHECK — Ensure all readings have geometry
        # ============================================================
        print(f"\n{'=' * 65}")
        print("4. GEOMETRY VERIFICATION")
        print("=" * 65)
        
        for table in ["traffic_readings", "weather_readings", "air_quality_readings"]:
            result = conn.execute(text(f"""
                SELECT count(*) FROM {table} WHERE geometry IS NOT NULL
            """))
            geo_count = result.scalar()
            
            result = conn.execute(text(f"SELECT count(*) FROM {table}"))
            total = result.scalar()
            
            status = "✅" if geo_count == total and total > 0 else "⚠️"
            print(f"  {status} {table}: {geo_count}/{total} rows have geometry")
    
    # ============================================================
    # 5. JSON BACKUP FILES
    # ============================================================
    print(f"\n{'=' * 65}")
    print("5. JSON BACKUP FILES (data/realtime/)")
    print("=" * 65)
    
    realtime_dir = settings.PROJECT_ROOT / "data" / "realtime"
    
    if realtime_dir.exists():
        json_files = sorted(realtime_dir.glob("*.json"))
        print(f"\n  Found {len(json_files)} backup file(s):")
        for f in json_files:
            size_kb = f.stat().st_size / 1024
            print(f"    📄 {f.name} ({size_kb:.1f} KB)")
    else:
        print("  ⚠️  data/realtime/ directory not found!")
    
    # ============================================================
    # SUMMARY
    # ============================================================
    print(f"\n{'=' * 65}")
    print("📋 VERIFICATION SUMMARY")
    print("=" * 65)
    
    with engine.connect() as conn:
        for table in ["traffic_readings", "weather_readings", "air_quality_readings"]:
            result = conn.execute(text(f"SELECT count(*) FROM {table}"))
            count = result.scalar()
            expected = 8 if table == "traffic_readings" else 1
            status = "✅" if count >= expected else "❌"
            print(f"  {status} {table}: {count} rows (expected ≥ {expected})")
    
    json_count = len(list((settings.PROJECT_ROOT / "data" / "realtime").glob("*.json"))) if (settings.PROJECT_ROOT / "data" / "realtime").exists() else 0
    status = "✅" if json_count >= 3 else "❌"
    print(f"  {status} JSON backups: {json_count} files (expected ≥ 3)")
    
    print(f"\n{'=' * 65}")
    print("VERIFICATION COMPLETE")
    print("=" * 65)


if __name__ == "__main__":
    verify_realtime()