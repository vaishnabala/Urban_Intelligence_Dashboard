"""
Verify real-time data quality - check for NULLs, ranges, and anomalies.
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from sqlalchemy import text
from src.database.connection import engine


def verify_quality():
    """Run comprehensive data quality checks."""
    
    issues_found = 0
    
    with engine.connect() as conn:
        
        # ============================================================
        # 1. ROW COUNTS
        # ============================================================
        print("╔══════════════════════════════════════════════════════╗")
        print("║          DATA QUALITY VERIFICATION REPORT           ║")
        print("╚══════════════════════════════════════════════════════╝")
        
        print("\n" + "=" * 60)
        print("1. ROW COUNTS")
        print("=" * 60)
        
        tables = ["traffic_readings", "weather_readings", "air_quality_readings"]
        counts = {}
        for table in tables:
            result = conn.execute(text(f"SELECT count(*) FROM {table}"))
            count = result.scalar()
            counts[table] = count
            status = "✅" if count > 0 else "❌"
            print(f"  {status} {table:<30s} {count:>6,} rows")
        
        # ============================================================
        # 2. TIME RANGE COVERED
        # ============================================================
        print(f"\n{'=' * 60}")
        print("2. TIME RANGE COVERED")
        print("=" * 60)
        
        for table in tables:
            if counts[table] == 0:
                print(f"  ⚠️  {table}: No data yet")
                continue
                
            result = conn.execute(text(f"""
                SELECT 
                    MIN(timestamp) as earliest,
                    MAX(timestamp) as latest,
                    MAX(timestamp) - MIN(timestamp) as duration,
                    count(DISTINCT DATE(timestamp)) as unique_days
                FROM {table}
            """))
            row = result.fetchone()
            
            print(f"\n  📅 {table}:")
            print(f"     Earliest:    {row[0]}")
            print(f"     Latest:      {row[1]}")
            print(f"     Duration:    {row[2]}")
            print(f"     Unique days: {row[3]}")
        
        # ============================================================
        # 3. NULL VALUE CHECK
        # ============================================================
        print(f"\n{'=' * 60}")
        print("3. NULL VALUE CHECK")
        print("=" * 60)
        
        # Traffic critical columns
        if counts["traffic_readings"] > 0:
            print(f"\n  🚗 traffic_readings:")
            traffic_cols = [
                "timestamp", "location_name", "lat", "lon",
                "current_speed", "free_flow_speed", "congestion_ratio",
                "confidence", "geometry"
            ]
            for col in traffic_cols:
                result = conn.execute(text(f"""
                    SELECT count(*) FROM traffic_readings WHERE {col} IS NULL
                """))
                null_count = result.scalar()
                status = "✅" if null_count == 0 else "⚠️"
                if null_count > 0:
                    issues_found += 1
                print(f"     {status} {col:<25s} {null_count:>5} NULLs")
        
        # Weather critical columns
        if counts["weather_readings"] > 0:
            print(f"\n  🌤️ weather_readings:")
            weather_cols = [
                "timestamp", "temperature", "humidity", "pressure",
                "weather_description", "wind_speed", "geometry"
            ]
            for col in weather_cols:
                result = conn.execute(text(f"""
                    SELECT count(*) FROM weather_readings WHERE {col} IS NULL
                """))
                null_count = result.scalar()
                status = "✅" if null_count == 0 else "⚠️"
                if null_count > 0:
                    issues_found += 1
                print(f"     {status} {col:<25s} {null_count:>5} NULLs")
        
        # AQI critical columns
        if counts["air_quality_readings"] > 0:
            print(f"\n  🌬️ air_quality_readings:")
            aqi_cols = [
                "timestamp", "aqi", "pm25", "pm10",
                "no2", "o3", "co", "so2", "geometry"
            ]
            for col in aqi_cols:
                result = conn.execute(text(f"""
                    SELECT count(*) FROM air_quality_readings WHERE {col} IS NULL
                """))
                null_count = result.scalar()
                status = "✅" if null_count == 0 else "⚠️"
                if null_count > 0:
                    issues_found += 1
                print(f"     {status} {col:<25s} {null_count:>5} NULLs")
        
        # ============================================================
        # 4. TRAFFIC SPEED RANGES
        # ============================================================
        print(f"\n{'=' * 60}")
        print("4. TRAFFIC SPEED RANGE CHECK (expected: 0-120 km/h)")
        print("=" * 60)
        
        if counts["traffic_readings"] > 0:
            result = conn.execute(text("""
                SELECT 
                    location_name,
                    MIN(current_speed) as min_speed,
                    MAX(current_speed) as max_speed,
                    ROUND(AVG(current_speed)::numeric, 1) as avg_speed,
                    MIN(free_flow_speed) as min_ff,
                    MAX(free_flow_speed) as max_ff,
                    MIN(congestion_ratio) as min_ratio,
                    MAX(congestion_ratio) as max_ratio,
                    ROUND(AVG(congestion_ratio)::numeric, 2) as avg_ratio,
                    count(*) as readings
                FROM traffic_readings
                GROUP BY location_name
                ORDER BY location_name
            """))
            rows = result.fetchall()
            
            print(f"\n  {'Location':<28s} {'Readings':>8s} {'MinSpd':>7s} {'MaxSpd':>7s} {'AvgSpd':>7s} {'AvgRatio':>9s}")
            print(f"  {'-'*28} {'-'*8} {'-'*7} {'-'*7} {'-'*7} {'-'*9}")
            
            for row in rows:
                loc = row[0].replace("_", " ").title()
                if len(loc) > 26:
                    loc = loc[:26] + ".."
                
                # Flag unusual values
                flag = ""
                if row[1] < 0:
                    flag = " ⚠️ NEGATIVE SPEED"
                    issues_found += 1
                elif row[2] > 120:
                    flag = " ⚠️ VERY HIGH SPEED"
                    issues_found += 1
                
                print(f"  {loc:<28s} {row[9]:>8} {row[1]:>7.1f} {row[2]:>7.1f} {row[3]:>7.1f} {row[8]:>9.2f}{flag}")
            
            # Check for zero speeds
            result = conn.execute(text("""
                SELECT count(*) FROM traffic_readings WHERE current_speed = 0
            """))
            zero_speeds = result.scalar()
            if zero_speeds > 0:
                pct = round(zero_speeds / counts["traffic_readings"] * 100, 1)
                print(f"\n  ⚠️  {zero_speeds} readings with speed = 0 ({pct}%)")
                if pct > 20:
                    issues_found += 1
            else:
                print(f"\n  ✅ No zero-speed readings")
            
            # Check congestion ratio distribution
            result = conn.execute(text("""
                SELECT 
                    CASE 
                        WHEN congestion_ratio <= 1.1 THEN '🟢 Free flow (≤1.1)'
                        WHEN congestion_ratio <= 1.3 THEN '🟡 Light (1.1-1.3)'
                        WHEN congestion_ratio <= 1.6 THEN '🟠 Moderate (1.3-1.6)'
                        WHEN congestion_ratio <= 2.0 THEN '🔴 Heavy (1.6-2.0)'
                        ELSE '⛔ Severe (>2.0)'
                    END as congestion_level,
                    count(*) as cnt
                FROM traffic_readings
                GROUP BY 1
                ORDER BY 1
            """))
            rows = result.fetchall()
            
            print(f"\n  Congestion Distribution:")
            for row in rows:
                bar = "█" * max(1, int(row[1] / max(1, counts["traffic_readings"]) * 40))
                print(f"    {row[0]:<30s} {row[1]:>5} {bar}")
        
        # ============================================================
        # 5. TEMPERATURE RANGE CHECK
        # ============================================================
        print(f"\n{'=' * 60}")
        print("5. TEMPERATURE RANGE CHECK (expected: 15-40°C for Bangalore)")
        print("=" * 60)
        
        if counts["weather_readings"] > 0:
            result = conn.execute(text("""
                SELECT 
                    MIN(temperature) as min_temp,
                    MAX(temperature) as max_temp,
                    ROUND(AVG(temperature)::numeric, 1) as avg_temp,
                    MIN(humidity) as min_hum,
                    MAX(humidity) as max_hum,
                    ROUND(AVG(humidity)::numeric, 1) as avg_hum,
                    MIN(wind_speed) as min_wind,
                    MAX(wind_speed) as max_wind,
                    count(DISTINCT weather_description) as unique_descs
                FROM weather_readings
            """))
            row = result.fetchone()
            
            temp_ok = 10 <= row[0] and row[1] <= 45
            hum_ok = 0 <= row[3] and row[4] <= 100
            
            print(f"\n  🌡️  Temperature: {row[0]}°C — {row[1]}°C (avg: {row[2]}°C) {'✅' if temp_ok else '⚠️ OUT OF RANGE'}")
            print(f"  💧 Humidity:    {row[3]}% — {row[4]}% (avg: {row[5]}%) {'✅' if hum_ok else '⚠️ OUT OF RANGE'}")
            print(f"  💨 Wind Speed:  {row[6]} — {row[7]} m/s")
            print(f"  ☁️  Unique weather descriptions: {row[8]}")
            
            if not temp_ok:
                issues_found += 1
            if not hum_ok:
                issues_found += 1
            
            # Show all weather descriptions seen
            result = conn.execute(text("""
                SELECT weather_description, count(*) as cnt
                FROM weather_readings
                GROUP BY weather_description
                ORDER BY cnt DESC
            """))
            rows = result.fetchall()
            print(f"\n  Weather descriptions seen:")
            for row in rows:
                print(f"    • {row[0]:<30s} ({row[1]} times)")
        
        # ============================================================
        # 6. AQI VALUE CHECK
        # ============================================================
        print(f"\n{'=' * 60}")
        print("6. AQI VALUE CHECK (OWM scale: 1-5)")
        print("=" * 60)
        
        if counts["air_quality_readings"] > 0:
            result = conn.execute(text("""
                SELECT 
                    MIN(aqi) as min_aqi,
                    MAX(aqi) as max_aqi,
                    ROUND(AVG(pm25)::numeric, 1) as avg_pm25,
                    MAX(pm25) as max_pm25,
                    ROUND(AVG(pm10)::numeric, 1) as avg_pm10,
                    MAX(pm10) as max_pm10,
                    ROUND(AVG(no2)::numeric, 1) as avg_no2,
                    ROUND(AVG(o3)::numeric, 1) as avg_o3,
                    ROUND(AVG(co)::numeric, 1) as avg_co,
                    ROUND(AVG(so2)::numeric, 1) as avg_so2
                FROM air_quality_readings
            """))
            row = result.fetchone()
            
            aqi_ok = 1 <= row[0] and row[1] <= 5
            
            aqi_labels = {1: "Good", 2: "Fair", 3: "Moderate", 4: "Poor", 5: "Very Poor"}
            
            print(f"\n  📊 AQI Range: {row[0]} — {row[1]} {'✅' if aqi_ok else '⚠️ OUT OF RANGE'}")
            print(f"     Min AQI: {row[0]} ({aqi_labels.get(row[0], '?')})")
            print(f"     Max AQI: {row[1]} ({aqi_labels.get(row[1], '?')})")
            print(f"\n  Pollutant Averages:")
            print(f"     PM2.5: {row[2]} μg/m³ (max: {row[3]})")
            print(f"     PM10:  {row[4]} μg/m³ (max: {row[5]})")
            print(f"     NO₂:   {row[6]} μg/m³")
            print(f"     O₃:    {row[7]} μg/m³")
            print(f"     CO:    {row[8]} μg/m³")
            print(f"     SO₂:   {row[9]} μg/m³")
            
            if not aqi_ok:
                issues_found += 1
            
            # AQI distribution
            result = conn.execute(text("""
                SELECT aqi, count(*) as cnt
                FROM air_quality_readings
                GROUP BY aqi
                ORDER BY aqi
            """))
            rows = result.fetchall()
            print(f"\n  AQI Distribution:")
            for row in rows:
                label = aqi_labels.get(row[0], "Unknown")
                bar = "█" * max(1, int(row[1] / max(1, counts["air_quality_readings"]) * 30))
                print(f"    AQI {row[0]} ({label:<10s}) {row[1]:>5} {bar}")
        
        # ============================================================
        # 7. DUPLICATE CHECK
        # ============================================================
        print(f"\n{'=' * 60}")
        print("7. DUPLICATE CHECK")
        print("=" * 60)
        
        if counts["traffic_readings"] > 0:
            result = conn.execute(text("""
                SELECT count(*) FROM (
                    SELECT timestamp, location_name, count(*) as cnt
                    FROM traffic_readings
                    GROUP BY timestamp, location_name
                    HAVING count(*) > 1
                ) dupes
            """))
            dupes = result.scalar()
            status = "✅" if dupes == 0 else f"⚠️ {dupes} duplicates"
            print(f"  Traffic:     {status}")
            if dupes > 0:
                issues_found += 1
        
        if counts["weather_readings"] > 0:
            result = conn.execute(text("""
                SELECT count(*) FROM (
                    SELECT timestamp, count(*) as cnt
                    FROM weather_readings
                    GROUP BY timestamp
                    HAVING count(*) > 1
                ) dupes
            """))
            dupes = result.scalar()
            status = "✅" if dupes == 0 else f"⚠️ {dupes} duplicates"
            print(f"  Weather:     {status}")
            if dupes > 0:
                issues_found += 1
        
        if counts["air_quality_readings"] > 0:
            result = conn.execute(text("""
                SELECT count(*) FROM (
                    SELECT timestamp, count(*) as cnt
                    FROM air_quality_readings
                    GROUP BY timestamp
                    HAVING count(*) > 1
                ) dupes
            """))
            dupes = result.scalar()
            status = "✅" if dupes == 0 else f"⚠️ {dupes} duplicates"
            print(f"  Air Quality: {status}")
            if dupes > 0:
                issues_found += 1
    
    # ============================================================
    # FINAL VERDICT
    # ============================================================
    print(f"\n{'=' * 60}")
    print("📋 FINAL VERDICT")
    print("=" * 60)
    
    if issues_found == 0:
        print(f"\n  ✅ ALL CHECKS PASSED — Data quality is GOOD!")
    else:
        print(f"\n  ⚠️  {issues_found} issue(s) found — review above for details")
    
    print(f"\n{'=' * 60}")


if __name__ == "__main__":
    verify_quality()