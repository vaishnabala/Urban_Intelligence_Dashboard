"""
Monitor real-time data collection progress.
Run this in a SEPARATE terminal while the scheduler is running.
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from datetime import datetime, timezone
from sqlalchemy import text
from src.database.connection import engine
from src.config.settings import settings


def monitor():
    """Check current state of real-time data collection."""
    
    print("")
    print("╔══════════════════════════════════════════════════════╗")
    print("║       DATA COLLECTION MONITOR                       ║")
    print(f"║       Checked at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S'):>33s} ║")
    print("╚══════════════════════════════════════════════════════╝")
    
    with engine.connect() as conn:
        
        # ---- Row Counts ----
        print("\n📊 DATABASE ROW COUNTS")
        print("-" * 50)
        
        tables = {
            "traffic_readings": "🚗",
            "weather_readings": "🌤️",
            "air_quality_readings": "🌬️",
        }
        
        for table, icon in tables.items():
            result = conn.execute(text(f"SELECT count(*) FROM {table}"))
            count = result.scalar()
            print(f"  {icon} {table:<28s} {count:>6,} rows")
        
        # ---- Latest Traffic ----
        print(f"\n🚗 LATEST TRAFFIC (most recent per location)")
        print("-" * 50)
        
        result = conn.execute(text("""
            SELECT DISTINCT ON (location_name)
                location_name,
                current_speed,
                free_flow_speed,
                congestion_ratio,
                timestamp
            FROM traffic_readings
            ORDER BY location_name, timestamp DESC
        """))
        rows = result.fetchall()
        
        if rows:
            print(f"  {'Location':<30s} {'Speed':>6s} {'Free':>6s} {'Ratio':>6s} {'When':>12s}")
            print(f"  {'-'*30} {'-'*6} {'-'*6} {'-'*6} {'-'*12}")
            for row in rows:
                loc = row[0].replace("_", " ").title()
                if len(loc) > 28:
                    loc = loc[:28] + ".."
                
                # How long ago
                if row[4]:
                    now_utc = datetime.now(timezone.utc)
                    ts = row[4].replace(tzinfo=timezone.utc) if row[4].tzinfo is None else row[4]
                    ago = now_utc - ts
                    mins_ago = int(ago.total_seconds() / 60)
                    when = f"{mins_ago}m ago"
                else:
                    when = "?"
                
                # Color indicator
                ratio = row[3]
                if ratio <= 1.1:
                    indicator = "🟢"
                elif ratio <= 1.3:
                    indicator = "🟡"
                elif ratio <= 1.6:
                    indicator = "🟠"
                elif ratio <= 2.0:
                    indicator = "🔴"
                else:
                    indicator = "⛔"
                
                print(f"  {indicator} {loc:<28s} {row[1]:>6.1f} {row[2]:>6.1f} {row[3]:>6.2f} {when:>12s}")
        else:
            print("  No traffic data yet.")
        
        # ---- Latest Weather ----
        print(f"\n🌤️ LATEST WEATHER")
        print("-" * 50)
        
        result = conn.execute(text("""
            SELECT 
                temperature, humidity, pressure,
                weather_description, wind_speed, rain_1h,
                timestamp
            FROM weather_readings
            ORDER BY timestamp DESC
            LIMIT 1
        """))
        row = result.fetchone()
        
        if row:
            now_utc = datetime.now(timezone.utc)
            ts = row[6].replace(tzinfo=timezone.utc) if row[6].tzinfo is None else row[6]
            mins_ago = int((now_utc - ts).total_seconds() / 60)
            
            print(f"  🌡️  {row[0]}°C | 💧 {row[1]}% | 💨 {row[4]} m/s | ☁️ {row[3]}")
            print(f"  🌧️  Rain: {row[5]} mm | 🔵 Pressure: {row[2]} hPa")
            print(f"  🕐 Collected {mins_ago}m ago")
        else:
            print("  No weather data yet.")
        
        # ---- Latest AQI ----
        print(f"\n🌬️ LATEST AIR QUALITY")
        print("-" * 50)
        
        result = conn.execute(text("""
            SELECT 
                aqi, pm25, pm10, no2, o3, co, so2,
                timestamp
            FROM air_quality_readings
            ORDER BY timestamp DESC
            LIMIT 1
        """))
        row = result.fetchone()
        
        if row:
            aqi_labels = {1: "Good 🟢", 2: "Fair 🟡", 3: "Moderate 🟠", 4: "Poor 🔴", 5: "Very Poor ⛔"}
            label = aqi_labels.get(row[0], "Unknown")
            
            now_utc = datetime.now(timezone.utc)
            ts = row[7].replace(tzinfo=timezone.utc) if row[7].tzinfo is None else row[7]
            mins_ago = int((now_utc - ts).total_seconds() / 60)
            
            print(f"  AQI: {row[0]} — {label}")
            print(f"  PM2.5: {row[1]} | PM10: {row[2]} | NO₂: {row[3]} | O₃: {row[4]}")
            print(f"  CO: {row[5]} | SO₂: {row[6]}")
            print(f"  🕐 Collected {mins_ago}m ago")
        else:
            print("  No air quality data yet.")
        
        # ---- Data Growth Estimate ----
        print(f"\n📈 EXPECTED DATA GROWTH")
        print("-" * 50)
        
        result = conn.execute(text("SELECT count(*) FROM traffic_readings"))
        traffic_count = result.scalar()
        
        result = conn.execute(text("""
            SELECT MIN(timestamp), MAX(timestamp) FROM traffic_readings
        """))
        row = result.fetchone()
        
        if row[0] and row[1]:
            first_ts = row[0].replace(tzinfo=timezone.utc) if row[0].tzinfo is None else row[0]
            last_ts = row[1].replace(tzinfo=timezone.utc) if row[1].tzinfo is None else row[1]
            duration = last_ts - first_ts
            hours = duration.total_seconds() / 3600
            
            print(f"  Collection running for: {hours:.1f} hours")
            print(f"  Traffic readings:       {traffic_count}")
            
            if hours > 0:
                rate = traffic_count / hours
                print(f"  Rate:                   ~{rate:.0f} traffic readings/hour")
                print(f"  Expected in 3 hours:    ~{int(3 * 8 * 12)} traffic readings")
                print(f"  Expected in 12 hours:   ~{int(12 * 8 * 12)} traffic readings")
        else:
            print("  Not enough data to estimate yet.")
        
        # ---- JSON Backups ----
        print(f"\n📁 JSON BACKUPS")
        print("-" * 50)
        
        realtime_dir = settings.PROJECT_ROOT / "data" / "realtime"
        if realtime_dir.exists():
            traffic_files = list(realtime_dir.glob("traffic_*.json"))
            weather_files = list(realtime_dir.glob("weather_*.json"))
            aqi_files = list(realtime_dir.glob("air_quality_*.json"))
            
            total_size = sum(f.stat().st_size for f in realtime_dir.glob("*.json")) / (1024 * 1024)
            
            print(f"  Traffic snapshots:  {len(traffic_files)}")
            print(f"  Weather snapshots:  {len(weather_files)}")
            print(f"  AQI snapshots:      {len(aqi_files)}")
            print(f"  Total disk usage:   {total_size:.2f} MB")
        else:
            print("  data/realtime/ not found")
    
    # ---- Log Files ----
    print(f"\n📝 LOG FILES")
    print("-" * 50)
    
    log_dir = settings.PROJECT_ROOT / "logs"
    if log_dir.exists():
        log_files = sorted(log_dir.glob("*.log"))
        for f in log_files:
            size_kb = f.stat().st_size / 1024
            print(f"  📄 {f.name} ({size_kb:.1f} KB)")
    else:
        print("  logs/ not found")
    
    print("\n" + "=" * 55)
    print("TIP: Run this script again anytime to check progress!")
    print("=" * 55)
    print("")


if __name__ == "__main__":
    monitor()