"""
Test database operations.
Run from project root: python tests/test_operations.py
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.database.operations import (
    insert_traffic_reading,
    insert_weather_reading,
    insert_air_quality_reading,
    get_latest_traffic,
    get_latest_weather,
    get_traffic_history,
    get_table_counts,
    spatial_query_within_radius,
    spatial_query_within_polygon,
)


def test_inserts():
    """Test inserting one record into each real-time table."""

    print("=" * 60)
    print("TEST 1: Insert Traffic Reading")
    print("=" * 60)

    traffic_id = insert_traffic_reading({
        "location_name": "Sarjapur Road Junction",
        "lat": 12.9100,
        "lon": 77.6870,
        "current_speed": 22.5,
        "free_flow_speed": 45.0,
        "confidence": 0.85,
        "congestion_ratio": 0.50,
    })
    print(f"  Traffic reading ID: {traffic_id}")
    assert traffic_id is not None, "Traffic insert failed!"
    print("  ✅ PASSED\n")

    print("=" * 60)
    print("TEST 2: Insert Weather Reading")
    print("=" * 60)

    weather_id = insert_weather_reading({
        "lat": 12.9100,
        "lon": 77.6870,
        "temperature": 28.5,
        "humidity": 72.0,
        "pressure": 1012.0,
        "weather_description": "scattered clouds",
        "wind_speed": 3.5,
        "rain_1h": 0.0,
        "visibility": 8000.0,
    })
    print(f"  Weather reading ID: {weather_id}")
    assert weather_id is not None, "Weather insert failed!"
    print("  ✅ PASSED\n")

    print("=" * 60)
    print("TEST 3: Insert Air Quality Reading")
    print("=" * 60)

    aqi_id = insert_air_quality_reading({
        "lat": 12.9100,
        "lon": 77.6870,
        "aqi": 3,
        "pm25": 35.2,
        "pm10": 58.1,
        "no2": 22.4,
        "o3": 45.0,
        "co": 500.0,
        "so2": 8.5,
    })
    print(f"  AQI reading ID: {aqi_id}")
    assert aqi_id is not None, "AQI insert failed!"
    print("  ✅ PASSED\n")


def test_queries():
    """Test retrieval functions."""

    print("=" * 60)
    print("TEST 4: Get Latest Traffic")
    print("=" * 60)

    latest = get_latest_traffic()
    print(f"  Found {len(latest)} location(s) with traffic data")
    for r in latest:
        print(f"    📍 {r['location_name']}: speed={r['current_speed']} km/h, "
              f"congestion={r['congestion_ratio']}")
    assert len(latest) > 0, "No traffic data found!"
    print("  ✅ PASSED\n")

    print("=" * 60)
    print("TEST 5: Get Latest Weather")
    print("=" * 60)

    weather = get_latest_weather()
    print(f"  Found {len(weather)} location(s) with weather data")
    for r in weather:
        print(f"    🌤️  ({r['lat']}, {r['lon']}): temp={r['temperature']}°C, "
              f"humidity={r['humidity']}%")
    assert len(weather) > 0, "No weather data found!"
    print("  ✅ PASSED\n")

    print("=" * 60)
    print("TEST 6: Get Traffic History")
    print("=" * 60)

    df = get_traffic_history("Sarjapur Road Junction", hours=1)
    print(f"  Found {len(df)} readings for Sarjapur Road Junction (last 1 hour)")
    if not df.empty:
        print(f"  Columns: {list(df.columns)}")
    assert len(df) > 0, "No traffic history found!"
    print("  ✅ PASSED\n")


def test_spatial_queries():
    """Test spatial queries."""

    print("=" * 60)
    print("TEST 7: Spatial Query — Radius Search (traffic_readings)")
    print("=" * 60)

    # Search for traffic readings within 2km of Sarjapur Road Junction
    gdf = spatial_query_within_radius(12.91, 77.687, 2000, "traffic_readings")
    print(f"  Found {len(gdf)} traffic readings within 2km of Sarjapur Road Junction")
    if not gdf.empty:
        print(f"  Columns: {list(gdf.columns)}")
    print("  ✅ PASSED\n")

    print("=" * 60)
    print("TEST 8: Spatial Query — Polygon Search (traffic_readings)")
    print("=" * 60)

    # Search within our study area bounding box
    bbox_wkt = "POLYGON((77.68 12.86, 77.77 12.86, 77.77 12.95, 77.68 12.95, 77.68 12.86))"
    gdf = spatial_query_within_polygon(bbox_wkt, "traffic_readings")
    print(f"  Found {len(gdf)} traffic readings within study area polygon")
    print("  ✅ PASSED\n")


def test_table_counts():
    """Show current row counts."""

    print("=" * 60)
    print("TEST 9: Table Counts")
    print("=" * 60)

    counts = get_table_counts()
    for table, count in counts.items():
        emoji = "📊" if count > 0 else "📭"
        print(f"  {emoji}  {table}: {count} rows")
    print("  ✅ PASSED\n")


def main():
    print("\n🧪 TESTING DATABASE OPERATIONS\n")

    test_inserts()
    test_queries()
    test_spatial_queries()
    test_table_counts()

    print("=" * 60)
    print("🎉 ALL TESTS PASSED!")
    print("=" * 60)
    print("\nDatabase operations layer is working correctly.")
    print("Ready for data collection phase!\n")


if __name__ == "__main__":
    main()