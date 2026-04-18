"""
Debug script to see the actual error when inserting a traffic reading.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from datetime import datetime, timezone
from shapely.geometry import Point
from geoalchemy2.shape import from_shape

from src.database.connection import SessionLocal
from src.database.models import TrafficReading

session = SessionLocal()

try:
    reading = TrafficReading(
        location_name="Sarjapur Road Junction",
        latitude=12.9100,
        longitude=77.6870,
        geometry=from_shape(Point(77.6870, 12.9100), srid=4326),
        current_speed=22.5,
        free_flow_speed=45.0,
        congestion_level=0.50,
        travel_time=120.0,
        road_closure=False,
        timestamp=datetime.now(timezone.utc),
    )

    session.add(reading)
    session.commit()
    session.refresh(reading)

    print(f"✅ SUCCESS! Inserted row ID: {reading.id}")

except Exception as e:
    session.rollback()
    print(f"❌ ERROR: {type(e).__name__}")
    print(f"   {e}")

finally:
    session.close()