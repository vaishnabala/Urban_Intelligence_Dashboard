"""
Show the actual column names for all models.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from sqlalchemy import inspect
from src.database.connection import engine

inspector = inspect(engine)

tables_to_check = [
    "traffic_readings",
    "weather_readings",
    "air_quality_readings",
]

for table_name in tables_to_check:
    print(f"\n📋 {table_name}:")
    print("-" * 40)
    columns = inspector.get_columns(table_name)
    for col in columns:
        print(f"  {col['name']:25s}  {str(col['type'])}")