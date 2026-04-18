"""
Show actual column names for roads, buildings, and points_of_interest tables.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from sqlalchemy import inspect
from src.database.connection import engine

inspector = inspect(engine)

for table_name in ["roads", "buildings", "points_of_interest"]:
    print(f"\n📋 {table_name}:")
    print("-" * 50)
    columns = inspector.get_columns(table_name)
    for col in columns:
        print(f"  {col['name']:30s}  {str(col['type'])}")