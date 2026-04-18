"""Quick check of anomalies table structure."""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from sqlalchemy import text
from src.database.connection import engine

with engine.connect() as conn:
    result = conn.execute(text("""
        SELECT column_name, data_type, is_nullable
        FROM information_schema.columns
        WHERE table_name = 'anomalies'
        ORDER BY ordinal_position
    """))
    rows = result.fetchall()
    
    if rows:
        print("ANOMALIES TABLE COLUMNS:")
        print(f"  {'Column':<25s} {'Type':<25s} {'Nullable':<10s}")
        print(f"  {'-'*25} {'-'*25} {'-'*10}")
        for row in rows:
            print(f"  {row[0]:<25s} {row[1]:<25s} {row[2]:<10s}")
    else:
        print("⚠️ anomalies table not found!")