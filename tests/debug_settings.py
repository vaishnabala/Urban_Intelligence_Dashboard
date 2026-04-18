"""
Show all attributes in settings so we use the correct names.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.config.settings import settings

# Print all attributes that are NOT private
for attr in sorted(dir(settings)):
    if not attr.startswith("_"):
        try:
            val = getattr(settings, attr)
            if not callable(val):
                print(f"  {attr:40s}  =  {val}")
        except Exception as e:
            print(f"  {attr:40s}  =  ERROR: {e}")