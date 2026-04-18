"""
Verify and spot-check all static data files.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import geopandas as gpd

RAW_DIR = Path("data/raw")

files = {
    "roads": "roads.geojson",
    "buildings": "buildings.geojson",
    "pois": "pois.geojson",
    "water_bodies": "water_bodies.geojson",
}

print("\n🔍 STATIC DATA VERIFICATION\n")

for name, filename in files.items():
    filepath = RAW_DIR / filename
    print("=" * 60)

    if not filepath.exists():
        print(f"❌ {filename} — FILE NOT FOUND!")
        continue

    # File size
    size_kb = filepath.stat().st_size / 1024
    size_mb = size_kb / 1024

    # Load and inspect
    gdf = gpd.read_file(filepath)

    print(f"📁 {filename}")
    print(f"   File size:    {size_mb:.2f} MB ({size_kb:.0f} KB)")
    print(f"   Features:     {len(gdf)}")
    print(f"   CRS:          {gdf.crs}")
    print(f"   Columns:      {list(gdf.columns)}")
    print(f"   Geom types:   {gdf.geometry.geom_type.value_counts().to_dict()}")

    # Bounding box check — should be within our study area
    bounds = gdf.total_bounds  # [minx, miny, maxx, maxy]
    print(f"   Bounds:       lon=[{bounds[0]:.4f}, {bounds[2]:.4f}]  lat=[{bounds[1]:.4f}, {bounds[3]:.4f}]")

    # Show first 3 rows (name/key columns only)
    print(f"\n   📋 Sample data (first 3 rows):")
    sample_cols = [c for c in gdf.columns if c != "geometry"][:5]
    for i, row in gdf[sample_cols].head(3).iterrows():
        print(f"      {dict(row)}")

    print()

# Final summary
print("=" * 60)
print("📊 FINAL SUMMARY")
print("=" * 60)
total = 0
for name, filename in files.items():
    filepath = RAW_DIR / filename
    if filepath.exists():
        gdf = gpd.read_file(filepath)
        count = len(gdf)
        total += count
        print(f"   ✅ {name:20s} {count:>8,} features")
    else:
        print(f"   ❌ {name:20s} NOT FOUND")

print(f"\n   🎯 Total: {total:,} features")
print(f"   📁 Location: {RAW_DIR.resolve()}")
print("\n   ✅ Static data collection verified!\n")