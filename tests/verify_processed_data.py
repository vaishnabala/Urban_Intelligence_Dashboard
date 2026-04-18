"""
Verify processed static data — summary statistics for all cleaned datasets.
Run from project root: python tests/verify_processed_data.py
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import geopandas as gpd

PROCESSED_DIR = Path("data/processed")

files = {
    "roads": "roads.geojson",
    "buildings": "buildings.geojson",
    "pois": "pois.geojson",
}


def print_summary(name: str, gdf: gpd.GeoDataFrame) -> None:
    """Print detailed summary statistics for a GeoDataFrame."""

    print(f"\n{'=' * 65}")
    print(f"📊  {name.upper()}")
    print(f"{'=' * 65}")

    # --- Feature count ---
    print(f"\n  📏 Feature count:  {len(gdf):,}")

    # --- CRS ---
    print(f"  🌐 CRS:            {gdf.crs}")

    # --- Geometry types ---
    geom_types = gdf.geometry.geom_type.value_counts().to_dict()
    print(f"  📐 Geometry types:")
    for gtype, count in geom_types.items():
        print(f"      {gtype}: {count:,}")

    # --- Bounding box ---
    bounds = gdf.total_bounds  # [minx, miny, maxx, maxy]
    print(f"  📦 Bounding box:")
    print(f"      Longitude: {bounds[0]:.4f} → {bounds[2]:.4f}")
    print(f"      Latitude:  {bounds[1]:.4f} → {bounds[3]:.4f}")

    # --- Columns ---
    print(f"  📋 Columns ({len(gdf.columns)}):")
    for col in gdf.columns:
        if col == "geometry":
            continue
        dtype = gdf[col].dtype
        non_null = gdf[col].notna().sum()
        pct = (non_null / len(gdf)) * 100
        print(f"      {col:25s}  {str(dtype):15s}  ({pct:.0f}% filled)")

    # --- Numeric column stats ---
    numeric_cols = gdf.select_dtypes(include=["float64", "int64", "float32", "int32"]).columns
    if len(numeric_cols) > 0:
        print(f"\n  📈 Numeric column stats:")
        for col in numeric_cols:
            if gdf[col].notna().sum() > 0:
                print(f"      {col}:")
                print(f"         min={gdf[col].min():.1f}  "
                      f"max={gdf[col].max():.1f}  "
                      f"mean={gdf[col].mean():.1f}  "
                      f"median={gdf[col].median():.1f}")

    # --- Categorical column value counts ---
    cat_cols = [c for c in gdf.columns if c not in ["geometry", "osm_id", "name", "geom_wkt"]
                and gdf[c].dtype == "object"]
    if cat_cols:
        print(f"\n  🏷️  Categorical breakdowns:")
        for col in cat_cols:
            counts = gdf[col].value_counts()
            if len(counts) <= 15:
                print(f"      {col}:")
                for val, cnt in counts.items():
                    print(f"         {val}: {cnt:,}")
            else:
                print(f"      {col}: ({len(counts)} unique values, showing top 10)")
                for val, cnt in counts.head(10).items():
                    print(f"         {val}: {cnt:,}")

    # --- Sample rows ---
    print(f"\n  🔍 Sample data (first 5 rows):")
    sample_cols = [c for c in gdf.columns if c != "geometry"]
    # Truncate wide columns for readability
    sample = gdf[sample_cols].head(5).copy()
    for col in sample.columns:
        if sample[col].dtype == "object":
            sample[col] = sample[col].astype(str).str[:40]
    print(sample.to_string(index=False, max_colwidth=40))


def main():
    print("\n🔍 PROCESSED DATA VERIFICATION\n")

    all_good = True

    for name, filename in files.items():
        filepath = PROCESSED_DIR / filename

        if not filepath.exists():
            print(f"\n❌ {filename} NOT FOUND in {PROCESSED_DIR}")
            all_good = False
            continue

        # Check file size
        size_kb = filepath.stat().st_size / 1024
        size_mb = size_kb / 1024

        # Load
        gdf = gpd.read_file(filepath)
        print_summary(name, gdf)

        # Also check parquet exists
        parquet_path = PROCESSED_DIR / f"{name}.parquet"
        if parquet_path.exists():
            pq_size_kb = parquet_path.stat().st_size / 1024
            print(f"\n  💾 File sizes:")
            print(f"      GeoJSON:  {size_mb:.2f} MB ({size_kb:.0f} KB)")
            print(f"      Parquet:  {pq_size_kb:.0f} KB")
            print(f"      Compression ratio: {size_kb / pq_size_kb:.1f}x")
        else:
            print(f"\n  ⚠️  Parquet file not found: {parquet_path.name}")

    # Final summary
    print(f"\n{'=' * 65}")
    print("📊 FINAL SUMMARY")
    print(f"{'=' * 65}")

    total_features = 0
    for name, filename in files.items():
        filepath = PROCESSED_DIR / filename
        if filepath.exists():
            gdf = gpd.read_file(filepath)
            count = len(gdf)
            total_features += count
            print(f"  ✅ {name:20s}  {count:>8,} features")
        else:
            print(f"  ❌ {name:20s}  NOT FOUND")

    print(f"\n  🎯 Total processed features: {total_features:,}")
    print(f"  📁 Location: {PROCESSED_DIR.resolve()}")

    if all_good:
        print("\n  ✅ All processed data verified successfully!")
    else:
        print("\n  ⚠️  Some files missing — run geo_processor.py first")

    print()


if __name__ == "__main__":
    main()