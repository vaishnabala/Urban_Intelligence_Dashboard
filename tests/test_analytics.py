"""
Comprehensive test of all analytics modules.
Tests congestion analysis, anomaly detection, and traffic prediction.
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from datetime import datetime, timezone

from src.config.settings import settings


def test_congestion_analysis():
    """Test 1: Congestion analysis for all locations."""
    
    print("\n" + "=" * 70)
    print("TEST 1: CONGESTION ANALYSIS")
    print("=" * 70)
    
    from src.analytics.risk_scorer import (
        calculate_congestion_index,
        get_zone_risk_score,
        analyze_all_locations,
    )
    
    # 1A: Analyze all locations
    print("\n--- 1A: All Locations (24h window) ---")
    results = analyze_all_locations(time_window_hours=24)
    
    if not results:
        print("  ⚠️  No congestion data available yet")
        return False
    
    # 1B: Detailed look at one location
    print("\n--- 1B: Detailed Analysis — Sarjapur Road Junction ---")
    detail = calculate_congestion_index("sarjapur_road_junction", time_window_hours=24)
    
    if detail:
        print(f"  Location:         {detail.location_name}")
        print(f"  Readings:         {detail.readings_count}")
        print(f"  Score:            {detail.congestion_score}/100")
        print(f"  Level:            {detail.congestion_level}")
        print(f"  Avg Speed:        {detail.avg_speed} km/h")
        print(f"  Avg Free Flow:    {detail.avg_free_flow_speed} km/h")
        print(f"  Speed Util:       {detail.speed_utilization_pct}%")
        print(f"  Avg Ratio:        {detail.avg_congestion_ratio}")
        print(f"  Min/Max Ratio:    {detail.min_congestion_ratio} / {detail.max_congestion_ratio}")
        print(f"  Peak Hour:        {detail.peak_congestion_hour}")
        print(f"  Lowest Hour:      {detail.lowest_congestion_hour}")
        print(f"  Trend:            {detail.trend} (slope: {detail.trend_slope})")
        print(f"  Hourly Breakdown: {detail.hourly_breakdown}")
    
    # 1C: Zone risk scores
    print("\n--- 1C: Zone Risk Scores ---")
    test_zones = [
        ("Sarjapur Road Junction", 12.9100, 77.6870),
        ("Dommasandra Circle", 12.9180, 77.7520),
        ("Varthur-Gunjur Junction", 12.9370, 77.7440),
        ("Carmelaram Junction", 12.9060, 77.7060),
    ]
    
    level_emojis = {"low": "🟢", "moderate": "🟡", "high": "🟠", "critical": "🔴"}
    
    print(f"\n  {'Zone':<30s} {'Overall':>8s} {'Traffic':>8s} {'AQI':>8s} {'Weather':>8s} {'Level':<10s}")
    print(f"  {'-'*30} {'-'*8} {'-'*8} {'-'*8} {'-'*8} {'-'*10}")
    
    for zone_name, lat, lon in test_zones:
        risk = get_zone_risk_score(lat, lon, radius_m=3000, time_window_hours=24)
        emoji = level_emojis.get(risk.risk_level, "⚪")
        print(f"  {emoji} {zone_name:<28s} {risk.overall_score:>8.1f} "
              f"{risk.traffic_score:>8.1f} {risk.air_quality_score:>8.1f} "
              f"{risk.weather_score:>8.1f} {risk.risk_level:<10s}")
    
    print("\n  ✅ Congestion analysis: PASSED")
    return True


def test_anomaly_detection():
    """Test 2: Anomaly detection."""
    
    print("\n" + "=" * 70)
    print("TEST 2: ANOMALY DETECTION")
    print("=" * 70)
    
    from src.analytics.anomaly_detection import (
        detect_traffic_anomalies,
        detect_aqi_anomalies,
        get_active_anomalies,
        run_full_anomaly_scan,
    )
    
    # 2A: Traffic anomalies
    print("\n--- 2A: Traffic Anomalies ---")
    traffic_anomalies = detect_traffic_anomalies(lookback_hours=24, z_threshold=2.0)
    print(f"\n  Found {len(traffic_anomalies)} traffic anomalies")
    
    if traffic_anomalies:
        for a in traffic_anomalies[:5]:  # Show max 5
            loc = (a.location_name or "?").replace("_", " ").title()
            print(f"    [{a.severity}] {a.anomaly_type} @ {loc}")
            print(f"      {a.description[:80]}...")
    
    # 2B: AQI anomalies
    print("\n--- 2B: AQI Anomalies ---")
    aqi_anomalies = detect_aqi_anomalies(lookback_hours=24, pm25_threshold=60.0)
    print(f"\n  Found {len(aqi_anomalies)} AQI anomalies")
    
    if aqi_anomalies:
        for a in aqi_anomalies[:5]:
            print(f"    [{a.severity}] {a.anomaly_type}")
            print(f"      {a.description[:80]}...")
    
    # 2C: Active anomalies as GeoDataFrame
    print("\n--- 2C: Active Anomalies (GeoDataFrame) ---")
    active_gdf = get_active_anomalies(hours=24)
    print(f"  GeoDataFrame shape: {active_gdf.shape}")
    print(f"  CRS: {active_gdf.crs}")
    
    if len(active_gdf) > 0:
        print(f"  Columns: {list(active_gdf.columns)}")
        print(f"\n  Types found:")
        type_counts = active_gdf["anomaly_type"].value_counts()
        for atype, count in type_counts.items():
            print(f"    • {atype}: {count}")
        
        print(f"\n  Severities:")
        sev_counts = active_gdf["severity"].value_counts()
        severity_emojis = {"low": "🟡", "medium": "🟠", "high": "🔴", "critical": "⛔"}
        for sev, count in sev_counts.items():
            emoji = severity_emojis.get(sev, "⚪")
            print(f"    {emoji} {sev}: {count}")
    else:
        print("  ✅ No anomalies (all readings within normal range)")
    
    print("\n  ✅ Anomaly detection: PASSED")
    return True


def test_traffic_prediction():
    """Test 3: Traffic prediction."""
    
    print("\n" + "=" * 70)
    print("TEST 3: TRAFFIC PREDICTION")
    print("=" * 70)
    
    from src.analytics.traffic_predictor import (
        predict_traffic,
        predict_all_locations,
    )
    
    # 3A: Single location prediction
    print("\n--- 3A: Single Location — Sarjapur Road Junction (+30min) ---")
    pred = predict_traffic("sarjapur_road_junction", future_minutes=30)
    
    if pred is None:
        print("  ⚠️  Model not trained yet. Run: python scripts/train_model.py")
        return False
    
    print(f"  Location:          {pred.location_name}")
    print(f"  Coordinates:       ({pred.lat}, {pred.lon})")
    print(f"  Prediction Time:   {pred.prediction_time}")
    print(f"  Predicted Speed:   {pred.predicted_speed} km/h")
    print(f"  Free Flow Speed:   {pred.free_flow_speed} km/h")
    print(f"  Congestion Ratio:  {pred.predicted_congestion_ratio}")
    print(f"  Congestion Level:  {pred.congestion_level}")
    print(f"  Confidence:        {pred.confidence_note}")
    
    # Sanity checks
    assert pred.predicted_speed >= 0, "Speed can't be negative!"
    assert pred.predicted_speed <= 120, "Speed seems too high!"
    assert pred.free_flow_speed > 0, "Free flow speed must be positive!"
    assert pred.predicted_congestion_ratio > 0, "Ratio must be positive!"
    print("  ✅ Sanity checks passed")
    
    # 3B: All locations at different horizons
    print("\n--- 3B: All Locations at Multiple Horizons ---")
    
    level_emojis = {
        "free_flow": "🟢", "light": "🟡", "moderate": "🟠",
        "heavy": "🔴", "severe": "⛔", "unknown": "⚪"
    }
    
    for minutes in [15, 30, 60]:
        print(f"\n  ⏱️  +{minutes} minutes:")
        predictions = predict_all_locations(future_minutes=minutes)
        
        if not predictions:
            print("    ⚠️  No predictions returned")
            continue
        
        print(f"    {'Location':<30s} {'Speed':>7s} {'FF':>7s} {'Ratio':>6s} {'Level':<10s}")
        print(f"    {'-'*30} {'-'*7} {'-'*7} {'-'*6} {'-'*10}")
        
        for p in sorted(predictions, key=lambda x: x.predicted_congestion_ratio, reverse=True):
            display = p.location_name.replace("_", " ").title()
            if len(display) > 28:
                display = display[:28] + ".."
            emoji = level_emojis.get(p.congestion_level, "⚪")
            print(f"    {emoji} {display:<28s} {p.predicted_speed:>7.1f} "
                  f"{p.free_flow_speed:>7.1f} {p.predicted_congestion_ratio:>6.2f} "
                  f"{p.congestion_level}")
    
    print("\n  ✅ Traffic prediction: PASSED")
    return True


def test_database_counts():
    """Test 4: Verify database has data for analytics."""
    
    print("\n" + "=" * 70)
    print("TEST 4: DATABASE DATA CHECK")
    print("=" * 70)
    
    from sqlalchemy import text
    from src.database.connection import engine
    
    with engine.connect() as conn:
        tables = {
            "traffic_readings": "🚗",
            "weather_readings": "🌤️",
            "air_quality_readings": "🌬️",
            "anomalies": "🚨",
            "roads": "🛣️",
            "buildings": "🏠",
            "points_of_interest": "📍",
        }
        
        print(f"\n  {'Table':<28s} {'Rows':>8s}")
        print(f"  {'-'*28} {'-'*8}")
        
        all_ok = True
        for table, emoji in tables.items():
            result = conn.execute(text(f"SELECT count(*) FROM {table}"))
            count = result.scalar()
            status = "✅" if count > 0 else "⚠️"
            print(f"  {status} {emoji} {table:<24s} {count:>8,}")
            
            if table in ["traffic_readings", "weather_readings"] and count == 0:
                all_ok = False
        
        # Time ranges
        print(f"\n  Time Ranges:")
        for table in ["traffic_readings", "weather_readings", "air_quality_readings"]:
            result = conn.execute(text(f"""
                SELECT MIN(timestamp), MAX(timestamp),
                       MAX(timestamp) - MIN(timestamp) as duration
                FROM {table}
            """))
            row = result.fetchone()
            if row[0]:
                print(f"    {table}: {row[2]} (from {row[0]} to {row[1]})")
            else:
                print(f"    {table}: No data")
    
    print(f"\n  {'✅' if all_ok else '⚠️'} Database check: {'PASSED' if all_ok else 'NEEDS MORE DATA'}")
    return all_ok


def test_model_files():
    """Test 5: Verify model files exist."""
    
    print("\n" + "=" * 70)
    print("TEST 5: MODEL FILES CHECK")
    print("=" * 70)
    
    models_dir = settings.PROJECT_ROOT / "models"
    
    required_files = [
        "traffic_speed_model.joblib",
        "location_encoder.joblib",
        "model_config.joblib",
    ]
    
    all_ok = True
    for filename in required_files:
        filepath = models_dir / filename
        if filepath.exists():
            size_kb = filepath.stat().st_size / 1024
            print(f"  ✅ {filename} ({size_kb:.1f} KB)")
        else:
            print(f"  ❌ {filename} — NOT FOUND")
            all_ok = False
    
    if all_ok:
        import joblib
        config = joblib.load(models_dir / "model_config.joblib")
        print(f"\n  Model Info:")
        print(f"    Model:      {config['model_name']}")
        print(f"    Train size: {config['train_size']:,}")
        print(f"    Test size:  {config['test_size']:,}")
        print(f"    Test MAE:   {config['test_mae']:.2f} km/h")
        print(f"    Test R²:    {config['test_r2']:.4f}")
        print(f"    Features:   {len(config['feature_cols'])}")
        print(f"    Locations:  {config['location_classes']}")
        if 'trained_at' in config:
            print(f"    Trained at: {config['trained_at']}")
    
    print(f"\n  {'✅' if all_ok else '❌'} Model files: {'PASSED' if all_ok else 'FAILED'}")
    return all_ok


# ================================================================
# MAIN
# ================================================================
if __name__ == "__main__":
    
    print("\n" + "🧪 " * 25)
    print("PHASE 4 — COMPREHENSIVE ANALYTICS TEST")
    print("🧪 " * 25)
    print(f"\nTimestamp: {datetime.now(timezone.utc).isoformat()}")
    print(f"Study Area: {settings.STUDY_AREA_NAME}")
    
    results = {}
    
    # Run all tests
    results["database"] = test_database_counts()
    results["model_files"] = test_model_files()
    results["congestion"] = test_congestion_analysis()
    results["anomalies"] = test_anomaly_detection()
    results["prediction"] = test_traffic_prediction()
    
    # Final summary
    print("\n" + "=" * 70)
    print("📋 PHASE 4 — FINAL RESULTS")
    print("=" * 70)
    
    all_passed = True
    for test_name, passed in results.items():
        status = "✅ PASSED" if passed else "⚠️ NEEDS ATTENTION"
        print(f"  {status}  {test_name}")
        if not passed:
            all_passed = False
    
    print(f"\n{'='*70}")
    if all_passed:
        print("🎉 ALL TESTS PASSED — Phase 4 (Analytics & ML) is COMPLETE!")
    else:
        print("⚠️  Some tests need attention — check details above")
    print(f"{'='*70}\n")