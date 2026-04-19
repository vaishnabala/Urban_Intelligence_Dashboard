"""
Anomaly detection for traffic and air quality data.
Uses statistical methods (z-score based) to flag unusual readings.
Inserts detected anomalies into the anomalies table.
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from datetime import datetime, timezone, timedelta
from dataclasses import dataclass
from typing import Optional

import numpy as np
import geopandas as gpd
from shapely.geometry import Point
from sqlalchemy import text
from loguru import logger

from src.database.connection import engine
from src.config.settings import settings


# ================================================================
# DATA CLASSES
# ================================================================
@dataclass
class Anomaly:
    """Represents a detected anomaly."""
    timestamp: datetime
    anomaly_type: str          # "traffic_speed", "traffic_congestion", "aqi_spike", "pm25_spike"
    severity: str              # "low", "medium", "high", "critical"
    description: str
    location_name: Optional[str]
    lat: float
    lon: float


# ================================================================
# HELPER: Insert anomaly into database
# ================================================================
def _insert_anomaly(anomaly: Anomaly):
    """Insert a single anomaly into the anomalies table."""
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO anomalies (timestamp, anomaly_type, severity, description, location_name, geometry)
            VALUES (
                :timestamp,
                :anomaly_type,
                :severity,
                :description,
                :location_name,
                ST_SetSRID(ST_MakePoint(:lon, :lat), 4326)
            )
        """), {
            "timestamp": anomaly.timestamp,
            "anomaly_type": anomaly.anomaly_type,
            "severity": anomaly.severity,
            "description": anomaly.description,
            "location_name": anomaly.location_name,
            "lon": anomaly.lon,
            "lat": anomaly.lat,
        })


# ================================================================
# HELPER: Determine severity from z-score
# ================================================================
def _zscore_to_severity(z: float) -> str:
    """Convert absolute z-score to severity level."""
    z = abs(z)
    if z >= 4.0:
        return "critical"
    elif z >= 3.0:
        return "high"
    elif z >= 2.5:
        return "medium"
    else:
        return "low"


# ================================================================
# FUNCTION 1: Detect Traffic Anomalies
# ================================================================
def detect_traffic_anomalies(
    lookback_hours: int = 24,
    z_threshold: float = 2.0,
) -> list[Anomaly]:
    """
    Detect traffic anomalies for all monitoring points.
    
    Method:
        1. For each location, get the last `lookback_hours` of traffic data
        2. Group by hour-of-day to build a baseline (mean & std per hour)
        3. Compare the LATEST reading against its hour-of-day baseline
        4. Flag as anomaly if speed deviates by more than z_threshold * std
        5. Also check for unusual congestion ratios
    
    Args:
        lookback_hours: Hours of historical data to build baseline (default: 24)
        z_threshold: Number of standard deviations for anomaly (default: 2.0)
    
    Returns:
        List of detected Anomaly objects
    """
    logger.info(f"{'='*55}")
    logger.info("DETECTING TRAFFIC ANOMALIES")
    logger.info(f"  Lookback: {lookback_hours}h | Z-threshold: {z_threshold}")
    logger.info(f"{'='*55}")
    
    anomalies = []
    cutoff = datetime.now(timezone.utc) - timedelta(hours=lookback_hours)
    
    for location_name, (lat, lon) in settings.MONITORING_POINTS.items():
        try:
            with engine.connect() as conn:
                
                # ---- Get all readings in the lookback window ----
                result = conn.execute(text("""
                    SELECT 
                        timestamp,
                        current_speed,
                        free_flow_speed,
                        congestion_ratio
                    FROM traffic_readings
                    WHERE location_name = :loc
                      AND timestamp >= :cutoff
                    ORDER BY timestamp ASC
                """), {"loc": location_name, "cutoff": cutoff})
                
                rows = result.fetchall()
            
            if len(rows) < 5:
                logger.debug(f"  {location_name}: Only {len(rows)} readings, skipping (need ≥5)")
                continue
            
            # ---- Extract data ----
            timestamps = [row[0] for row in rows]
            speeds = np.array([row[1] for row in rows])
            free_flows = np.array([row[2] for row in rows])
            ratios = np.array([row[3] for row in rows])
            
            # Make timestamps timezone-aware
            timestamps = [
                ts.replace(tzinfo=timezone.utc) if ts.tzinfo is None else ts
                for ts in timestamps
            ]
            
            # ---- Build hour-of-day baseline ----
            hourly_speeds = {}
            hourly_ratios = {}
            
            for ts, speed, ratio in zip(timestamps, speeds, ratios):
                hour = ts.hour
                if hour not in hourly_speeds:
                    hourly_speeds[hour] = []
                    hourly_ratios[hour] = []
                hourly_speeds[hour].append(speed)
                hourly_ratios[hour].append(ratio)
            
            # ---- Get the latest reading ----
            latest_ts = timestamps[-1]
            latest_speed = speeds[-1]
            latest_ratio = ratios[-1]
            latest_ff = free_flows[-1]
            latest_hour = latest_ts.hour
            
            display_name = location_name.replace("_", " ").title()
            
            # ---- Check speed anomaly ----
            if latest_hour in hourly_speeds and len(hourly_speeds[latest_hour]) >= 3:
                hour_speeds = np.array(hourly_speeds[latest_hour])
                mean_speed = np.mean(hour_speeds)
                std_speed = np.std(hour_speeds)
                
                if std_speed > 0:
                    z_speed = (latest_speed - mean_speed) / std_speed
                    
                    if abs(z_speed) >= z_threshold:
                        severity = _zscore_to_severity(z_speed)
                        
                        if z_speed < 0:
                            # Unusually slow
                            desc = (
                                f"Unusually slow traffic at {display_name}. "
                                f"Current speed: {latest_speed:.1f} km/h, "
                                f"expected for hour {latest_hour:02d}: "
                                f"{mean_speed:.1f} ± {std_speed:.1f} km/h "
                                f"(z-score: {z_speed:.2f})"
                            )
                            anom_type = "traffic_speed_low"
                        else:
                            # Unusually fast (might indicate sensor issue or empty road)
                            desc = (
                                f"Unusually fast traffic at {display_name}. "
                                f"Current speed: {latest_speed:.1f} km/h, "
                                f"expected for hour {latest_hour:02d}: "
                                f"{mean_speed:.1f} ± {std_speed:.1f} km/h "
                                f"(z-score: {z_speed:.2f})"
                            )
                            anom_type = "traffic_speed_high"
                        
                        anomaly = Anomaly(
                            timestamp=latest_ts,
                            anomaly_type=anom_type,
                            severity=severity,
                            description=desc,
                            location_name=location_name,
                            lat=lat,
                            lon=lon,
                        )
                        anomalies.append(anomaly)
                        _insert_anomaly(anomaly)
                        
                        logger.warning(f"  🚨 {display_name}: {anom_type} "
                                       f"(z={z_speed:.2f}, severity={severity})")
            
            # ---- Check congestion ratio anomaly ----
            if latest_hour in hourly_ratios and len(hourly_ratios[latest_hour]) >= 3:
                hour_ratios = np.array(hourly_ratios[latest_hour])
                mean_ratio = np.mean(hour_ratios)
                std_ratio = np.std(hour_ratios)
                
                if std_ratio > 0:
                    z_ratio = (latest_ratio - mean_ratio) / std_ratio
                    
                    # Only flag high congestion (positive z-score)
                    if z_ratio >= z_threshold:
                        severity = _zscore_to_severity(z_ratio)
                        
                        desc = (
                            f"Unusual congestion at {display_name}. "
                            f"Congestion ratio: {latest_ratio:.2f}, "
                            f"expected for hour {latest_hour:02d}: "
                            f"{mean_ratio:.2f} ± {std_ratio:.2f} "
                            f"(z-score: {z_ratio:.2f}). "
                            f"Speed: {latest_speed:.1f}/{latest_ff:.1f} km/h"
                        )
                        
                        anomaly = Anomaly(
                            timestamp=latest_ts,
                            anomaly_type="traffic_congestion",
                            severity=severity,
                            description=desc,
                            location_name=location_name,
                            lat=lat,
                            lon=lon,
                        )
                        anomalies.append(anomaly)
                        _insert_anomaly(anomaly)
                        
                        logger.warning(f"  🚨 {display_name}: traffic_congestion "
                                       f"(z={z_ratio:.2f}, severity={severity})")
            
            # ---- Check for near-zero speed (gridlock) ----
            if latest_speed < 3.0 and latest_ff > 20.0:
                desc = (
                    f"Near-gridlock at {display_name}. "
                    f"Current speed: {latest_speed:.1f} km/h, "
                    f"free flow: {latest_ff:.1f} km/h. "
                    f"Traffic is almost stopped."
                )
                
                anomaly = Anomaly(
                    timestamp=latest_ts,
                    anomaly_type="traffic_gridlock",
                    severity="critical",
                    description=desc,
                    location_name=location_name,
                    lat=lat,
                    lon=lon,
                )
                anomalies.append(anomaly)
                _insert_anomaly(anomaly)
                
                logger.warning(f"  🚨 {display_name}: GRIDLOCK! "
                               f"({latest_speed:.1f} km/h)")
            
            # If no anomaly for this location, log it
            loc_anomalies = [a for a in anomalies if a.location_name == location_name]
            if not loc_anomalies:
                logger.info(f"  ✅ {display_name}: Normal "
                            f"(speed={latest_speed:.1f}, ratio={latest_ratio:.2f})")
                
        except Exception as e:
            logger.error(f"  ❌ {location_name}: Error - {type(e).__name__}: {e}")
    
    logger.info(f"\nTraffic anomaly scan complete: {len(anomalies)} anomalies detected")
    return anomalies


# ================================================================
# FUNCTION 2: Detect AQI Anomalies
# ================================================================
def detect_aqi_anomalies(
    lookback_hours: int = 24,
    pm25_threshold: float = 60.0,
    z_threshold: float = 2.0,
) -> list[Anomaly]:
    """
    Detect air quality anomalies.
    
    Method:
        1. Check if PM2.5 exceeds absolute threshold (60 μg/m³)
        2. Check if AQI suddenly changed (z-score based)
        3. Check if any pollutant spiked vs historical baseline
    
    Args:
        lookback_hours: Hours of historical data for baseline
        pm25_threshold: Absolute PM2.5 threshold in μg/m³ (WHO: 15, India NAAQ: 60)
        z_threshold: Z-score threshold for statistical anomaly
    
    Returns:
        List of detected Anomaly objects
    """
    logger.info(f"\n{'='*55}")
    logger.info("DETECTING AIR QUALITY ANOMALIES")
    logger.info(f"  Lookback: {lookback_hours}h | PM2.5 threshold: {pm25_threshold} μg/m³")
    logger.info(f"{'='*55}")
    
    anomalies = []
    cutoff = datetime.now(timezone.utc) - timedelta(hours=lookback_hours)
    lat, lon = settings.STUDY_AREA_CENTER
    
    try:
        with engine.connect() as conn:
            
            # ---- Get all AQI readings in lookback window ----
            result = conn.execute(text("""
                SELECT 
                    timestamp,
                    aqi,
                    pm25,
                    pm10,
                    no2,
                    o3,
                    co,
                    so2
                FROM air_quality_readings
                WHERE timestamp >= :cutoff
                ORDER BY timestamp ASC
            """), {"cutoff": cutoff})
            
            rows = result.fetchall()
        
        if len(rows) < 2:
            logger.info(f"  Only {len(rows)} AQI readings, need ≥2 for analysis")
            return anomalies
        
        # ---- Extract arrays ----
        timestamps = [row[0] for row in rows]
        aqis = np.array([row[1] for row in rows])
        pm25s = np.array([row[2] for row in rows])
        pm10s = np.array([row[3] for row in rows])
        no2s = np.array([row[4] for row in rows])
        o3s = np.array([row[5] for row in rows])
        cos = np.array([row[6] for row in rows])
        so2s = np.array([row[7] for row in rows])
        
        # Latest reading
        latest_ts = timestamps[-1]
        if latest_ts.tzinfo is None:
            latest_ts = latest_ts.replace(tzinfo=timezone.utc)
        
        latest_aqi = aqis[-1]
        latest_pm25 = pm25s[-1]
        latest_pm10 = pm10s[-1]
        
        aqi_labels = {1: "Good", 2: "Fair", 3: "Moderate", 4: "Poor", 5: "Very Poor"}
        
        # ---- CHECK 1: Absolute PM2.5 threshold ----
        if latest_pm25 >= pm25_threshold:
            if latest_pm25 >= 150:
                severity = "critical"
            elif latest_pm25 >= 100:
                severity = "high"
            elif latest_pm25 >= 75:
                severity = "medium"
            else:
                severity = "low"
            
            desc = (
                f"PM2.5 exceeds threshold: {latest_pm25:.1f} μg/m³ "
                f"(threshold: {pm25_threshold} μg/m³). "
                f"AQI: {latest_aqi} ({aqi_labels.get(latest_aqi, 'Unknown')}). "
                f"PM10: {latest_pm10:.1f} μg/m³."
            )
            
            anomaly = Anomaly(
                timestamp=latest_ts,
                anomaly_type="pm25_spike",
                severity=severity,
                description=desc,
                location_name="study_area_center",
                lat=lat,
                lon=lon,
            )
            anomalies.append(anomaly)
            _insert_anomaly(anomaly)
            
            logger.warning(f"  🚨 PM2.5 spike: {latest_pm25:.1f} μg/m³ ({severity})")
        
        # ---- CHECK 2: AQI level jump ----
        if len(aqis) >= 3:
            # Check if AQI jumped by 2+ levels compared to recent average
            recent_avg_aqi = np.mean(aqis[:-1])
            aqi_jump = latest_aqi - recent_avg_aqi
            
            if aqi_jump >= 2:
                severity = "high" if aqi_jump >= 3 else "medium"
                
                desc = (
                    f"AQI level jumped significantly: "
                    f"current {latest_aqi} ({aqi_labels.get(latest_aqi, '?')}), "
                    f"recent average {recent_avg_aqi:.1f}. "
                    f"Jump of {aqi_jump:.1f} levels."
                )
                
                anomaly = Anomaly(
                    timestamp=latest_ts,
                    anomaly_type="aqi_spike",
                    severity=severity,
                    description=desc,
                    location_name="study_area_center",
                    lat=lat,
                    lon=lon,
                )
                anomalies.append(anomaly)
                _insert_anomaly(anomaly)
                
                logger.warning(f"  🚨 AQI jump: {recent_avg_aqi:.1f} → {latest_aqi} ({severity})")
        
        # ---- CHECK 3: Statistical anomaly for each pollutant ----
        pollutant_checks = [
            ("pm25", pm25s, "PM2.5"),
            ("pm10", pm10s, "PM10"),
            ("no2", no2s, "NO₂"),
            ("o3", o3s, "O₃"),
            ("co", cos, "CO"),
            ("so2", so2s, "SO₂"),
        ]
        
        for col_name, values, display_name in pollutant_checks:
            if len(values) < 5:
                continue
            
            mean_val = np.mean(values[:-1])  # Baseline excludes latest
            std_val = np.std(values[:-1])
            latest_val = values[-1]
            
            if std_val > 0:
                z_score = (latest_val - mean_val) / std_val
                
                if z_score >= z_threshold:
                    severity = _zscore_to_severity(z_score)
                    
                    desc = (
                        f"{display_name} statistical anomaly: "
                        f"current {latest_val:.1f} μg/m³, "
                        f"baseline {mean_val:.1f} ± {std_val:.1f} μg/m³ "
                        f"(z-score: {z_score:.2f})"
                    )
                    
                    anomaly = Anomaly(
                        timestamp=latest_ts,
                        anomaly_type=f"{col_name}_anomaly",
                        severity=severity,
                        description=desc,
                        location_name="study_area_center",
                        lat=lat,
                        lon=lon,
                    )
                    anomalies.append(anomaly)
                    _insert_anomaly(anomaly)
                    
                    logger.warning(f"  🚨 {display_name} anomaly: "
                                   f"{latest_val:.1f} (z={z_score:.2f}, {severity})")
        
        # ---- Log if no anomalies ----
        if not anomalies:
            logger.info(f"  ✅ Air quality normal: AQI={latest_aqi} "
                        f"({aqi_labels.get(latest_aqi, '?')}), "
                        f"PM2.5={latest_pm25:.1f}")
        
    except Exception as e:
        logger.error(f"  ❌ AQI anomaly detection error: {type(e).__name__}: {e}")
    
    logger.info(f"\nAQI anomaly scan complete: {len(anomalies)} anomalies detected")
    return anomalies


# ================================================================
# FUNCTION 3: Get Active Anomalies (for map display)
# ================================================================
def get_active_anomalies(hours: int = 1) -> gpd.GeoDataFrame:
    """
    Query the anomalies table for recent anomalies.
    Returns a GeoDataFrame ready for map display.
    
    Args:
        hours: How far back to look (default: 1 hour)
    
    Returns:
        GeoDataFrame with columns: timestamp, anomaly_type, severity, 
        description, location_name, geometry
    """
    cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
    
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT 
                id,
                timestamp,
                anomaly_type,
                severity,
                description,
                location_name,
                ST_X(geometry) as lon,
                ST_Y(geometry) as lat
            FROM anomalies
            WHERE timestamp >= :cutoff
            ORDER BY timestamp DESC
        """), {"cutoff": cutoff})
        
        rows = result.fetchall()
    
    if not rows:
        logger.info(f"No active anomalies in the last {hours} hour(s)")
        # Return empty GeoDataFrame with correct schema
        return gpd.GeoDataFrame(
            columns=["id", "timestamp", "anomaly_type", "severity", 
                      "description", "location_name", "geometry"],
            geometry="geometry",
            crs="EPSG:4326",
        )
    
    # Build GeoDataFrame
    data = []
    for row in rows:
        data.append({
            "id": row[0],
            "timestamp": row[1],
            "anomaly_type": row[2],
            "severity": row[3],
            "description": row[4],
            "location_name": row[5],
            "geometry": Point(row[6], row[7]),
        })
    
    gdf = gpd.GeoDataFrame(data, geometry="geometry", crs="EPSG:4326")
    
    logger.info(f"Found {len(gdf)} active anomalies in the last {hours} hour(s)")
    return gdf


# ================================================================
# CONVENIENCE: Run Full Anomaly Scan
# ================================================================
def run_full_anomaly_scan(
    lookback_hours: int = 24,
    z_threshold: float = 2.0,
    pm25_threshold: float = 60.0,
) -> dict:
    """
    Run both traffic and AQI anomaly detection.
    
    Returns:
        dict with results summary
    """
    logger.info("\n" + "🔍 " * 20)
    logger.info("FULL ANOMALY SCAN")
    logger.info("🔍 " * 20 + "\n")
    
    # Traffic anomalies
    traffic_anomalies = detect_traffic_anomalies(
        lookback_hours=lookback_hours,
        z_threshold=z_threshold,
    )
    
    # AQI anomalies
    aqi_anomalies = detect_aqi_anomalies(
        lookback_hours=lookback_hours,
        pm25_threshold=pm25_threshold,
        z_threshold=z_threshold,
    )
    
    all_anomalies = traffic_anomalies + aqi_anomalies
    
    # Get active anomalies as GeoDataFrame
    active_gdf = get_active_anomalies(hours=lookback_hours)
    
    # ---- Print summary ----
    print(f"\n{'='*65}")
    print("📋 ANOMALY SCAN SUMMARY")
    print(f"{'='*65}")
    print(f"  Lookback window:    {lookback_hours} hours")
    print(f"  Z-score threshold:  {z_threshold}")
    print(f"  PM2.5 threshold:    {pm25_threshold} μg/m³")
    print("")
    print(f"  Traffic anomalies:  {len(traffic_anomalies)}")
    print(f"  AQI anomalies:      {len(aqi_anomalies)}")
    print(f"  Total detected:     {len(all_anomalies)}")
    print(f"  Active in DB:       {len(active_gdf)}")
    
    # Breakdown by severity
    if all_anomalies:
        print("\n  By Severity:")
        severity_counts = {}
        for a in all_anomalies:
            severity_counts[a.severity] = severity_counts.get(a.severity, 0) + 1
        
        severity_emojis = {"low": "🟡", "medium": "🟠", "high": "🔴", "critical": "⛔"}
        for sev in ["critical", "high", "medium", "low"]:
            if sev in severity_counts:
                print(f"    {severity_emojis.get(sev, '⚪')} {sev:<10s}: {severity_counts[sev]}")
        
        print("\n  By Type:")
        type_counts = {}
        for a in all_anomalies:
            type_counts[a.anomaly_type] = type_counts.get(a.anomaly_type, 0) + 1
        for atype, count in sorted(type_counts.items()):
            print(f"    • {atype:<25s}: {count}")
        
        print("\n  Details:")
        for i, a in enumerate(all_anomalies, 1):
            loc = (a.location_name or "unknown").replace("_", " ").title()
            sev_emoji = severity_emojis.get(a.severity, "⚪")
            print(f"    {i}. {sev_emoji} [{a.severity}] {a.anomaly_type}")
            print(f"       Location: {loc}")
            print(f"       {a.description[:100]}{'...' if len(a.description) > 100 else ''}")
            print()
    else:
        print("\n  ✅ No anomalies detected — all readings within normal range")
    
    print(f"{'='*65}")
    
    return {
        "traffic_anomalies": len(traffic_anomalies),
        "aqi_anomalies": len(aqi_anomalies),
        "total": len(all_anomalies),
        "active_in_db": len(active_gdf),
        "anomalies": all_anomalies,
        "active_geodataframe": active_gdf,
    }


# ================================================================
# MAIN — Test anomaly detection
# ================================================================
if __name__ == "__main__":
    result = run_full_anomaly_scan(
        lookback_hours=24,
        z_threshold=2.0,
        pm25_threshold=60.0,
    )