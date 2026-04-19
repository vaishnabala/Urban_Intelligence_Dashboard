"""
Risk scoring and congestion analysis engine.
Calculates congestion index, zone risk scores, and trend analysis.
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from datetime import datetime, timezone, timedelta
from dataclasses import dataclass, field
from typing import Optional

import numpy as np
from sqlalchemy import text
from loguru import logger

from src.database.connection import engine
from src.config.settings import settings


# ================================================================
# DATA CLASSES FOR RESULTS
# ================================================================
@dataclass
class CongestionResult:
    """Result from congestion index calculation."""
    location_name: str
    time_window_hours: int
    readings_count: int
    congestion_score: float          # 0-100
    avg_congestion_ratio: float
    min_congestion_ratio: float
    max_congestion_ratio: float
    avg_speed: float
    avg_free_flow_speed: float
    speed_utilization_pct: float     # current/freeflow * 100
    peak_congestion_hour: Optional[int]    # Hour of day (0-23)
    lowest_congestion_hour: Optional[int]  # Hour of day (0-23)
    trend: str                       # "improving", "worsening", "stable"
    trend_slope: float               # Negative = improving
    congestion_level: str            # "free_flow", "light", "moderate", "heavy", "severe"
    hourly_breakdown: dict = field(default_factory=dict)


@dataclass
class RiskBreakdown:
    """Breakdown of zone risk scoring."""
    traffic_score: float       # 0-100
    air_quality_score: float   # 0-100
    weather_score: float       # 0-100
    overall_score: float       # 0-100 (weighted)
    risk_level: str            # "low", "moderate", "high", "critical"
    details: dict = field(default_factory=dict)


# ================================================================
# CONGESTION LEVEL THRESHOLDS
# ================================================================
def _ratio_to_score(ratio: float) -> float:
    """
    Convert congestion ratio to 0-100 score.
    
    Ratio meaning: free_flow_speed / current_speed
      1.0 = perfect free flow → score 0
      1.3 = light congestion  → score ~25
      1.6 = moderate          → score ~50
      2.0 = heavy             → score ~75
      3.0+ = severe/gridlock  → score 100
    """
    if ratio <= 1.0:
        return 0.0
    elif ratio >= 3.0:
        return 100.0
    else:
        # Linear mapping: ratio 1.0→0, ratio 3.0→100
        score = ((ratio - 1.0) / 2.0) * 100.0
        return round(min(100.0, max(0.0, score)), 1)


def _score_to_level(score: float) -> str:
    """Convert congestion score to human-readable level."""
    if score <= 15:
        return "free_flow"
    elif score <= 35:
        return "light"
    elif score <= 55:
        return "moderate"
    elif score <= 75:
        return "heavy"
    else:
        return "severe"


def _calculate_trend(timestamps: list, ratios: list) -> tuple:
    """
    Calculate trend using simple linear regression on congestion ratios.
    
    Returns:
        (trend_label, slope)
        - Positive slope = worsening (congestion increasing)
        - Negative slope = improving (congestion decreasing)
    """
    if len(timestamps) < 3:
        return "stable", 0.0
    
    # Convert timestamps to numeric (minutes from first reading)
    t0 = timestamps[0]
    x = np.array([(t - t0).total_seconds() / 60.0 for t in timestamps])
    y = np.array(ratios)
    
    # Simple linear regression: y = mx + b
    n = len(x)
    if n < 2 or np.std(x) == 0:
        return "stable", 0.0
    
    slope = (n * np.sum(x * y) - np.sum(x) * np.sum(y)) / \
            (n * np.sum(x ** 2) - np.sum(x) ** 2)
    
    # Slope is change in congestion ratio per minute
    # Convert to per hour for readability
    slope_per_hour = slope * 60.0
    
    # Threshold for "meaningful" change: 0.05 ratio change per hour
    if slope_per_hour > 0.05:
        return "worsening", round(slope_per_hour, 4)
    elif slope_per_hour < -0.05:
        return "improving", round(slope_per_hour, 4)
    else:
        return "stable", round(slope_per_hour, 4)


# ================================================================
# FUNCTION 1: Calculate Congestion Index
# ================================================================
def calculate_congestion_index(
    location_name: str,
    time_window_hours: int = 3
) -> Optional[CongestionResult]:
    """
    Calculate congestion index for a specific monitoring point.
    
    Args:
        location_name: Name of the monitoring point (e.g., "sarjapur_road_junction")
        time_window_hours: How many hours back to analyze (default: 3)
    
    Returns:
        CongestionResult with full analysis, or None if no data
    """
    logger.info(f"Calculating congestion index for: {location_name} "
                f"(last {time_window_hours}h)")
    
    cutoff = datetime.now(timezone.utc) - timedelta(hours=time_window_hours)
    
    with engine.connect() as conn:
        
        # ---- Fetch readings ----
        result = conn.execute(text("""
            SELECT 
                timestamp,
                current_speed,
                free_flow_speed,
                congestion_ratio,
                confidence
            FROM traffic_readings
            WHERE location_name = :loc
              AND timestamp >= :cutoff
            ORDER BY timestamp ASC
        """), {"loc": location_name, "cutoff": cutoff})
        
        rows = result.fetchall()
        
        if not rows:
            logger.warning(f"  No data found for {location_name} in last {time_window_hours}h")
            return None
        
        # ---- Extract arrays ----
        timestamps = [row[0] for row in rows]
        speeds = [row[1] for row in rows]
        free_flows = [row[2] for row in rows]
        ratios = [row[3] for row in rows]
        
        # Make timestamps timezone-aware if needed
        timestamps_aware = []
        for ts in timestamps:
            if ts.tzinfo is None:
                timestamps_aware.append(ts.replace(tzinfo=timezone.utc))
            else:
                timestamps_aware.append(ts)
        
        # ---- Basic stats ----
        avg_ratio = float(np.mean(ratios))
        min_ratio = float(np.min(ratios))
        max_ratio = float(np.max(ratios))
        avg_speed = float(np.mean(speeds))
        avg_ff = float(np.mean(free_flows))
        speed_util = round((avg_speed / avg_ff * 100), 1) if avg_ff > 0 else 0
        
        # ---- Congestion score ----
        congestion_score = _ratio_to_score(avg_ratio)
        congestion_level = _score_to_level(congestion_score)
        
        # ---- Hourly breakdown ----
        hourly = {}
        for ts, ratio in zip(timestamps_aware, ratios):
            hour = ts.hour
            if hour not in hourly:
                hourly[hour] = []
            hourly[hour].append(ratio)
        
        hourly_avg = {h: round(float(np.mean(vals)), 3) for h, vals in hourly.items()}
        
        # Peak and lowest congestion hours
        if hourly_avg:
            peak_hour = max(hourly_avg, key=hourly_avg.get)
            lowest_hour = min(hourly_avg, key=hourly_avg.get)
        else:
            peak_hour = None
            lowest_hour = None
        
        # ---- Trend analysis ----
        trend_label, trend_slope = _calculate_trend(timestamps_aware, ratios)
        
        # ---- Build result ----
        result = CongestionResult(
            location_name=location_name,
            time_window_hours=time_window_hours,
            readings_count=len(rows),
            congestion_score=congestion_score,
            avg_congestion_ratio=round(avg_ratio, 3),
            min_congestion_ratio=round(min_ratio, 3),
            max_congestion_ratio=round(max_ratio, 3),
            avg_speed=round(avg_speed, 1),
            avg_free_flow_speed=round(avg_ff, 1),
            speed_utilization_pct=speed_util,
            peak_congestion_hour=peak_hour,
            lowest_congestion_hour=lowest_hour,
            trend=trend_label,
            trend_slope=trend_slope,
            congestion_level=congestion_level,
            hourly_breakdown=hourly_avg,
        )
        
        logger.info(f"  Score: {congestion_score}/100 ({congestion_level}) | "
                     f"Trend: {trend_label} | Readings: {len(rows)}")
        
        return result


# ================================================================
# FUNCTION 2: Get Zone Risk Score
# ================================================================
def get_zone_risk_score(
    center_lat: float,
    center_lon: float,
    radius_m: float = 2000,
    time_window_hours: int = 3,
    weights: Optional[dict] = None,
) -> RiskBreakdown:
    """
    Calculate overall risk score for a zone around a point.
    
    Combines:
        - Traffic congestion (from nearby monitoring points)
        - Air quality (from latest AQI reading)
        - Weather risk (rain, low visibility, high wind)
    
    Args:
        center_lat: Center latitude of the zone
        center_lon: Center longitude of the zone
        radius_m: Radius in meters to search for data (default: 2km)
        time_window_hours: Hours of data to consider
        weights: Optional dict with keys 'traffic', 'air_quality', 'weather'
                 Values should sum to 1.0. Default: {0.5, 0.3, 0.2}
    
    Returns:
        RiskBreakdown with scores and details
    """
    if weights is None:
        weights = {"traffic": 0.50, "air_quality": 0.30, "weather": 0.20}
    
    logger.info(f"Calculating zone risk: ({center_lat}, {center_lon}), "
                f"radius={radius_m}m, window={time_window_hours}h")
    
    details = {}
    
    # ===========================================================
    # A. TRAFFIC SCORE
    # ===========================================================
    traffic_score = _calculate_traffic_score(
        center_lat, center_lon, radius_m, time_window_hours, details
    )
    
    # ===========================================================
    # B. AIR QUALITY SCORE
    # ===========================================================
    air_quality_score = _calculate_air_quality_score(time_window_hours, details)
    
    # ===========================================================
    # C. WEATHER RISK SCORE
    # ===========================================================
    weather_score = _calculate_weather_score(time_window_hours, details)
    
    # ===========================================================
    # D. COMBINE WEIGHTED SCORES
    # ===========================================================
    overall = (
        traffic_score * weights["traffic"]
        + air_quality_score * weights["air_quality"]
        + weather_score * weights["weather"]
    )
    overall = round(overall, 1)
    
    # Risk level
    if overall <= 25:
        risk_level = "low"
    elif overall <= 50:
        risk_level = "moderate"
    elif overall <= 75:
        risk_level = "high"
    else:
        risk_level = "critical"
    
    details["weights"] = weights
    
    result = RiskBreakdown(
        traffic_score=round(traffic_score, 1),
        air_quality_score=round(air_quality_score, 1),
        weather_score=round(weather_score, 1),
        overall_score=overall,
        risk_level=risk_level,
        details=details,
    )
    
    logger.info(f"  Zone Risk: {overall}/100 ({risk_level}) | "
                f"Traffic={traffic_score:.0f} AQI={air_quality_score:.0f} "
                f"Weather={weather_score:.0f}")
    
    return result


# ================================================================
# HELPER: Traffic Score Component
# ================================================================
def _calculate_traffic_score(
    lat: float, lon: float, radius_m: float,
    hours: int, details: dict
) -> float:
    """Find nearby monitoring points and average their congestion scores."""
    
    cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
    
    with engine.connect() as conn:
        # Find monitoring points within radius
        result = conn.execute(text("""
            SELECT DISTINCT location_name
            FROM traffic_readings
            WHERE ST_DWithin(
                geometry,
                ST_SetSRID(ST_MakePoint(:lon, :lat), 4326)::geography,
                :radius
            )
            AND timestamp >= :cutoff
        """), {"lat": lat, "lon": lon, "radius": radius_m, "cutoff": cutoff})
        
        nearby_locations = [row[0] for row in result.fetchall()]
    
    if not nearby_locations:
        # No traffic data nearby — try all monitoring points as fallback
        logger.debug("  No traffic data within radius, using all monitoring points")
        nearby_locations = list(settings.MONITORING_POINTS.keys())
        details["traffic_note"] = "Used all monitoring points (none within radius)"
    else:
        details["traffic_nearby_points"] = nearby_locations
    
    # Calculate congestion for each nearby point
    scores = []
    point_details = {}
    
    for loc in nearby_locations:
        congestion = calculate_congestion_index(loc, hours)
        if congestion:
            scores.append(congestion.congestion_score)
            point_details[loc] = {
                "score": congestion.congestion_score,
                "level": congestion.congestion_level,
                "avg_ratio": congestion.avg_congestion_ratio,
                "trend": congestion.trend,
                "readings": congestion.readings_count,
            }
    
    details["traffic_points"] = point_details
    
    if scores:
        # Use the 75th percentile (weighted toward worse conditions)
        traffic_score = float(np.percentile(scores, 75))
    else:
        traffic_score = 50.0  # Default medium if no data
        details["traffic_note"] = "No traffic data available, using default"
    
    return traffic_score


# ================================================================
# HELPER: Air Quality Score Component
# ================================================================
def _calculate_air_quality_score(hours: int, details: dict) -> float:
    """
    Convert AQI reading to 0-100 risk score.
    
    OWM AQI: 1=Good, 2=Fair, 3=Moderate, 4=Poor, 5=Very Poor
    Our score: 1→0, 2→25, 3→50, 4→75, 5→100
    """
    cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
    
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT 
                aqi,
                pm25,
                pm10,
                no2,
                o3,
                timestamp
            FROM air_quality_readings
            WHERE timestamp >= :cutoff
            ORDER BY timestamp DESC
            LIMIT 1
        """), {"cutoff": cutoff})
        
        row = result.fetchone()
    
    if not row:
        details["aqi_note"] = "No AQI data in time window, using default"
        return 50.0  # Default medium
    
    aqi = row[0]
    pm25 = row[1]
    
    # Map OWM AQI (1-5) to score (0-100)
    aqi_score_map = {1: 0, 2: 25, 3: 50, 4: 75, 5: 100}
    base_score = aqi_score_map.get(aqi, 50)
    
    # Adjust based on PM2.5 levels (WHO guidelines: <15 μg/m³ is good)
    # Add up to 10 extra points for high PM2.5
    pm25_adjustment = 0
    if pm25 > 35:
        pm25_adjustment = min(10, (pm25 - 35) / 5)
    
    score = min(100.0, base_score + pm25_adjustment)
    
    aqi_labels = {1: "Good", 2: "Fair", 3: "Moderate", 4: "Poor", 5: "Very Poor"}
    
    details["aqi_value"] = aqi
    details["aqi_label"] = aqi_labels.get(aqi, "Unknown")
    details["pm25"] = pm25
    details["pm10"] = row[2]
    details["aqi_timestamp"] = str(row[5])
    
    return round(score, 1)


# ================================================================
# HELPER: Weather Risk Score Component
# ================================================================
def _calculate_weather_score(hours: int, details: dict) -> float:
    """
    Calculate weather-based risk score.
    
    Risk factors:
        - Rain (biggest factor for traffic/flooding risk)
        - Low visibility
        - High wind speed
        - Certain weather descriptions (thunderstorm, heavy rain)
    """
    cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
    
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT 
                temperature,
                humidity,
                wind_speed,
                rain_1h,
                visibility,
                weather_description,
                timestamp
            FROM weather_readings
            WHERE timestamp >= :cutoff
            ORDER BY timestamp DESC
            LIMIT 1
        """), {"cutoff": cutoff})
        
        row = result.fetchone()
    
    if not row:
        details["weather_note"] = "No weather data in time window, using default"
        return 25.0  # Default low-medium
    
    temp = row[0]
    humidity = row[1]
    wind_speed = row[2]
    rain_1h = row[3] if row[3] else 0
    visibility = row[4] if row[4] else 10000
    description = (row[5] or "").lower()
    
    score = 0.0
    risk_factors = []
    
    # ---- Rain risk (0-40 points) ----
    if rain_1h > 0:
        if rain_1h < 2.5:
            rain_score = 15
            risk_factors.append(f"Light rain ({rain_1h}mm/h)")
        elif rain_1h < 7.5:
            rain_score = 30
            risk_factors.append(f"Moderate rain ({rain_1h}mm/h)")
        else:
            rain_score = 40
            risk_factors.append(f"Heavy rain ({rain_1h}mm/h)")
        score += rain_score
    
    # ---- Visibility risk (0-25 points) ----
    if visibility < 1000:
        vis_score = 25
        risk_factors.append(f"Very poor visibility ({visibility}m)")
    elif visibility < 3000:
        vis_score = 15
        risk_factors.append(f"Poor visibility ({visibility}m)")
    elif visibility < 5000:
        vis_score = 5
        risk_factors.append(f"Reduced visibility ({visibility}m)")
    else:
        vis_score = 0
    score += vis_score
    
    # ---- Wind risk (0-15 points) ----
    if wind_speed > 15:
        wind_score = 15
        risk_factors.append(f"Very high wind ({wind_speed}m/s)")
    elif wind_speed > 10:
        wind_score = 10
        risk_factors.append(f"High wind ({wind_speed}m/s)")
    elif wind_speed > 6:
        wind_score = 5
        risk_factors.append(f"Moderate wind ({wind_speed}m/s)")
    else:
        wind_score = 0
    score += wind_score
    
    # ---- Weather description risk (0-20 points) ----
    severe_keywords = ["thunderstorm", "heavy rain", "tornado", "hurricane"]
    moderate_keywords = ["rain", "drizzle", "storm", "squall"]
    mild_keywords = ["mist", "fog", "haze", "smoke"]
    
    desc_score = 0
    if any(kw in description for kw in severe_keywords):
        desc_score = 20
        risk_factors.append(f"Severe weather: {description}")
    elif any(kw in description for kw in moderate_keywords):
        desc_score = 10
        risk_factors.append(f"Adverse weather: {description}")
    elif any(kw in description for kw in mild_keywords):
        desc_score = 5
        risk_factors.append(f"Mild adverse weather: {description}")
    score += desc_score
    
    score = min(100.0, score)
    
    if not risk_factors:
        risk_factors.append("No significant weather risks")
    
    details["weather_description"] = description
    details["temperature"] = temp
    details["humidity"] = humidity
    details["wind_speed"] = wind_speed
    details["rain_1h"] = rain_1h
    details["visibility"] = visibility
    details["weather_risk_factors"] = risk_factors
    details["weather_timestamp"] = str(row[6])
    
    return round(score, 1)


# ================================================================
# CONVENIENCE: Analyze All Monitoring Points
# ================================================================
def analyze_all_locations(time_window_hours: int = 3) -> dict:
    """
    Run congestion analysis for all 8 monitoring points.
    
    Returns:
        dict mapping location_name → CongestionResult
    """
    logger.info(f"\n{'='*55}")
    logger.info(f"ANALYZING ALL {len(settings.MONITORING_POINTS)} MONITORING POINTS")
    logger.info(f"Time window: {time_window_hours} hours")
    logger.info(f"{'='*55}\n")
    
    results = {}
    
    for location_name in settings.MONITORING_POINTS:
        result = calculate_congestion_index(location_name, time_window_hours)
        if result:
            results[location_name] = result
    
    # Print summary table
    if results:
        print(f"\n{'='*80}")
        print(f"{'Location':<30s} {'Score':>6s} {'Level':<12s} {'AvgSpd':>7s} {'Trend':<12s} {'Readings':>8s}")
        print(f"{'-'*30} {'-'*6} {'-'*12} {'-'*7} {'-'*12} {'-'*8}")
        
        for loc, r in sorted(results.items(), key=lambda x: x[1].congestion_score, reverse=True):
            display = loc.replace("_", " ").title()
            if len(display) > 28:
                display = display[:28] + ".."
            
            # Emoji for level
            emojis = {
                "free_flow": "🟢", "light": "🟡", "moderate": "🟠",
                "heavy": "🔴", "severe": "⛔"
            }
            emoji = emojis.get(r.congestion_level, "⚪")
            
            # Trend arrow
            arrows = {"improving": "📉", "worsening": "📈", "stable": "➡️"}
            arrow = arrows.get(r.trend, "➡️")
            
            print(f"{emoji} {display:<28s} {r.congestion_score:>6.1f} {r.congestion_level:<12s} "
                  f"{r.avg_speed:>7.1f} {arrow} {r.trend:<10s} {r.readings_count:>8}")
        
        print(f"{'='*80}")
    
    return results


# ================================================================
# MAIN — Test the analysis
# ================================================================
if __name__ == "__main__":
    
    print("\n" + "🔍 " * 20)
    print("URBAN INTELLIGENCE — RISK & CONGESTION ANALYSIS")
    print("🔍 " * 20 + "\n")
    
    # 1. Analyze all locations
    print("PART 1: Congestion Analysis (All Locations)")
    print("-" * 50)
    congestion_results = analyze_all_locations(time_window_hours=24)
    
    # 2. Zone risk scores for key areas
    print("\n\nPART 2: Zone Risk Scores")
    print("-" * 50)
    
    test_zones = [
        ("Sarjapur Road Junction", 12.9100, 77.6870),
        ("Dommasandra Circle", 12.9180, 77.7520),
        ("Varthur-Gunjur Junction", 12.9370, 77.7440),
        ("Carmelaram Junction", 12.9060, 77.7060),
    ]
    
    for zone_name, lat, lon in test_zones:
        print(f"\n📍 {zone_name} ({lat}, {lon}):")
        risk = get_zone_risk_score(lat, lon, radius_m=2000, time_window_hours=24)
        
        emojis = {"low": "🟢", "moderate": "🟡", "high": "🟠", "critical": "🔴"}
        emoji = emojis.get(risk.risk_level, "⚪")
        
        print(f"   {emoji} Overall Risk: {risk.overall_score}/100 ({risk.risk_level})")
        print(f"   🚗 Traffic:     {risk.traffic_score}/100")
        print(f"   🌬️  Air Quality: {risk.air_quality_score}/100")
        print(f"   🌧️  Weather:     {risk.weather_score}/100")
        
        if risk.details.get("weather_risk_factors"):
            for factor in risk.details["weather_risk_factors"]:
                print(f"      → {factor}")
    
    print("\n\n✅ Analysis complete!")