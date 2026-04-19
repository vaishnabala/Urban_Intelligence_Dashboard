"""
Real-time data collector for traffic, weather, and air quality.
Uses TomTom API for traffic, OpenWeatherMap for weather and AQI.
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

import json
import time
from datetime import datetime, timezone

import requests
from loguru import logger

from src.config.settings import settings
from src.database.operations import (
    insert_traffic_reading,
    insert_weather_reading,
    insert_air_quality_reading,
)


# ================================================================
# DIRECTORIES
# ================================================================
REALTIME_DIR = settings.PROJECT_ROOT / "data" / "realtime"
REALTIME_DIR.mkdir(parents=True, exist_ok=True)


# ================================================================
# HELPER: Save JSON snapshot (backup)
# ================================================================
def _save_snapshot(data: dict, prefix: str):
    """Save a JSON snapshot to data/realtime/ for backup."""
    timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = REALTIME_DIR / f"{prefix}_{timestamp_str}.json"
    try:
        with open(filename, "w") as f:
            json.dump(data, f, indent=2, default=str)
        logger.debug(f"Snapshot saved: {filename.name}")
    except Exception as e:
        logger.warning(f"Failed to save snapshot {filename.name}: {e}")


# ================================================================
# FUNCTION 1: Collect Traffic (TomTom API)
# ================================================================
def collect_traffic() -> dict:
    """
    Call TomTom Traffic Flow API for all 8 monitoring points.
    
    Returns:
        dict with 'success_count', 'fail_count', 'errors' list
    """
    logger.info("=" * 50)
    logger.info("COLLECTING TRAFFIC DATA (TomTom)")
    logger.info("=" * 50)
    
    success_count = 0
    fail_count = 0
    errors = []
    all_readings = []
    
    for location_name, (lat, lon) in settings.MONITORING_POINTS.items():
        try:
            # Build TomTom Traffic Flow URL
            url = settings.TOMTOM_TRAFFIC_URL
            params = {
                "key": settings.TOMTOM_API_KEY,
                "point": f"{lat},{lon}",
                "unit": "KMPH",
                "thickness": 1,
            }
            
            logger.info(f"  Querying: {location_name} ({lat}, {lon})")
            
            response = requests.get(
                url,
                params=params,
                timeout=settings.REQUEST_TIMEOUT_SECONDS,
            )
            response.raise_for_status()
            
            data = response.json()
            flow = data.get("flowSegmentData", {})
            
            # Extract fields
            current_speed = flow.get("currentSpeed", 0)
            free_flow_speed = flow.get("freeFlowSpeed", 0)
            confidence = flow.get("confidence", 0)
            
            # Calculate congestion ratio
            # Higher ratio = more congested (free_flow / current)
            # If current_speed is 0, set a high ratio
            if current_speed > 0:
                congestion_ratio = round(current_speed / free_flow_speed, 3)
            else:
                congestion_ratio = 10.0  # Max congestion indicator
            
            # Build reading dict
            reading = {
                "timestamp": datetime.now(timezone.utc),
                "location_name": location_name,
                "lat": lat,
                "lon": lon,
                "current_speed": current_speed,
                "free_flow_speed": free_flow_speed,
                "confidence": confidence,
                "congestion_ratio": congestion_ratio,
            }
            
            # Insert into database
            insert_traffic_reading(reading)
            all_readings.append(reading)
            success_count += 1
            
            speed_pct = round((current_speed / free_flow_speed * 100), 1) if free_flow_speed > 0 else 0
            logger.info(
                f"    ✅ Speed: {current_speed}/{free_flow_speed} km/h "
                f"({speed_pct}%) | Congestion ratio: {congestion_ratio}"
            )
            
            # Small delay to respect rate limits (0.5 seconds between calls)
            time.sleep(0.5)
            
        except requests.exceptions.HTTPError as e:
            fail_count += 1
            error_msg = f"{location_name}: HTTP {e.response.status_code} - {e}"
            errors.append(error_msg)
            logger.error(f"    ❌ {error_msg}")
            time.sleep(0.5)
            
        except requests.exceptions.Timeout:
            fail_count += 1
            error_msg = f"{location_name}: Request timed out"
            errors.append(error_msg)
            logger.error(f"    ❌ {error_msg}")
            time.sleep(0.5)
            
        except requests.exceptions.ConnectionError:
            fail_count += 1
            error_msg = f"{location_name}: Connection failed"
            errors.append(error_msg)
            logger.error(f"    ❌ {error_msg}")
            time.sleep(0.5)
            
        except Exception as e:
            fail_count += 1
            error_msg = f"{location_name}: {type(e).__name__} - {e}"
            errors.append(error_msg)
            logger.error(f"    ❌ {error_msg}")
            time.sleep(0.5)
    
    # Save snapshot backup
    snapshot = {
        "collected_at": datetime.now(timezone.utc).isoformat(),
        "source": "tomtom_traffic_flow",
        "success_count": success_count,
        "fail_count": fail_count,
        "readings": all_readings,
    }
    _save_snapshot(snapshot, "traffic")
    
    logger.info(f"  Traffic collection done: {success_count} success, {fail_count} failed")
    
    return {
        "source": "traffic",
        "success_count": success_count,
        "fail_count": fail_count,
        "errors": errors,
    }


# ================================================================
# FUNCTION 2: Collect Weather (OpenWeatherMap)
# ================================================================
def collect_weather() -> dict:
    """
    Call OpenWeatherMap Current Weather API for study area center.
    
    Returns:
        dict with 'success', 'error' (if any)
    """
    logger.info("=" * 50)
    logger.info("COLLECTING WEATHER DATA (OpenWeatherMap)")
    logger.info("=" * 50)
    
    lat, lon = settings.STUDY_AREA_CENTER
    
    try:
        url = settings.OWM_CURRENT_WEATHER_URL
        params = {
            "lat": lat,
            "lon": lon,
            "appid": settings.OWM_API_KEY,
            "units": "metric",  # Celsius
        }
        
        logger.info(f"  Querying weather for center: ({lat}, {lon})")
        
        response = requests.get(
            url,
            params=params,
            timeout=settings.REQUEST_TIMEOUT_SECONDS,
        )
        response.raise_for_status()
        
        data = response.json()
        
        # Parse fields from response
        main = data.get("main", {})
        weather_list = data.get("weather", [{}])
        weather_desc = weather_list[0].get("description", "unknown") if weather_list else "unknown"
        wind = data.get("wind", {})
        rain = data.get("rain", {})
        
        reading = {
            "timestamp": datetime.now(timezone.utc),
            "lat": lat,
            "lon": lon,
            "temperature": main.get("temp", 0),
            "humidity": main.get("humidity", 0),
            "pressure": main.get("pressure", 0),
            "weather_description": weather_desc,
            "wind_speed": wind.get("speed", 0),
            "rain_1h": rain.get("1h", 0),  # Rain in last 1 hour (mm), 0 if no rain
            "visibility": data.get("visibility", 0),
        }
        
        # Insert into database
        insert_weather_reading(reading)
        
        logger.info(f"    ✅ Temp: {reading['temperature']}°C | "
                     f"Humidity: {reading['humidity']}% | "
                     f"Wind: {reading['wind_speed']} m/s | "
                     f"Desc: {weather_desc}")
        
        # Save snapshot
        snapshot = {
            "collected_at": datetime.now(timezone.utc).isoformat(),
            "source": "openweathermap_current",
            "raw_response": data,
            "parsed_reading": reading,
        }
        _save_snapshot(snapshot, "weather")
        
        return {"source": "weather", "success": True, "error": None}
        
    except requests.exceptions.HTTPError as e:
        error_msg = f"HTTP {e.response.status_code} - {e}"
        logger.error(f"    ❌ Weather failed: {error_msg}")
        return {"source": "weather", "success": False, "error": error_msg}
        
    except requests.exceptions.Timeout:
        error_msg = "Request timed out"
        logger.error(f"    ❌ Weather failed: {error_msg}")
        return {"source": "weather", "success": False, "error": error_msg}
        
    except requests.exceptions.ConnectionError:
        error_msg = "Connection failed"
        logger.error(f"    ❌ Weather failed: {error_msg}")
        return {"source": "weather", "success": False, "error": error_msg}
        
    except Exception as e:
        error_msg = f"{type(e).__name__} - {e}"
        logger.error(f"    ❌ Weather failed: {error_msg}")
        return {"source": "weather", "success": False, "error": error_msg}


# ================================================================
# FUNCTION 3: Collect Air Quality (OpenWeatherMap Air Pollution)
# ================================================================
def collect_air_quality() -> dict:
    """
    Call OpenWeatherMap Air Pollution API for study area center.
    
    API docs: https://openweathermap.org/api/air-pollution
    
    Returns:
        dict with 'success', 'error' (if any)
    """
    logger.info("=" * 50)
    logger.info("COLLECTING AIR QUALITY DATA (OpenWeatherMap)")
    logger.info("=" * 50)
    
    lat, lon = settings.STUDY_AREA_CENTER
    
    try:
        # OWM Air Pollution API endpoint
        url = "http://api.openweathermap.org/data/2.5/air_pollution"
        params = {
            "lat": lat,
            "lon": lon,
            "appid": settings.OWM_API_KEY,
        }
        
        logger.info(f"  Querying air quality for center: ({lat}, {lon})")
        
        response = requests.get(
            url,
            params=params,
            timeout=settings.REQUEST_TIMEOUT_SECONDS,
        )
        response.raise_for_status()
        
        data = response.json()
        
        # Parse the response
        # Structure: {"list": [{"main": {"aqi": 1-5}, "components": {...}}]}
        item_list = data.get("list", [])
        if not item_list:
            raise ValueError("Empty response from Air Pollution API")
        
        item = item_list[0]
        aqi_main = item.get("main", {})
        components = item.get("components", {})
        
        # OWM AQI scale: 1=Good, 2=Fair, 3=Moderate, 4=Poor, 5=Very Poor
        # We store the raw OWM AQI value
        reading = {
            "timestamp": datetime.now(timezone.utc),
            "lat": lat,
            "lon": lon,
            "aqi": aqi_main.get("aqi", 0),
            "pm25": components.get("pm2_5", 0),
            "pm10": components.get("pm10", 0),
            "no2": components.get("no2", 0),
            "o3": components.get("o3", 0),
            "co": components.get("co", 0),
            "so2": components.get("so2", 0),
        }
        
        # Insert into database
        insert_air_quality_reading(reading)
        
        # AQI label
        aqi_labels = {1: "Good", 2: "Fair", 3: "Moderate", 4: "Poor", 5: "Very Poor"}
        aqi_label = aqi_labels.get(reading["aqi"], "Unknown")
        
        logger.info(f"    ✅ AQI: {reading['aqi']} ({aqi_label}) | "
                     f"PM2.5: {reading['pm25']} | PM10: {reading['pm10']} | "
                     f"NO2: {reading['no2']} | O3: {reading['o3']}")
        
        # Save snapshot
        snapshot = {
            "collected_at": datetime.now(timezone.utc).isoformat(),
            "source": "openweathermap_air_pollution",
            "raw_response": data,
            "parsed_reading": reading,
        }
        _save_snapshot(snapshot, "air_quality")
        
        return {"source": "air_quality", "success": True, "error": None}
        
    except requests.exceptions.HTTPError as e:
        error_msg = f"HTTP {e.response.status_code} - {e}"
        logger.error(f"    ❌ Air quality failed: {error_msg}")
        return {"source": "air_quality", "success": False, "error": error_msg}
        
    except requests.exceptions.Timeout:
        error_msg = "Request timed out"
        logger.error(f"    ❌ Air quality failed: {error_msg}")
        return {"source": "air_quality", "success": False, "error": error_msg}
        
    except requests.exceptions.ConnectionError:
        error_msg = "Connection failed"
        logger.error(f"    ❌ Air quality failed: {error_msg}")
        return {"source": "air_quality", "success": False, "error": error_msg}
        
    except Exception as e:
        error_msg = f"{type(e).__name__} - {e}"
        logger.error(f"    ❌ Air quality failed: {error_msg}")
        return {"source": "air_quality", "success": False, "error": error_msg}


# ================================================================
# FUNCTION 4: Collect All
# ================================================================
def collect_all() -> dict:
    """
    Run all three collectors and return a summary.
    
    Returns:
        dict with results from each collector + overall timestamp
    """
    logger.info("🚀 STARTING FULL DATA COLLECTION CYCLE")
    logger.info(f"   Time: {datetime.now(timezone.utc).isoformat()}")
    logger.info("")
    
    start_time = time.time()
    
    # Collect traffic (8 API calls)
    traffic_result = collect_traffic()
    
    # Small pause between different APIs
    time.sleep(1)
    
    # Collect weather (1 API call)
    weather_result = collect_weather()
    
    # Small pause
    time.sleep(1)
    
    # Collect air quality (1 API call)
    aqi_result = collect_air_quality()
    
    elapsed = round(time.time() - start_time, 1)
    
    # Build summary
    summary = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "elapsed_seconds": elapsed,
        "traffic": traffic_result,
        "weather": weather_result,
        "air_quality": aqi_result,
    }
    
    # Log summary
    logger.info("")
    logger.info("=" * 50)
    logger.info("📊 COLLECTION CYCLE SUMMARY")
    logger.info("=" * 50)
    logger.info(f"  Traffic:     {traffic_result['success_count']}/8 points collected")
    logger.info(f"  Weather:     {'✅ Success' if weather_result['success'] else '❌ Failed'}")
    logger.info(f"  Air Quality: {'✅ Success' if aqi_result['success'] else '❌ Failed'}")
    logger.info(f"  Total time:  {elapsed}s")
    logger.info("=" * 50)
    
    return summary


# ================================================================
# MAIN — Run a single collection cycle for testing
# ================================================================
if __name__ == "__main__":
    print("\n🔍 Running a single data collection cycle...\n")
    result = collect_all()
    print("\n📋 Final Summary:")
    print(json.dumps(result, indent=2, default=str))