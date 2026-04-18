"""
Scheduler for continuous real-time data collection.
Uses APScheduler to run traffic, weather, and AQI collection on intervals.
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

import signal
import time
from datetime import datetime, timezone

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.events import EVENT_JOB_ERROR, EVENT_JOB_EXECUTED
from loguru import logger

from src.config.settings import settings
from src.data_collection.realtime_collector import (
    collect_traffic,
    collect_weather,
    collect_air_quality,
)


# ================================================================
# LOGGING SETUP
# ================================================================
LOG_DIR = settings.PROJECT_ROOT / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

# Remove default loguru handler (console) and re-add with format
logger.remove()

# Console logging — clean and readable
logger.add(
    sys.stdout,
    format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level:<8}</level> | <cyan>{message}</cyan>",
    level="INFO",
)

# File logging — detailed, rotates daily, keeps 7 days
logger.add(
    LOG_DIR / "scheduler_{time:YYYY-MM-DD}.log",
    format="{time:YYYY-MM-DD HH:mm:ss.SSS} | {level:<8} | {module}:{function}:{line} | {message}",
    level="DEBUG",
    rotation="00:00",      # New file at midnight
    retention="7 days",    # Keep 7 days of logs
    compression="zip",     # Compress old logs
)

# Error-only log file
logger.add(
    LOG_DIR / "errors_{time:YYYY-MM-DD}.log",
    format="{time:YYYY-MM-DD HH:mm:ss.SSS} | {level:<8} | {module}:{function}:{line} | {message}",
    level="ERROR",
    rotation="00:00",
    retention="14 days",
    compression="zip",
)


# ================================================================
# COLLECTION CYCLE COUNTER
# ================================================================
cycle_stats = {
    "traffic_runs": 0,
    "traffic_success": 0,
    "traffic_fail": 0,
    "weather_runs": 0,
    "weather_success": 0,
    "weather_fail": 0,
    "aqi_runs": 0,
    "aqi_success": 0,
    "aqi_fail": 0,
    "started_at": None,
}


# ================================================================
# JOB WRAPPERS (each handles its own errors)
# ================================================================
def job_collect_traffic():
    """Scheduled job: collect traffic data for all 8 monitoring points."""
    cycle_stats["traffic_runs"] += 1
    run_num = cycle_stats["traffic_runs"]
    
    try:
        logger.info(f"🚗 Traffic collection #{run_num} starting...")
        result = collect_traffic()
        
        cycle_stats["traffic_success"] += result["success_count"]
        cycle_stats["traffic_fail"] += result["fail_count"]
        
        logger.info(
            f"🚗 Traffic collection #{run_num} complete: "
            f"{result['success_count']}/8 success"
        )
        
        if result["errors"]:
            for err in result["errors"]:
                logger.warning(f"   Traffic error: {err}")
                
    except Exception as e:
        cycle_stats["traffic_fail"] += 1
        logger.error(f"🚗 Traffic collection #{run_num} CRASHED: {type(e).__name__} - {e}")


def job_collect_weather():
    """Scheduled job: collect weather data."""
    cycle_stats["weather_runs"] += 1
    run_num = cycle_stats["weather_runs"]
    
    try:
        logger.info(f"🌤️ Weather collection #{run_num} starting...")
        result = collect_weather()
        
        if result["success"]:
            cycle_stats["weather_success"] += 1
            logger.info(f"🌤️ Weather collection #{run_num} complete: ✅ Success")
        else:
            cycle_stats["weather_fail"] += 1
            logger.error(f"🌤️ Weather collection #{run_num} failed: {result['error']}")
            
    except Exception as e:
        cycle_stats["weather_fail"] += 1
        logger.error(f"🌤️ Weather collection #{run_num} CRASHED: {type(e).__name__} - {e}")


def job_collect_air_quality():
    """Scheduled job: collect air quality data."""
    cycle_stats["aqi_runs"] += 1
    run_num = cycle_stats["aqi_runs"]
    
    try:
        logger.info(f"🌬️ Air quality collection #{run_num} starting...")
        result = collect_air_quality()
        
        if result["success"]:
            cycle_stats["aqi_success"] += 1
            logger.info(f"🌬️ Air quality collection #{run_num} complete: ✅ Success")
        else:
            cycle_stats["aqi_fail"] += 1
            logger.error(f"🌬️ Air quality collection #{run_num} failed: {result['error']}")
            
    except Exception as e:
        cycle_stats["aqi_fail"] += 1
        logger.error(f"🌬️ Air quality collection #{run_num} CRASHED: {type(e).__name__} - {e}")


def job_print_stats():
    """Scheduled job: print running statistics every 30 minutes."""
    uptime = datetime.now(timezone.utc) - cycle_stats["started_at"] if cycle_stats["started_at"] else "N/A"
    
    logger.info("")
    logger.info("=" * 55)
    logger.info("📊 SCHEDULER STATISTICS")
    logger.info("=" * 55)
    logger.info(f"  Uptime: {uptime}")
    logger.info(f"  Traffic:     {cycle_stats['traffic_runs']} runs | "
                f"{cycle_stats['traffic_success']} points OK | "
                f"{cycle_stats['traffic_fail']} points failed")
    logger.info(f"  Weather:     {cycle_stats['weather_runs']} runs | "
                f"{cycle_stats['weather_success']} OK | "
                f"{cycle_stats['weather_fail']} failed")
    logger.info(f"  Air Quality: {cycle_stats['aqi_runs']} runs | "
                f"{cycle_stats['aqi_success']} OK | "
                f"{cycle_stats['aqi_fail']} failed")
    logger.info("=" * 55)
    logger.info("")


# ================================================================
# APSCHEDULER EVENT LISTENER
# ================================================================
def job_listener(event):
    """Listen for job events and log errors."""
    if event.exception:
        logger.error(f"⚠️ Job {event.job_id} raised an exception: {event.exception}")


# ================================================================
# MAIN SCHEDULER
# ================================================================
def start_scheduler():
    """Configure and start the APScheduler."""
    
    cycle_stats["started_at"] = datetime.now(timezone.utc)
    
    # Print startup banner
    logger.info("")
    logger.info("╔══════════════════════════════════════════════════════╗")
    logger.info("║     URBAN INTELLIGENCE — DATA COLLECTION SCHEDULER  ║")
    logger.info("╚══════════════════════════════════════════════════════╝")
    logger.info("")
    logger.info(f"  Study Area:  {settings.STUDY_AREA_NAME}")
    logger.info(f"  Center:      {settings.STUDY_AREA_CENTER}")
    logger.info(f"  Points:      {len(settings.MONITORING_POINTS)} monitoring locations")
    logger.info("")
    logger.info("  📅 Schedule:")
    logger.info("     🚗 Traffic:      Every 5 minutes")
    logger.info("     🌤️ Weather:      Every 15 minutes")
    logger.info("     🌬️ Air Quality:  Every 30 minutes")
    logger.info("     📊 Stats:        Every 30 minutes")
    logger.info("")
    logger.info(f"  📁 Logs:     {LOG_DIR}")
    logger.info(f"  📁 Backups:  {settings.PROJECT_ROOT / 'data' / 'realtime'}")
    logger.info("")
    logger.info("  Press Ctrl+C to stop the scheduler.")
    logger.info("")
    
    # Create the scheduler
    scheduler = BlockingScheduler(
        job_defaults={
            "coalesce": True,          # If multiple missed runs, only run once
            "max_instances": 1,         # Don't overlap same job
            "misfire_grace_time": 60,   # Allow 60s grace for missed jobs
        }
    )
    
    # Add event listener for errors
    scheduler.add_listener(job_listener, EVENT_JOB_ERROR | EVENT_JOB_EXECUTED)
    
    # ---- Schedule jobs ----
    
    # Traffic: every 5 minutes
    scheduler.add_job(
        job_collect_traffic,
        "interval",
        minutes=5,
        id="traffic_collector",
        name="Traffic Collection (TomTom)",
        next_run_time=datetime.now(),  # Run immediately on start
    )
    
    # Weather: every 15 minutes
    scheduler.add_job(
        job_collect_weather,
        "interval",
        minutes=15,
        id="weather_collector",
        name="Weather Collection (OWM)",
        next_run_time=datetime.now(),  # Run immediately on start
    )
    
    # Air Quality: every 30 minutes
    scheduler.add_job(
        job_collect_air_quality,
        "interval",
        minutes=30,
        id="aqi_collector",
        name="Air Quality Collection (OWM)",
        next_run_time=datetime.now(),  # Run immediately on start
    )
    
    # Stats: every 30 minutes
    scheduler.add_job(
        job_print_stats,
        "interval",
        minutes=30,
        id="stats_printer",
        name="Statistics Printer",
    )
    
    # ---- Handle graceful shutdown ----
    def shutdown(signum, frame):
        logger.info("")
        logger.info("🛑 Shutdown signal received. Stopping scheduler...")
        job_print_stats()  # Print final stats
        scheduler.shutdown(wait=False)
        logger.info("👋 Scheduler stopped. Goodbye!")
    
    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)
    
    # ---- Start ----
    try:
        logger.info("🚀 Scheduler starting NOW...")
        logger.info("")
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("")
        logger.info("🛑 Scheduler interrupted.")
        job_print_stats()
        logger.info("👋 Goodbye!")


# ================================================================
# ENTRY POINT
# ================================================================
if __name__ == "__main__":
    start_scheduler()