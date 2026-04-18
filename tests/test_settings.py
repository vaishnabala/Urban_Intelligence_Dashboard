import sys
from pathlib import Path

# Add project root to Python path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.config.settings import settings

print("=" * 50)
print("  URBAN INTELLIGENCE DASHBOARD — SETTINGS CHECK")
print("=" * 50)

# API Keys (show only first 6 chars for safety)
print(f"\n🔑 TomTom API Key:  {settings.TOMTOM_API_KEY[:6]}...")
print(f"🔑 OWM API Key:     {settings.OWM_API_KEY[:6]}...")

# Database
print(f"\n🗄️  Database URL:    {settings.DATABASE_URL}")
print(f"📦 Redis URL:       {settings.REDIS_URL}")

# Study Area
print(f"\n📍 Study Area:      {settings.STUDY_AREA_NAME}")
print(f"📐 Bounding Box:    {settings.STUDY_AREA_BBOX}")
print(f"🎯 Center:          {settings.STUDY_AREA_CENTER}")

# Monitoring Points
print(f"\n📡 Monitoring Points ({len(settings.MONITORING_POINTS)} locations):")
for name, coords in settings.MONITORING_POINTS.items():
    label = name.replace("_", " ").title()
    print(f"   • {label:30s} → ({coords[0]:.4f}, {coords[1]:.4f})")

# Paths
print(f"\n📁 Project Root:    {settings.PROJECT_ROOT}")
print(f"📁 Data Dir:        {settings.DATA_DIR}")

# API URLs
print(f"\n🌐 TomTom Traffic:  {settings.TOMTOM_TRAFFIC_URL[:50]}...")
print(f"🌐 OWM Weather:     {settings.OWM_CURRENT_WEATHER_URL}")

print(f"\n✅ All settings loaded successfully!")