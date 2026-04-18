# %% [markdown]
# # 📊 Urban Intelligence Dashboard — Data Exploration
# 
# This notebook explores the real-time data collected from:
# - **TomTom** — Traffic flow data (8 monitoring points)
# - **OpenWeatherMap** — Weather and Air Quality data
# 
# Study Area: **Sarjapur-Dommasandra-Varthur Belt, Bengaluru**

# %%
import sys
from pathlib import Path
sys.path.insert(0, str(Path.cwd()))

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from sqlalchemy import text
from src.database.connection import engine

print("✅ Imports loaded successfully")

# %% [markdown]
# ## 1. Load Data from PostGIS

# %%
# Load traffic data
traffic_df = pd.read_sql(
    "SELECT * FROM traffic_readings ORDER BY timestamp",
    engine
)
traffic_df["timestamp"] = pd.to_datetime(traffic_df["timestamp"])

# Load weather data
weather_df = pd.read_sql(
    "SELECT * FROM weather_readings ORDER BY timestamp",
    engine
)
weather_df["timestamp"] = pd.to_datetime(weather_df["timestamp"])

# Load air quality data
aqi_df = pd.read_sql(
    "SELECT * FROM air_quality_readings ORDER BY timestamp",
    engine
)
aqi_df["timestamp"] = pd.to_datetime(aqi_df["timestamp"])

print(f"📊 Data loaded:")
print(f"   Traffic:     {len(traffic_df):,} rows")
print(f"   Weather:     {len(weather_df):,} rows")
print(f"   Air Quality: {len(aqi_df):,} rows")

# %% [markdown]
# ## 2. Traffic Data Overview

# %%
# Basic stats per location
traffic_stats = traffic_df.groupby("location_name").agg(
    readings=("id", "count"),
    avg_speed=("current_speed", "mean"),
    min_speed=("current_speed", "min"),
    max_speed=("current_speed", "max"),
    avg_free_flow=("free_flow_speed", "mean"),
    avg_congestion=("congestion_ratio", "mean"),
).round(1)

traffic_stats["speed_pct"] = (
    (traffic_stats["avg_speed"] / traffic_stats["avg_free_flow"]) * 100
).round(1)

print(traffic_stats.to_string())

# %% [markdown]
# ## 3. Traffic Speed Over Time (All Locations)

# %%
# Clean location names for display
traffic_df["location_display"] = (
    traffic_df["location_name"]
    .str.replace("_", " ")
    .str.title()
)

fig = px.line(
    traffic_df,
    x="timestamp",
    y="current_speed",
    color="location_display",
    title="🚗 Traffic Speed Over Time — All Monitoring Points",
    labels={
        "timestamp": "Time",
        "current_speed": "Current Speed (km/h)",
        "location_display": "Location",
    },
)
fig.update_layout(
    height=500,
    legend=dict(orientation="h", yanchor="bottom", y=-0.4),
    hovermode="x unified",
)
fig.show()

# %% [markdown]
# ## 4. Congestion Ratio Over Time

# %%
fig = px.line(
    traffic_df,
    x="timestamp",
    y="congestion_ratio",
    color="location_display",
    title="🚦 Congestion Ratio Over Time (higher = more congested)",
    labels={
        "timestamp": "Time",
        "congestion_ratio": "Congestion Ratio (free_flow / current)",
        "location_display": "Location",
    },
)
fig.add_hline(y=1.0, line_dash="dash", line_color="green", annotation_text="Free flow")
fig.add_hline(y=1.3, line_dash="dash", line_color="orange", annotation_text="Moderate")
fig.add_hline(y=2.0, line_dash="dash", line_color="red", annotation_text="Heavy")
fig.update_layout(
    height=500,
    legend=dict(orientation="h", yanchor="bottom", y=-0.4),
    hovermode="x unified",
)
fig.show()

# %% [markdown]
# ## 5. Average Congestion by Location (Bar Chart)

# %%
avg_congestion = (
    traffic_df.groupby("location_display")["congestion_ratio"]
    .mean()
    .sort_values(ascending=True)
    .reset_index()
)

fig = px.bar(
    avg_congestion,
    x="congestion_ratio",
    y="location_display",
    orientation="h",
    title="📊 Average Congestion Ratio by Location",
    labels={
        "congestion_ratio": "Avg Congestion Ratio",
        "location_display": "Location",
    },
    color="congestion_ratio",
    color_continuous_scale=["green", "yellow", "orange", "red"],
)
fig.update_layout(height=400)
fig.show()

# %% [markdown]
# ## 6. Weather — Temperature Over Time

# %%
fig = make_subplots(
    rows=3, cols=1,
    subplot_titles=("🌡️ Temperature (°C)", "💧 Humidity (%)", "💨 Wind Speed (m/s)"),
    shared_xaxes=True,
    vertical_spacing=0.08,
)

fig.add_trace(
    go.Scatter(x=weather_df["timestamp"], y=weather_df["temperature"],
               mode="lines+markers", name="Temperature", line=dict(color="red")),
    row=1, col=1,
)

fig.add_trace(
    go.Scatter(x=weather_df["timestamp"], y=weather_df["humidity"],
               mode="lines+markers", name="Humidity", line=dict(color="blue")),
    row=2, col=1,
)

fig.add_trace(
    go.Scatter(x=weather_df["timestamp"], y=weather_df["wind_speed"],
               mode="lines+markers", name="Wind Speed", line=dict(color="green")),
    row=3, col=1,
)

fig.update_layout(
    height=700,
    title_text="🌤️ Weather Conditions Over Time",
    showlegend=False,
)
fig.show()

# %% [markdown]
# ## 7. Air Quality Over Time

# %%
fig = make_subplots(
    rows=2, cols=1,
    subplot_titles=("📊 AQI Index (1=Good → 5=Very Poor)", "Pollutant Levels (μg/m³)"),
    shared_xaxes=True,
    vertical_spacing=0.12,
)

# AQI line
aqi_colors = {1: "green", 2: "yellowgreen", 3: "orange", 4: "red", 5: "purple"}
fig.add_trace(
    go.Scatter(
        x=aqi_df["timestamp"], y=aqi_df["aqi"],
        mode="lines+markers", name="AQI",
        line=dict(color="purple", width=2),
        marker=dict(size=8),
    ),
    row=1, col=1,
)
fig.update_yaxes(range=[0, 6], dtick=1, row=1, col=1)

# Pollutant lines
pollutants = {
    "pm25": ("PM2.5", "red"),
    "pm10": ("PM10", "orange"),
    "no2": ("NO₂", "blue"),
    "o3": ("O₃", "green"),
}

for col_name, (label, color) in pollutants.items():
    fig.add_trace(
        go.Scatter(
            x=aqi_df["timestamp"], y=aqi_df[col_name],
            mode="lines+markers", name=label,
            line=dict(color=color),
        ),
        row=2, col=1,
    )

fig.update_layout(
    height=600,
    title_text="🌬️ Air Quality Over Time",
)
fig.show()

# %% [markdown]
# ## 8. Data Quality Summary

# %%
print("=" * 55)
print("📋 DATA QUALITY SUMMARY")
print("=" * 55)

# Traffic checks
print(f"\n🚗 TRAFFIC:")
print(f"   Rows: {len(traffic_df):,}")
print(f"   Time range: {traffic_df['timestamp'].min()} → {traffic_df['timestamp'].max()}")
print(f"   Speed range: {traffic_df['current_speed'].min():.1f} — {traffic_df['current_speed'].max():.1f} km/h")
print(f"   NULLs in current_speed: {traffic_df['current_speed'].isna().sum()}")
print(f"   Zero speeds: {(traffic_df['current_speed'] == 0).sum()}")
print(f"   Locations tracked: {traffic_df['location_name'].nunique()}")

# Weather checks
print(f"\n🌤️ WEATHER:")
print(f"   Rows: {len(weather_df):,}")
print(f"   Time range: {weather_df['timestamp'].min()} → {weather_df['timestamp'].max()}")
print(f"   Temp range: {weather_df['temperature'].min():.1f} — {weather_df['temperature'].max():.1f} °C")
print(f"   Humidity range: {weather_df['humidity'].min():.0f} — {weather_df['humidity'].max():.0f} %")
print(f"   NULLs in temperature: {weather_df['temperature'].isna().sum()}")

# AQI checks
print(f"\n🌬️ AIR QUALITY:")
print(f"   Rows: {len(aqi_df):,}")
print(f"   Time range: {aqi_df['timestamp'].min()} → {aqi_df['timestamp'].max()}")
print(f"   AQI range: {aqi_df['aqi'].min()} — {aqi_df['aqi'].max()}")
print(f"   PM2.5 range: {aqi_df['pm25'].min():.1f} — {aqi_df['pm25'].max():.1f}")
print(f"   NULLs in aqi: {aqi_df['aqi'].isna().sum()}")

# Overall verdict
total_nulls = (
    traffic_df[["current_speed", "free_flow_speed", "congestion_ratio"]].isna().sum().sum()
    + weather_df[["temperature", "humidity"]].isna().sum().sum()
    + aqi_df[["aqi", "pm25"]].isna().sum().sum()
)

print(f"\n{'=' * 55}")
if total_nulls == 0:
    print("✅ ALL DATA QUALITY CHECKS PASSED!")
else:
    print(f"⚠️  {total_nulls} NULL values found in critical columns")
print("=" * 55)