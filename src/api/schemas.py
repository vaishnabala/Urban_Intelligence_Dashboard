"""
Pydantic response/request schemas for the Urban Intelligence API.
"""

from datetime import datetime
from typing import Optional
from pydantic import BaseModel, Field


# ================================================================
# HEALTH CHECK
# ================================================================
class HealthCheckResponse(BaseModel):
    status: str = Field(..., description="API status", examples=["healthy"])
    timestamp: datetime = Field(..., description="Server timestamp")
    database: str = Field(..., description="Database connection status")
    tables: dict = Field(default_factory=dict, description="Row counts per table")


# ================================================================
# TRAFFIC
# ================================================================
class TrafficReading(BaseModel):
    id: int
    timestamp: datetime
    location_name: str = Field(..., description="Monitoring point name")
    lat: float
    lon: float
    current_speed: float = Field(..., description="Current speed in km/h")
    free_flow_speed: float = Field(..., description="Free flow speed in km/h")
    confidence: float = Field(..., description="Data confidence 0-1")
    congestion_ratio: float = Field(..., description="free_flow / current (1.0 = free flow)")

    class Config:
        from_attributes = True


class TrafficReadingList(BaseModel):
    count: int = Field(..., description="Number of readings returned")
    readings: list[TrafficReading]


class TrafficHistory(BaseModel):
    location_name: str
    count: int
    hours: int = Field(..., description="Hours of history requested")
    readings: list[TrafficReading]


# ================================================================
# WEATHER
# ================================================================
class WeatherReading(BaseModel):
    id: int
    timestamp: datetime
    temperature: float = Field(..., description="Temperature in °C")
    humidity: float = Field(..., description="Humidity in %")
    pressure: float = Field(..., description="Pressure in hPa")
    weather_description: str = Field(..., description="Weather condition text")
    wind_speed: float = Field(..., description="Wind speed in m/s")
    rain_1h: Optional[float] = Field(0, description="Rain in last hour (mm)")
    visibility: Optional[float] = Field(10000, description="Visibility in meters")

    class Config:
        from_attributes = True


class WeatherReadingList(BaseModel):
    count: int
    readings: list[WeatherReading]


# ================================================================
# AIR QUALITY
# ================================================================
class AirQualityReading(BaseModel):
    id: int
    timestamp: datetime
    aqi: int = Field(..., description="AQI index (1=Good → 5=Very Poor)")
    aqi_label: str = Field(..., description="Human-readable AQI level")
    pm25: float = Field(..., description="PM2.5 in μg/m³")
    pm10: float = Field(..., description="PM10 in μg/m³")
    no2: float = Field(..., description="NO₂ in μg/m³")
    o3: float = Field(..., description="O₃ in μg/m³")
    co: float = Field(..., description="CO in μg/m³")
    so2: float = Field(..., description="SO₂ in μg/m³")

    class Config:
        from_attributes = True


class AirQualityReadingList(BaseModel):
    count: int
    readings: list[AirQualityReading]


# ================================================================
# ANOMALIES
# ================================================================
class AnomalyAlert(BaseModel):
    id: int
    timestamp: datetime
    anomaly_type: str = Field(..., description="Type: traffic_speed_low, pm25_spike, etc.")
    severity: str = Field(..., description="low, medium, high, critical")
    description: str
    location_name: Optional[str] = None
    lat: float
    lon: float

    class Config:
        from_attributes = True


class AnomalyAlertList(BaseModel):
    count: int
    alerts: list[AnomalyAlert]


# ================================================================
# CONGESTION ANALYSIS
# ================================================================
class CongestionScore(BaseModel):
    location_name: str
    time_window_hours: int
    readings_count: int
    congestion_score: float = Field(..., description="Score 0-100")
    congestion_level: str = Field(..., description="free_flow, light, moderate, heavy, severe")
    avg_speed: float = Field(..., description="Average speed in km/h")
    avg_free_flow_speed: float
    speed_utilization_pct: float = Field(..., description="current/freeflow * 100")
    avg_congestion_ratio: float
    peak_congestion_hour: Optional[int] = Field(None, description="Hour with worst congestion (0-23)")
    lowest_congestion_hour: Optional[int] = Field(None, description="Hour with best flow (0-23)")
    trend: str = Field(..., description="improving, stable, worsening")
    trend_slope: float


class CongestionScoreList(BaseModel):
    count: int
    time_window_hours: int
    scores: list[CongestionScore]


# ================================================================
# RISK SCORE
# ================================================================
class RiskScoreResponse(BaseModel):
    lat: float
    lon: float
    radius_m: float
    overall_score: float = Field(..., description="Combined risk 0-100")
    risk_level: str = Field(..., description="low, moderate, high, critical")
    traffic_score: float = Field(..., description="Traffic component 0-100")
    air_quality_score: float = Field(..., description="AQI component 0-100")
    weather_score: float = Field(..., description="Weather component 0-100")
    details: dict = Field(default_factory=dict)


# ================================================================
# TRAFFIC PREDICTION
# ================================================================
class TrafficPredictionItem(BaseModel):
    location_name: str
    lat: float
    lon: float
    predicted_speed: float = Field(..., description="Predicted speed in km/h")
    free_flow_speed: float
    predicted_congestion_ratio: float
    congestion_level: str
    prediction_time: datetime
    confidence_note: str


class TrafficPredictionList(BaseModel):
    future_minutes: int
    count: int
    predictions: list[TrafficPredictionItem]


# ================================================================
# GEOJSON (for map layers)
# ================================================================
class GeoJSONFeature(BaseModel):
    type: str = "Feature"
    geometry: dict
    properties: dict


class GeoJSONResponse(BaseModel):
    type: str = "FeatureCollection"
    count: int = Field(..., description="Number of features")
    features: list[GeoJSONFeature]


# ================================================================
# STATIC DATA SUMMARY
# ================================================================
class DataSummary(BaseModel):
    roads: int = Field(..., description="Number of road segments")
    buildings: int = Field(..., description="Number of buildings")
    pois: int = Field(..., description="Number of points of interest")
    traffic_readings: int = Field(..., description="Total traffic readings")
    weather_readings: int = Field(..., description="Total weather readings")
    air_quality_readings: int = Field(..., description="Total AQI readings")
    anomalies: int = Field(..., description="Total anomalies detected")
    study_area: str
    monitoring_points: int


# ================================================================
# GENERIC
# ================================================================
class ErrorResponse(BaseModel):
    error: str
    detail: Optional[str] = None