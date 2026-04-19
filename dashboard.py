
"""
Urban Intelligence Dashboard — Streamlit App
"""
import streamlit as st
import requests
import folium
from streamlit_folium import st_folium
import plotly.graph_objects as go
from datetime import datetime
import time

# ──────────────────────────────────────
# Config
# ──────────────────────────────────────

API_BASE = "http://127.0.0.1:8000/api/v1"
MAP_CENTER = [12.905, 77.725]
MAP_ZOOM = 13

st.set_page_config(
    page_title="Urban Intelligence Dashboard",
    page_icon="🌍",
    layout="wide"
)

# ──────────────────────────────────────
# Helper: fetch from API
# ──────────────────────────────────────

def fetch(endpoint, params=None):
    try:
        r = requests.get(f"{API_BASE}{endpoint}", params=params, timeout=10)
        if r.status_code == 200:
            return r.json()
        return None
    except Exception:
        return None


# ──────────────────────────────────────
# Helper: congestion color
# ──────────────────────────────────────

def congestion_color(ratio):
    if ratio is None:
        return "gray"
    if ratio < 0.5:
        return "green"
    elif ratio < 0.75:
        return "orange"
    else:
        return "red"


# ──────────────────────────────────────
# Sidebar
# ──────────────────────────────────────

with st.sidebar:
    st.title("🌍 Urban Intelligence")
    st.caption("Sarjapur–Dommasandra Corridor, Bangalore")
    st.divider()

    st.subheader("📍 Map Layers")
    show_traffic = st.checkbox("🚦 Traffic Points", value=True)
    show_roads = st.checkbox("🛣️ Road Network", value=False)
    show_buildings = st.checkbox("🏢 Buildings", value=False)
    show_pois = st.checkbox("📌 Points of Interest", value=False)

    st.divider()

    st.subheader("🔍 POI Filter")
    poi_category = st.selectbox(
        "Category",
        ["all", "hospital", "school", "restaurant", "park", "bank", "hotel"]
    )

    st.divider()

    st.subheader("📍 Location Focus")
    locations = [
        "all",
        "sarjapur_road_junction",
        "dommasandra_circle",
        "chembenahalli",
        "varthur_gunjur_junction",
        "sarjapur_town_center",
        "carmelaram_junction",
        "harlur_road_junction",
        "iblur_wipro_junction"
    ]
    selected_location = st.selectbox("Location", locations)

    st.divider()

    auto_refresh = st.checkbox("🔄 Auto Refresh (30s)", value=False)
    if st.button("🔄 Refresh Now"):
        st.rerun()


# ──────────────────────────────────────
# Auto refresh
# ──────────────────────────────────────

if auto_refresh:
    time.sleep(30)
    st.rerun()


# ──────────────────────────────────────
# Fetch all data
# ──────────────────────────────────────

summary = fetch("/analytics/summary") or {}
traffic_data = fetch("/traffic/latest") or {}
weather_data = fetch("/weather/latest") or {}
aqi_data = fetch("/aqi/latest") or {}
anomalies_data = fetch("/analytics/anomalies") or {"count": 0, "anomalies": []}


# ──────────────────────────────────────
# Title Row
# ──────────────────────────────────────

st.title("🌍 Urban Intelligence Dashboard")
st.caption(f"Sarjapur–Dommasandra–Varthur Belt, Bengaluru  |  Last updated: {datetime.now().strftime('%H:%M:%S')}")


# ──────────────────────────────────────
# KPI Cards Row
# ──────────────────────────────────────

k1, k2, k3, k4, k5 = st.columns(5)

traffic_points = traffic_data.get("count", 0)
avg_speed = None
if traffic_data.get("readings"):
    speeds = [r["current_speed"] for r in traffic_data["readings"] if r.get("current_speed")]
    avg_speed = round(sum(speeds) / len(speeds), 1) if speeds else None

k1.metric("🚦 Traffic Points", traffic_points)
k2.metric("🚗 Avg Speed", f"{avg_speed} km/h" if avg_speed else "N/A")
k3.metric("🌡️ Temperature", f"{summary.get('weather', {}).get('temperature', 'N/A')} °C" if summary.get('weather') else "N/A")
k4.metric("💨 AQI", summary.get("aqi", {}).get("value", "N/A") if summary.get("aqi") else "N/A")
k5.metric("🚨 Anomalies", anomalies_data.get("count", 0))


st.divider()


# ──────────────────────────────────────
# Main Layout: Map + Charts
# ──────────────────────────────────────

map_col, chart_col = st.columns([3, 2])


# ── MAP ──
with map_col:
    st.subheader("🗺️ Live Map")

    m = folium.Map(location=MAP_CENTER, zoom_start=MAP_ZOOM, tiles="CartoDB positron")

    # Traffic layer
    if show_traffic and traffic_data.get("readings"):
        readings = traffic_data["readings"]
        if selected_location != "all":
            readings = [r for r in readings if r.get("location_name") == selected_location]

        for r in readings:
            color = congestion_color(r.get("congestion_ratio"))
            folium.CircleMarker(
                location=[r["lat"], r["lon"]],
                radius=12,
                color=color,
                fill=True,
                fill_color=color,
                fill_opacity=0.8,
                popup=folium.Popup(
                    f"""<b>{r.get('location_name', 'Unknown')}</b><br>
                    Speed: {r.get('current_speed')} km/h<br>
                    Free Flow: {r.get('free_flow_speed')} km/h<br>
                    Congestion: {round(r.get('congestion_ratio', 0) * 100)}%""",
                    max_width=200
                ),
                tooltip=r.get("location_name", "")
            ).add_to(m)

    # Roads layer
    if show_roads:
        roads = fetch("/geodata/roads", params={"limit": 300})
        if roads and roads.get("features"):
            for feature in roads["features"]:
                if feature.get("geometry"):
                    folium.GeoJson(
                        feature,
                        style_function=lambda x: {
                            "color": "#3388ff",
                            "weight": 2,
                            "opacity": 0.6
                        }
                    ).add_to(m)

    # Buildings layer
    if show_buildings:
        buildings = fetch("/geodata/buildings", params={"limit": 200})
        if buildings and buildings.get("features"):
            for feature in buildings["features"]:
                if feature.get("geometry"):
                    folium.GeoJson(
                        feature,
                        style_function=lambda x: {
                            "color": "#888888",
                            "fillColor": "#cccccc",
                            "weight": 1,
                            "fillOpacity": 0.4
                        }
                    ).add_to(m)

    # POIs layer
    if show_pois:
        cat = None if poi_category == "all" else poi_category
        pois = fetch("/geodata/pois", params={"category": cat, "limit": 100} if cat else {"limit": 100})
        if pois and pois.get("features"):
            for feature in pois["features"]:
                if feature.get("geometry"):
                    coords = feature["geometry"].get("coordinates", [])
                    if coords:
                        folium.Marker(
                            location=[coords[1], coords[0]],
                            popup=feature["properties"].get("name", "POI"),
                            tooltip=feature["properties"].get("category", ""),
                            icon=folium.Icon(color="purple", icon="info-sign")
                        ).add_to(m)

    st_folium(m, width=700, height=500)


# ── CHARTS ──
with chart_col:
    st.subheader("📊 Traffic Timeline")

    loc_for_chart = selected_location if selected_location != "all" else "sarjapur_road_junction"
    history = fetch(f"/traffic/history/{loc_for_chart}", params={"hours": 6})

    if history and history.get("readings"):
        times = [r["timestamp"] for r in history["readings"]]
        speeds = [r["current_speed"] for r in history["readings"]]
        congestion = [r.get("congestion_ratio", 0) * 100 for r in history["readings"]]

        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=times, y=speeds,
            name="Speed (km/h)",
            line=dict(color="#3388ff", width=2)
        ))
        fig.add_trace(go.Scatter(
            x=times, y=congestion,
            name="Congestion %",
            line=dict(color="#ff4444", width=2),
            yaxis="y2"
        ))
        fig.update_layout(
            height=220,
            margin=dict(l=0, r=0, t=20, b=0),
            yaxis=dict(title="Speed"),
            yaxis2=dict(title="Congestion %", overlaying="y", side="right"),
            legend=dict(orientation="h", y=-0.2)
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No traffic history available")

    st.subheader("🌤️ Weather & AQI")

    weather_history = fetch("/weather/history", params={"hours": 6})

    if weather_history and weather_history.get("readings"):
        times = [r["timestamp"] for r in weather_history["readings"]]
        temps = [r["temperature"] for r in weather_history["readings"]]
        humidity = [r["humidity"] for r in weather_history["readings"]]

        fig2 = go.Figure()
        fig2.add_trace(go.Scatter(
            x=times, y=temps,
            name="Temp (°C)",
            line=dict(color="#ff8800", width=2)
        ))
        fig2.add_trace(go.Scatter(
            x=times, y=humidity,
            name="Humidity %",
            line=dict(color="#00aaff", width=2),
            yaxis="y2"
        ))
        fig2.update_layout(
            height=220,
            margin=dict(l=0, r=0, t=20, b=0),
            yaxis=dict(title="Temp °C"),
            yaxis2=dict(title="Humidity %", overlaying="y", side="right"),
            legend=dict(orientation="h", y=-0.2)
        )
        st.plotly_chart(fig2, use_container_width=True)
    else:
        st.info("No weather history available")


# ──────────────────────────────────────
# Alerts Table
# ──────────────────────────────────────

st.divider()
st.subheader("🚨 Active Anomalies & Alerts")

anomalies = anomalies_data.get("anomalies", [])
if anomalies:
    for a in anomalies:
        severity = a.get("severity", "low")
        color = "🔴" if severity == "high" else "🟡" if severity == "medium" else "🟢"
        st.write(f"{color} **{a.get('anomaly_type')}** — {a.get('location_name')} — {a.get('description')}")
else:
    st.success("✅ No active anomalies detected")


# ──────────────────────────────────────
# Footer
# ──────────────────────────────────────

st.divider()
st.caption("Urban Intelligence Dashboard | Powered by FastAPI + PostGIS + Streamlit | Bengaluru, India")
