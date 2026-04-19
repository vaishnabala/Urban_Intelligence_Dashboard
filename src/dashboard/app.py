"""
Urban Intelligence Dashboard — Enhanced Map View
src/dashboard/app.py
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))




import streamlit as st
import requests
import folium
from streamlit_folium import st_folium
from datetime import datetime
import time
import pandas as pd
import plotly.graph_objects as go
# ──────────────────────────────────────
# Page Config
# ──────────────────────────────────────

st.set_page_config(
    page_title="Urban Intelligence Dashboard",
    page_icon="🌍",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ──────────────────────────────────────
# Constants
# ──────────────────────────────────────

API_BASE = "http://127.0.0.1:8000/api/v1"
MAP_CENTER = [12.905, 77.725]
MAP_ZOOM = 13

MONITORING_POINTS = {
   "sarjapur_road_junction":   (12.9165, 77.6750),  # Near Iblur/Sarjapur Main Road
        "dommasandra_circle":       (12.8832, 77.7524),  # Intersection of SH-35 and Sarjapur Road
        "chembenahalli":            (12.8793, 77.7616),  # Near the main village entrance/bus stop
        "varthur_gunjur_junction":  (12.9265, 77.7377),  # Specifically at Gunjur Village junction
        "sarjapur_town_center":     (12.8600, 77.7860),  # Central bus stand/Police station area
        "carmelaram_junction":      (12.9125, 77.7056),  # Near the railway station/Decathlon intersection
        "harlur_road_junction":     (12.9080, 77.6760),  # Entry to Harlur Road from Sarjapur Road
        "iblur_wipro_junction":     (12.9209, 77.6653),  # Major signal at Sarjapur Road-ORR intersection
}

# ──────────────────────────────────────
# Helper: Fetch from API
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
# Helper: Congestion color & radius
# ──────────────────────────────────────

def congestion_color(ratio):
    if ratio is None:
        return "#888888"
    if ratio >= 0.9:
        return "#00cc44"   # green — free flow
    elif ratio >= 0.7:
        return "#ffcc00"   # yellow — moderate
    elif ratio >= 0.5:
        return "#ff8800"   # orange — heavy
    else:
        return "#ff2200"   # red — severe

def congestion_radius(ratio):
    if ratio is None:
        return 10
    # Lower ratio = more congestion = bigger dot
    return int(10 + (1 - ratio) * 14)

def congestion_label(ratio):
    if ratio is None:
        return "Unknown"
    if ratio >= 0.9:
        return "Free Flow"
    elif ratio >= 0.7:
        return "Moderate"
    elif ratio >= 0.5:
        return "Heavy"
    else:
        return "Severe"

# ──────────────────────────────────────
# Sidebar
# ──────────────────────────────────────

with st.sidebar:
    st.image("https://cdn-icons-png.flaticon.com/512/854/854878.png", width=60)
    st.title("Control Panel")
    st.caption("Sarjapur–Dommasandra Corridor\nBengaluru, Karnataka")
    st.divider()

    # ── Layer Toggles ──
    st.subheader("🗺️ Map Layers")
    show_traffic  = st.checkbox("🚦 Traffic Points", value=True)
    show_pois     = st.checkbox("📌 Show POIs", value=False)
    show_anomalies_on_map = st.checkbox("🚨 Show Anomalies", value=True)
    show_risk     = st.checkbox("⚠️ Show Risk Zones", value=False)
    show_roads    = st.checkbox("🛣️ Road Network", value=False)

    st.divider()

    # ── Map Style ──
    st.subheader("🗺️ Map Style")
    map_style = st.selectbox(
        "Base Map",
        ["CartoDB positron", "OpenStreetMap", "CartoDB dark_matter"]
    )

    st.divider()

    # ── Location Selector ──
    st.subheader("📍 Location Focus")
    location_options = ["All Locations"] + [
        loc.replace("_", " ").title()
        for loc in MONITORING_POINTS.keys()
    ]
    selected_location_label = st.selectbox("Focus On", location_options)
    selected_location = (
        None if selected_location_label == "All Locations"
        else selected_location_label.lower().replace(" ", "_")
    )

    st.divider()

    # ── Time Range ──
    st.subheader("⏱️ Time Range")
    time_range = st.select_slider(
        "Show data from last:",
        options=["1h", "6h", "12h", "24h"],
        value="6h"
    )
    time_hours = int(time_range.replace("h", ""))

    st.divider()

    # ── POI Category ──
    if show_pois:
        st.subheader("📌 POI Category")
        poi_category = st.selectbox(
            "Category",
            ["all", "college", "Education", "Finance", "Food & Dining", 
             "Healthcare", "Place of Worship", "Shopping", "Transport",
             "hospital", "school", "restaurant", "park", "bank", "hotel"]
        )

    else:
        poi_category = "all"

    st.divider()

    # ── Refresh ──
    st.subheader("🔄 Refresh")
    auto_refresh = st.checkbox("Auto Refresh (60s)", value=False)
    if st.button("🔄 Refresh Now", use_container_width=True):
        st.rerun()

    st.divider()

    # ── Current Time ──
    st.caption(f"🕐 Current time:\n{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
# ──────────────────────────────────────
# Auto Refresh
# ──────────────────────────────────────

if auto_refresh:
    time.sleep(60)
    st.rerun()

# ──────────────────────────────────────
# Title
# ──────────────────────────────────────

st.title("🌍 Urban Intelligence Dashboard")
st.markdown(
    "**Sarjapur–Dommasandra–Varthur Belt, Bengaluru** &nbsp;|&nbsp; "
    "Real-time traffic, weather, and air quality monitoring"
)
st.divider()

# ──────────────────────────────────────
# Fetch Data
# ──────────────────────────────────────

with st.spinner("Fetching live data..."):
    traffic_data  = fetch("/traffic/latest") or {}
    summary       = fetch("/analytics/summary") or {}
    anomalies_data = fetch("/analytics/anomalies") or {"count": 0, "anomalies": []}

# ──────────────────────────────────────
# KPI Row
# ──────────────────────────────────────
readings = traffic_data.get("readings", [])
speeds = [r["current_speed"] for r in readings if r.get("current_speed")]
avg_speed = round(sum(speeds) / len(speeds), 1) if speeds else None

congestion_ratios = [r.get("congestion_ratio", 0) for r in readings if r.get("congestion_ratio") is not None]
avg_congestion = round(sum(congestion_ratios) / len(congestion_ratios) * 100, 1) if congestion_ratios else None

weather = summary.get("weather") or {}
aqi_info = summary.get("aqi") or {}

# ── Sidebar Summary Metrics ──
with st.sidebar:
    st.divider()
    st.subheader("📊 Live Summary")
    st.metric("📍 Monitoring Points", 8)
    st.metric("🚨 Active Anomalies", anomalies_data.get("count", 0))
    st.metric("🚧 Avg Congestion", f"{avg_congestion}%" if avg_congestion else "N/A")
    st.metric(
        "🌡️ Weather",
        f"{weather.get('temperature', 'N/A')}°C",
        delta=weather.get("description", "").title() if weather else None
    )
    st.metric("💨 AQI", aqi_info.get("value", "N/A") if aqi_info else "N/A")

# ── Top KPI Row ──
k1, k2, k3, k4, k5 = st.columns(5)
k1.metric("🚦 Monitoring Points", len(readings))
k2.metric("🚗 Avg Speed", f"{avg_speed} km/h" if avg_speed else "N/A")
k3.metric("🚧 Avg Congestion", f"{avg_congestion}%" if avg_congestion else "N/A")
k4.metric("🌡️ Temperature", f"{weather.get('temperature', 'N/A')} °C" if weather else "N/A")
k5.metric("💨 AQI", aqi_info.get("value", 'N/A') if aqi_info else "N/A")

# ──────────────────────────────────────
# Map Section
# ──────────────────────────────────────

st.subheader("🗺️ Live Traffic Map")

# Build Folium map
m = folium.Map(
    location=MAP_CENTER,
    zoom_start=MAP_ZOOM,
    tiles=map_style
)

# ── Layer 1: Study area boundary box ──
folium.Rectangle(
    bounds=[[12.86, 77.68], [12.95, 77.77]],
    color="#3388ff",
    weight=2,
    fill=False,
    dash_array="6 4",
    tooltip="Study Area Boundary"
).add_to(m)

# ── Layer 2: Traffic monitoring points ──
if show_traffic:
    # Build a lookup from location_name → reading
    reading_map = {}
    for r in readings:
        reading_map[r.get("location_name")] = r

    for loc_name, (lat, lon) in MONITORING_POINTS.items():
        r = reading_map.get(loc_name)

        if r:
            ratio = r.get("congestion_ratio")
            color = congestion_color(ratio)
            radius = congestion_radius(ratio)
            label = congestion_label(ratio)
            speed = r.get("current_speed", "N/A")
            free_flow = r.get("free_flow_speed", "N/A")
            ts = r.get("timestamp", "N/A")

            popup_html = f"""
            <div style="font-family:Arial; min-width:180px">
                <b style="font-size:13px">{loc_name.replace('_', ' ').title()}</b><br>
                <hr style="margin:4px 0">
                🚗 Speed: <b>{speed} km/h</b><br>
                🏎️ Free Flow: <b>{free_flow} km/h</b><br>
                🚧 f"🚧 Speed ratio: <b>{round((ratio or 0)*100)}% of free flow — {label}</b><br>
                🕐 Updated: {str(ts)[:19]}
            </div>
            """
        else:
            # No data yet — show gray marker
            color = "#888888"
            radius = 10
            label = "No Data"
            popup_html = f"""
            <div style="font-family:Arial">
                <b>{loc_name.replace('_', ' ').title()}</b><br>
                No data collected yet
            </div>
            """

        folium.CircleMarker(
            location=[lat, lon],
            radius=radius,
            color="white",
            weight=2,
            fill=True,
            fill_color=color,
            fill_opacity=0.9,
            popup=folium.Popup(popup_html, max_width=220),
            tooltip=f"{loc_name.replace('_', ' ').title()} — {label}"
        ).add_to(m)

# ── Layer 3: POIs ──
if show_pois:
    cat = None if poi_category == "all" else poi_category
    params = {"limit": 1000}
    if cat:
        params["category"] = cat
    pois = fetch("/geodata/pois", params=params)
    if pois and pois.get("features"):
        for feature in pois["features"]:
            geom = feature.get("geometry")
            if not geom:
                continue
            geom_type = geom.get("type")
            coords = geom.get("coordinates", [])
            if not coords:
                continue

            if geom_type == "Point":
                lat_p, lon_p = coords[1], coords[0]
            elif geom_type == "Polygon":
                ring = coords[0]
                lon_p = sum(c[0] for c in ring) / len(ring)
                lat_p = sum(c[1] for c in ring) / len(ring)
            else:
                continue

            name = feature["properties"].get("name") or "Unnamed POI"
            category = feature["properties"].get("category") or ""

            folium.CircleMarker(
                location=[lat_p, lon_p],
                radius=6,
                color="purple",
                fill=True,
                fill_color="purple",
                fill_opacity=0.7,
                popup=folium.Popup(
                    f"<b>{name}</b><br>Category: {category}",
                    max_width=200
                ),
                tooltip=f"📌 {name} ({category})"
            ).add_to(m)



# ── Anomalies layer ──
if show_anomalies_on_map and anomalies_data.get("anomalies"):
    anomalies_for_map = anomalies_data.get("anomalies", [])
    for a in anomalies_for_map:
        loc_name = a.get("location_name", "")
        coords_a = MONITORING_POINTS.get(loc_name)
        if not coords_a:
            continue
        lat_a, lon_a = coords_a
        severity = a.get("severity", "low")
        color_a = "#ff2200" if severity == "high" else "#ff8800" if severity == "medium" else "#ffcc00"
        icon_a = "⛔" if severity == "high" else "⚠️" if severity == "medium" else "ℹ️"

        folium.CircleMarker(
            location=[lat_a, lon_a],
            radius=18,
            color=color_a,
            weight=3,
            fill=True,
            fill_color=color_a,
            fill_opacity=0.3,
            popup=folium.Popup(
                f"""<div style='font-family:Arial'>
                <b>{icon_a} {a.get('anomaly_type','').replace('_',' ').title()}</b><br>
                Severity: <b>{severity.title()}</b><br>
                {a.get('description','')[:100]}
                </div>""",
                max_width=220
            ),
            tooltip=f"{icon_a} Anomaly — {loc_name.replace('_',' ').title()}"
        ).add_to(m)

# ── Risk zones layer ──
if show_risk:
    risk_scores_data = fetch("/analytics/risk-scores")
    if risk_scores_data and risk_scores_data.get("scores"):
        scores_dict = risk_scores_data["scores"]
        for loc_name, score_item in scores_dict.items():
            risk_score = score_item.get("congestion_score", 0) * 10  # Scale 0-10 to 0-100
            coords_r = MONITORING_POINTS.get(loc_name)
            if not coords_r:
                continue
            lat_r, lon_r = coords_r

            # Color based on risk score
            if risk_score >= 75:
                risk_color = "#ff0000"
                risk_label = "Critical Risk"
            elif risk_score >= 50:
                risk_color = "#ff8800"
                risk_label = "High Risk"
            elif risk_score >= 25:
                risk_color = "#ffcc00"
                risk_label = "Moderate Risk"
            else:
                risk_color = "#00cc44"
                risk_label = "Low Risk"

            # Draw circle zone
            folium.Circle(
                location=[lat_r, lon_r],
                radius=500,
                color=risk_color,
                weight=2,
                fill=True,
                fill_color=risk_color,
                fill_opacity=0.15,
                popup=folium.Popup(
                    f"""<div style='font-family:Arial'>
                    <b>⚠️ Risk Zone</b><br>
                    Location: {loc_name.replace('_', ' ').title()}<br>
                    Congestion Score: <b>{score_item.get('congestion_score', 0):.1f}</b>/10<br>
                    Avg Speed: {score_item.get('avg_speed', 0):.1f} km/h<br>
                    Level: <b>{risk_label}</b>
                    </div>""",
                    max_width=220
                ),
                tooltip=f"⚠️ {risk_label} — {loc_name.replace('_', ' ').title()}"
            ).add_to(m)

# ── Layer 3: Road network ──
if show_roads:
    with st.spinner("Loading road network..."):
        roads = fetch("/geodata/roads", params={"limit": 10000})
    if roads and roads.get("features"):
        # Filter to show only major roads
        # Filter to show only major roads
        major_road_types = ["Primary Road", "Secondary Road", "Trunk Road", "Tertiary Road", 
                           "Primary Road Link", "Secondary Road Link", "Trunk Road Link"]
        for feature in roads.get("features", []):
            road_type = feature.get("properties", {}).get("road_type", "")
            if road_type in major_road_types:
                folium.GeoJson(
                    feature,
                    style_function=lambda x: {
                        "color": "#3388ff",
                        "weight": 3,
                        "opacity": 0.7
                    },
                    tooltip=folium.GeoJsonTooltip(fields=["name", "road_type"])
                ).add_to(m)

# ── Legend ──
legend_html = """
<div style="
    position: fixed;
    bottom: 30px; right: 30px;
    background: white;
    border: 2px solid #ccc;
    border-radius: 8px;
    padding: 10px 14px;
    font-family: Arial;
    font-size: 13px;
    z-index: 9999;
    box-shadow: 2px 2px 6px rgba(0,0,0,0.2);
    color: #111111;
">
    <b style="color:#111111">🚦 Congestion Level</b><br>
    <span style="color:#00cc44">●</span> <span style="color:#111111">Free Flow (&lt;40%)</span><br>
    <span style="color:#ffcc00">●</span> <span style="color:#111111">Moderate (40–60%)</span><br>
    <span style="color:#ff8800">●</span> <span style="color:#111111">Heavy (60–80%)</span><br>
    <span style="color:#ff2200">●</span> <span style="color:#111111">Severe (&gt;80%)</span><br>
    <span style="color:#888888">●</span> <span style="color:#111111">No Data</span>
</div>
"""
m.get_root().html.add_child(folium.Element(legend_html))

# Display map
st_folium(m, width="100%", height=550, returned_objects=[])

# ──────────────────────────────────────
# Anomalies
# ──────────────────────────────────────
# ──────────────────────────────────────
# Anomaly Alerts Section
# ──────────────────────────────────────

st.divider()
st.subheader("🚨 Active Alerts & Anomalies")

anomalies = anomalies_data.get("anomalies", [])

if not anomalies:
    st.success("✅ No active anomalies detected — all systems normal")
else:
    # ── Summary badges ──
    high   = [a for a in anomalies if a.get("severity") == "high"]
    medium = [a for a in anomalies if a.get("severity") == "medium"]
    low    = [a for a in anomalies if a.get("severity") == "low"]

    b1, b2, b3, b4 = st.columns(4)
    b1.metric("🚨 Total Alerts", len(anomalies))
    b2.metric("🔴 High", len(high))
    b3.metric("🟡 Medium", len(medium))
    b4.metric("🟢 Low", len(low))

    st.markdown("---")

    # ── Build dataframe ──
    rows = []
    for a in anomalies:
        severity = a.get("severity", "low")
        icon = "🔴" if severity == "high" else "🟡" if severity == "medium" else "🟢"
        ts = a.get("timestamp", "")
        try:
            ts_fmt = ts[:19].replace("T", " ")
        except Exception:
            ts_fmt = str(ts)

        rows.append({
            "Severity":    f"{icon} {severity.title()}",
            "Time":        ts_fmt,
            "Location":    a.get("location_name", "N/A").replace("_", " ").title(),
            "Type":        a.get("anomaly_type", "N/A").replace("_", " ").title(),
            "Description": a.get("description", "N/A")
        })

    df = pd.DataFrame(rows)

    # ── Color styling ──
    def style_severity(val):
        if "High" in val:
            return "background-color: #ffe5e5; color: #cc0000; font-weight: bold"
        elif "Medium" in val:
            return "background-color: #fff8e1; color: #cc8800; font-weight: bold"
        else:
            return "background-color: #e9f7ef; color: #1a7a3a; font-weight: bold"

    styled_df = df.style.map(style_severity, subset=["Severity"])

    st.dataframe(
        styled_df,
        use_container_width=True,
        hide_index=True
    )

    # ── Detail cards for high severity ──
    if high:
        st.markdown("### 🔴 High Severity Alerts")
        for a in high:
            with st.expander(
                f"🔴 {a.get('anomaly_type','').replace('_',' ').title()} — "
                f"{a.get('location_name','').replace('_',' ').title()}"
            ):
                st.error(a.get("description", "No description available"))
                st.caption(f"Detected at: {str(a.get('timestamp',''))[:19]}")
# ──────────────────────────────────────
# Charts Section
# ──────────────────────────────────────

st.divider()
st.subheader("📊 Analytics Charts")

import pandas as pd

tab1, tab2, tab3, tab4 = st.tabs([
    "🚦 Traffic Speed",
    "🌤️ Weather",
    "💨 Air Quality",
    "🔥 Congestion Heatmap"
])


# ── Chart 1: Traffic Speed Timeline ──
import pandas as pd

with tab1:
    st.markdown("### 🚦 Traffic Speed Timeline")

    # Fetch traffic history for selected or all locations
    if selected_location:
        history_data = fetch(f"/traffic/history/{selected_location}", params={"hours": time_hours})
        all_histories = {selected_location: history_data}
    else:
        all_histories = {}
        for loc in MONITORING_POINTS.keys():
            h = fetch(f"/traffic/history/{loc}", params={"hours": time_hours})
            if h and h.get("readings"):
                all_histories[loc] = h

    fig1 = go.Figure()

    has_data = False
    for loc_name, history in all_histories.items():
        if not history or not history.get("readings"):
            continue
        has_data = True
        readings_list = history["readings"]
        times  = [r["timestamp"] for r in readings_list]
        speeds = [r["current_speed"] for r in readings_list]
        free_flows = [r.get("free_flow_speed") for r in readings_list]

        # Speed line
        fig1.add_trace(go.Scatter(
            x=times,
            y=speeds,
            name=loc_name.replace("_", " ").title(),
            mode="lines",
            line=dict(width=2)
        ))

        # Free flow reference line (first valid value)
        ff_vals = [f for f in free_flows if f]
        if ff_vals:
            fig1.add_hline(
                y=ff_vals[0],
                line_dash="dash",
                line_color="green",
                annotation_text="Free Flow",
                annotation_position="bottom right"
            )

        # Anomaly dots — where speed dropped > 40% below free flow
        anomaly_times  = []
        anomaly_speeds = []
        for r in readings_list:
            ff = r.get("free_flow_speed")
            sp = r.get("current_speed")
            if ff and sp and sp < ff * 0.6:
                anomaly_times.append(r["timestamp"])
                anomaly_speeds.append(sp)

        if anomaly_times:
            fig1.add_trace(go.Scatter(
                x=anomaly_times,
                y=anomaly_speeds,
                mode="markers",
                name=f"⚠️ Anomaly ({loc_name.replace('_',' ').title()})",
                marker=dict(color="red", size=10, symbol="circle")
            ))

    if not has_data:
        st.info("No traffic history data available for the selected time range.")
    else:
        fig1.update_layout(
            height=400,
            xaxis_title="Time",
            yaxis_title="Speed (km/h)",
            legend=dict(orientation="h", y=-0.2),
            margin=dict(l=0, r=0, t=20, b=0),
            hovermode="x unified"
        )
        st.plotly_chart(fig1, use_container_width=True)


# ── Chart 2: Weather Timeline ──
with tab2:
    st.markdown("### 🌤️ Weather Timeline")

    weather_history = fetch("/weather/history", params={"hours": time_hours})

    if not weather_history or not weather_history.get("readings"):
        st.info("No weather history data available.")
    else:
        wh = weather_history["readings"]
        times       = [r["timestamp"] for r in wh]
        temps       = [r.get("temperature") for r in wh]
        humidity    = [r.get("humidity") for r in wh]
        rain        = [r.get("rain_1h") or 0 for r in wh]

        fig2 = go.Figure()

        # Temperature line
        fig2.add_trace(go.Scatter(
            x=times, y=temps,
            name="Temperature (°C)",
            mode="lines",
            line=dict(color="#ff8800", width=2)
        ))

        # Humidity line
        fig2.add_trace(go.Scatter(
            x=times, y=humidity,
            name="Humidity (%)",
            mode="lines",
            line=dict(color="#00aaff", width=2),
            yaxis="y2"
        ))

        # Rain bars
        fig2.add_trace(go.Bar(
            x=times, y=rain,
            name="Rain (mm)",
            marker_color="rgba(0,100,255,0.3)",
            yaxis="y3"
        ))

        fig2.update_layout(
            height=400,
            xaxis_title="Time",
            yaxis=dict(title="Temperature (°C)", color="#ff8800"),
            yaxis2=dict(
                title="Humidity (%)",
                overlaying="y",
                side="right",
                color="#00aaff"
            ),
            yaxis3=dict(
                title="Rain (mm)",
                overlaying="y",
                side="right",
                position=0.95,
                color="#0033ff"
            ),
            legend=dict(orientation="h", y=-0.2),
            margin=dict(l=0, r=0, t=20, b=0),
            hovermode="x unified"
        )
        st.plotly_chart(fig2, use_container_width=True)


# ── Chart 3: Air Quality Timeline ──
with tab3:
    st.markdown("### 💨 Air Quality Timeline")

    # Fetch AQI history directly from DB via a custom approach
    # We'll use the latest reading and show what we have
    aqi_latest = fetch("/aqi/latest")

    # For history we'll call weather history endpoint as proxy
    # and show AQI bands
    st.info("Showing latest AQI snapshot — historical AQI chart requires more data points over time.")

    if aqi_latest:
        aqi_val = aqi_latest.get("aqi", 0)
        pm25    = aqi_latest.get("pm25", 0)
        pm10    = aqi_latest.get("pm10", 0)
        no2     = aqi_latest.get("no2", 0)
        o3      = aqi_latest.get("o3", 0)

        # AQI gauge
        fig3a = go.Figure(go.Indicator(
            mode="gauge+number",
            value=aqi_val,
            title={"text": "Air Quality Index (AQI)"},
            gauge={
                "axis": {"range": [1, 5]},
                "bar": {"color": "darkblue"},
                "steps": [
                    {"range": [1, 2], "color": "#00cc44"},
                    {"range": [2, 3], "color": "#ffcc00"},
                    {"range": [3, 4], "color": "#ff8800"},
                    {"range": [4, 5], "color": "#ff2200"},
                ],
                "threshold": {
                    "line": {"color": "black", "width": 4},
                    "thickness": 0.75,
                    "value": aqi_val
                }
            }
        ))
        fig3a.update_layout(height=300, margin=dict(l=0, r=0, t=40, b=0))
        st.plotly_chart(fig3a, use_container_width=True)

        # Pollutant bars
        fig3b = go.Figure(go.Bar(
            x=["PM2.5", "PM10", "NO2", "O3"],
            y=[pm25, pm10, no2, o3],
            marker_color=["#ff4444", "#ff8800", "#ffcc00", "#00aaff"],
            text=[f"{pm25}", f"{pm10}", f"{no2}", f"{o3}"],
            textposition="auto"
        ))
        fig3b.update_layout(
            height=300,
            title="Pollutant Levels (μg/m³)",
            margin=dict(l=0, r=0, t=40, b=0),
            yaxis_title="Concentration (μg/m³)"
        )
        st.plotly_chart(fig3b, use_container_width=True)


# ── Chart 4: Congestion Heatmap ──
with tab4:
    st.markdown("### 🔥 Congestion Heatmap (by Hour of Day)")

    # Fetch 24h history for all locations
    heatmap_data = {}
    for loc in MONITORING_POINTS.keys():
        h = fetch(f"/traffic/history/{loc}", params={"hours": 24})
        if h and h.get("readings"):
            heatmap_data[loc] = h["readings"]

    if not heatmap_data:
        st.info("No data available for heatmap. Need at least a few hours of data.")
    else:
        # Build hour x location matrix

        locations_list = list(heatmap_data.keys())
        hour_matrix = {loc: [[] for _ in range(24)] for loc in locations_list}

        for loc, rlist in heatmap_data.items():
            for r in rlist:
                try:
                    ts = r["timestamp"]
                    hour = int(ts[11:13])  # extract hour from ISO string
                    ratio = r.get("congestion_ratio")
                    if ratio is not None:
                        hour_matrix[loc][hour].append(ratio)
                except Exception:
                    continue

        # Average congestion per hour per location
        z_matrix = []
        for loc in locations_list:
            row = []
            for hour in range(24):
                vals = hour_matrix[loc][hour]
                row.append(round(sum(vals) / len(vals) * 100, 1) if vals else None)
            z_matrix.append(row)

        fig4 = go.Figure(go.Heatmap(
            z=z_matrix,
            x=[f"{h:02d}:00" for h in range(24)],
            y=[loc.replace("_", " ").title() for loc in locations_list],
            colorscale=[
                [0.0,  "#00cc44"],
                [0.4,  "#ffcc00"],
                [0.7,  "#ff8800"],
                [1.0,  "#ff2200"]
            ],
            colorbar=dict(title="Congestion %"),
            hoverongaps=False,
            zmin=0,
            zmax=100
        ))

        fig4.update_layout(
            height=400,
            xaxis_title="Hour of Day",
            yaxis_title="Location",
            margin=dict(l=0, r=0, t=20, b=0)
        )
        st.plotly_chart(fig4, use_container_width=True)
        st.caption("💡 Darker red = heavier congestion. Grey cells = no data for that hour yet.")


# ──────────────────────────────────────
# Traffic Prediction Section
# ──────────────────────────────────────

st.divider()
st.subheader("🔮 Traffic Prediction")

pred_col1, pred_col2 = st.columns([1, 2])

with pred_col1:
    st.markdown("#### ⚙️ Prediction Settings")

    pred_location = st.selectbox(
        "📍 Select Location",
        list(MONITORING_POINTS.keys()),
        format_func=lambda x: x.replace("_", " ").title(),
        key="pred_location"
    )

    pred_minutes = st.select_slider(
        "⏱️ Predict for next:",
        options=[15, 30, 45, 60],
        value=30,
        key="pred_minutes"
    )

    generate = st.button("🔮 Generate Prediction", use_container_width=True)

with pred_col2:
    st.markdown("#### 📊 Prediction Results")

    if generate:
        with st.spinner("Running ML model..."):
            pred_data = fetch(
                "/traffic/predict",
                params={"location": pred_location, "minutes": pred_minutes}
            )

        if not pred_data:
            st.error("❌ Could not generate prediction. Make sure the API is running.")
        else:
            # Get current speed for comparison
            current_speed = None
            for r in readings:
                if r.get("location_name") == pred_location:
                    current_speed = r.get("current_speed")
                    break

            # Extract prediction for selected location from predictions list
            location_pred = None
            for p in pred_data.get("predictions", []):
                if p.get("location_name") == pred_location:
                    location_pred = p
                    break

            predicted_speed = location_pred.get("predicted_speed") if location_pred else None
            congestion_pred = location_pred.get("congestion_level", "Unknown").replace("_", " ").title() if location_pred else "Unknown"
            confidence_note = location_pred.get("confidence_note", "") if location_pred else ""

            # Parse confidence from note
            if "High" in confidence_note:
                confidence = 0.85
            elif "Medium" in confidence_note:
                confidence = 0.60
            else:
                confidence = 0.35

            # ── Metric cards ──
            m1, m2, m3 = st.columns(3)

            m1.metric(
                "🔮 Predicted Speed",
                f"{predicted_speed} km/h" if predicted_speed else "N/A",
                delta=f"{round(predicted_speed - current_speed, 1)} km/h vs now"
                if predicted_speed and current_speed else None
            )

            m2.metric(
                "🚧 Congestion Level",
                congestion_pred if congestion_pred else "N/A"
            )

            m3.metric(
                "📊 Confidence",
                f"{round(confidence * 100)}%" if confidence else "N/A"
            )

            # ── Confidence bar ──
            st.markdown("**Model Confidence:**")
            conf_pct = round(confidence * 100) if confidence else 0
            if conf_pct >= 75:
                conf_color = "🟢"
            elif conf_pct >= 50:
                conf_color = "🟡"
            else:
                conf_color = "🔴"
            st.progress(conf_pct / 100)
            st.caption(f"{conf_color} {conf_pct}% confidence for next {pred_minutes} minutes")

            # ── Speed comparison chart ──
            if predicted_speed and current_speed:
                fig_pred = go.Figure()

                fig_pred.add_trace(go.Bar(
                    x=["Current Speed", f"Predicted ({pred_minutes} min)"],
                    y=[current_speed, predicted_speed],
                    marker_color=[
                        congestion_color(
                            next((r.get("congestion_ratio") for r in readings
                                  if r.get("location_name") == pred_location), None)
                        ),
                        "#8844ff"
                    ],
                    text=[f"{current_speed} km/h", f"{predicted_speed} km/h"],
                    textposition="auto"
                ))

                fig_pred.update_layout(
                    height=300,
                    title=f"Speed Forecast — {pred_location.replace('_', ' ').title()}",
                    yaxis_title="Speed (km/h)",
                    margin=dict(l=0, r=0, t=40, b=0),
                    showlegend=False
                )
                st.plotly_chart(fig_pred, use_container_width=True)

            # ── Raw response (expandable) ──
            with st.expander("🔍 Raw API Response"):
                st.json(pred_data)

    else:
        st.info("👈 Select a location and time window, then click **Generate Prediction**")


# ──────────────────────────────────────
# Footer
# ──────────────────────────────────────

st.divider()
st.caption("🌍 Urban Intelligence Dashboard | FastAPI + PostGIS + Streamlit | Bengaluru, India")
