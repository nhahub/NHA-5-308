import os
import time
from datetime import datetime
import pandas as pd
import plotly.express as px
import streamlit as st

st.set_page_config(
    page_title="Fleet IoT Live Monitor",
    page_icon="🚚",
    layout="wide",
    initial_sidebar_state="expanded",
)

HBASE_HOST = "localhost"
HBASE_PORT = 9090
HBASE_TABLE = "fleet_telemetry"
PARQUET_FALLBACK = os.path.join("dwh_export", "latest_status.parquet")
PII_COLS = {"Driver_MobileNo"}

EXPECTED_COLS = [
    "vehicle_no", "fleet_id", "driver_name", "BookingID",
    "Curr_lat", "Curr_lon", "Current_Location",
    "origin", "destination", "TRANSPORTATION_DISTANCE_IN_KM",
    "delay", "ontime", "Planned_ETA", "actual_eta",
    "vehicleType", "Material_Shipped", "GpsProvider",
    "Market_Regular", "supplierNameCode", "event_time",
]

THEME = {
    "bg": "#0B1120",
    "surface": "#111827",
    "surface_alt": "#162032",
    "border": "rgba(148, 163, 184, 0.16)",
    "border_strong": "rgba(148, 163, 184, 0.28)",
    "text": "#F1F5F9",
    "muted": "#94A3B8",
    "accent": "#38BDF8",
    "ok": "#22C55E",
    "bad": "#EF4444",
    "warn": "#F59E0B",
}

STATUS_COLORS = {
    "On time": THEME["ok"],
    "Delayed": THEME["bad"],
    "Unknown": THEME["warn"],
}

FONT_STACK = (
    '"Inter", "Segoe UI", -apple-system, BlinkMacSystemFont, '
    '"Helvetica Neue", Arial, sans-serif'
)

APP_CSS = f"""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');

:root {{
    --bg: {THEME["bg"]};
    --surface: {THEME["surface"]};
    --surface-alt: {THEME["surface_alt"]};
    --border: {THEME["border"]};
    --border-strong: {THEME["border_strong"]};
    --text: {THEME["text"]};
    --muted: {THEME["muted"]};
    --accent: {THEME["accent"]};
    --ok: {THEME["ok"]};
    --bad: {THEME["bad"]};
    --warn: {THEME["warn"]};
    --r-card: 10px;
    --r-sm: 7px;
}}

html, body, [class*="css"], .stApp {{
    font-family: {FONT_STACK};
    color: var(--text);
}}

.stApp {{
    background:
        radial-gradient(circle at 12% 0%, rgba(56, 189, 248, 0.06), transparent 36%),
        radial-gradient(circle at 88% 8%, rgba(34, 197, 94, 0.04), transparent 32%),
        var(--bg);
}}

.block-container {{
    max-width: 1520px;
    padding-top: 1.75rem;
    padding-bottom: 2.75rem;
}}

footer {{ visibility: hidden; }}
hr {{
    margin: 1.35rem 0;
    border: none;
    border-top: 1px solid var(--border);
}}

.ops-kicker {{
    font-size: 0.72rem;
    font-weight: 600;
    letter-spacing: 0.08em;
    text-transform: uppercase;
    color: var(--accent);
    margin-bottom: 0.25rem;
}}

h1 {{
    font-size: 1.55rem;
    font-weight: 700;
    letter-spacing: -0.02em;
    margin: 0;
    padding: 0;
}}

h2, h3 {{
    font-size: 1.02rem;
    font-weight: 600;
    letter-spacing: -0.01em;
    margin: 0;
    padding: 0 0 0.45rem 0;
}}

[data-testid="stCaptionContainer"] {{
    color: var(--muted);
    font-size: 0.8rem;
}}

[data-testid="stSidebar"] {{
    background: var(--surface);
    border-right: 1px solid var(--border);
}}

[data-testid="stSidebar"] label p {{
    font-size: 0.8rem;
    color: var(--muted);
    font-weight: 500;
}}

div[data-testid="stVerticalBlockBorderWrapper"] {{
    background: linear-gradient(180deg, rgba(22, 32, 50, 0.72), rgba(17, 24, 39, 0.92));
    border: 1px solid var(--border);
    border-radius: var(--r-card);
    padding: 0.35rem 0.55rem;
}}

.stat-box {{
    padding: 0.25rem 0.4rem;
}}

.stat-label {{
    font-size: 0.72rem;
    color: var(--muted);
    font-weight: 500;
    margin-bottom: 0.25rem;
}}

.stat-val {{
    font-size: 0.92rem;
    font-weight: 600;
    color: var(--text);
    font-variant-numeric: tabular-nums;
    white-space: nowrap;
}}

.status-dot {{
    display: inline-block;
    width: 8px;
    height: 8px;
    border-radius: 50%;
    margin-right: 0.45rem;
    vertical-align: middle;
}}
.dot-ok {{ background: var(--ok); box-shadow: 0 0 8px rgba(34, 197, 94, 0.45); }}
.dot-bad {{ background: var(--bad); box-shadow: 0 0 8px rgba(239, 68, 68, 0.45); }}
.dot-warn {{ background: var(--warn); box-shadow: 0 0 8px rgba(245, 158, 11, 0.45); }}
.dot-idle {{ background: #64748B; }}

[data-testid="stMetric"] {{
    background: linear-gradient(180deg, var(--surface-alt), var(--surface));
    border: 1px solid var(--border);
    border-radius: var(--r-card);
    padding: 0.85rem 1rem;
    transition: border-color 0.15s ease, transform 0.15s ease;
}}

[data-testid="stMetric"]:hover {{
    border-color: var(--border-strong);
}}

[data-testid="stMetricLabel"] p {{
    font-size: 0.76rem;
    color: var(--muted);
    font-weight: 500;
    letter-spacing: 0.01em;
}}

[data-testid="stMetricValue"] {{
    font-size: 1.6rem;
    font-weight: 700;
    letter-spacing: -0.02em;
    font-variant-numeric: tabular-nums;
    color: var(--text);
}}

[data-testid="stMetricDelta"] {{
    font-size: 0.76rem;
    font-variant-numeric: tabular-nums;
}}

.stButton > button, .stDownloadButton > button {{
    background: var(--surface-alt);
    color: var(--text);
    border: 1px solid var(--border-strong);
    border-radius: var(--r-sm);
    font-weight: 500;
    font-size: 0.8rem;
    padding: 0.4rem 0.85rem;
    transition: all 0.15s ease;
}}

.stButton > button:hover, .stDownloadButton > button:hover {{
    border-color: var(--accent);
    color: var(--accent);
    background: rgba(56, 189, 248, 0.08);
}}

[data-testid="stDataFrame"] {{
    border: 1px solid var(--border);
    border-radius: var(--r-card);
    overflow: hidden;
    background: var(--surface);
}}

[data-testid="stPlotlyChart"] {{
    border: 1px solid var(--border);
    border-radius: var(--r-card);
    overflow: hidden;
    background: var(--surface);
}}

[data-testid="stAlert"] {{
    border-radius: var(--r-sm);
    font-size: 0.84rem;
}}
</style>
"""
st.markdown(APP_CSS, unsafe_allow_html=True)


def stat_block(column, label: str, value_html: str) -> None:
    column.markdown(
        f'<div class="stat-box"><div class="stat-label">{label}</div>'
        f'<div class="stat-val">{value_html}</div></div>',
        unsafe_allow_html=True,
    )


def status_dot(kind: str) -> str:
    return f'<span class="status-dot dot-{kind}"></span>'


def load_from_hbase():
    import happybase

    conn = happybase.Connection(HBASE_HOST, port=HBASE_PORT, timeout=3000)
    rows = []
    for key, data in conn.table(HBASE_TABLE).scan():
        if b"d:BookingID" not in data:
            continue
        vehicle_no, ts = key.decode("utf-8", errors="ignore").split("#", 1)
        row = {"vehicle_no": vehicle_no, "event_time": ts}
        for col, val in data.items():
            name = col.decode("utf-8", errors="ignore").split(":", 1)[1]
            if name in PII_COLS:
                continue
            row[name] = val.decode("utf-8", errors="ignore")
        rows.append(row)
    conn.close()

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows)
    df["event_time"] = pd.to_datetime(df["event_time"], errors="coerce")
    df = (
        df.sort_values("event_time", ascending=False)
        .drop_duplicates(subset=["vehicle_no"], keep="first")
        .reset_index(drop=True)
    )
    return df


def prepare_df(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    for col in EXPECTED_COLS:
        if col not in df.columns:
            df[col] = None

    df["event_time"] = pd.to_datetime(df["event_time"], errors="coerce")
    df["lat"] = pd.to_numeric(df["Curr_lat"], errors="coerce")
    df["lon"] = pd.to_numeric(df["Curr_lon"], errors="coerce")
    df["distance_km"] = pd.to_numeric(
        df["TRANSPORTATION_DISTANCE_IN_KM"], errors="coerce"
    ).fillna(0.0)

    delay_flag = df["delay"].fillna("").astype(str).str.strip().str.upper()
    ontime_flag = df["ontime"].fillna("").astype(str).str.strip().str.upper()

    df["delay_status"] = "Unknown"
    df.loc[ontime_flag == "G", "delay_status"] = "On time"
    df.loc[delay_flag == "R", "delay_status"] = "Delayed"

    planned = pd.to_datetime(df["Planned_ETA"], errors="coerce")
    actual = pd.to_datetime(df["actual_eta"], errors="coerce")
    df["eta_diff_hrs"] = ((actual - planned).dt.total_seconds() / 3600.0).round(1)

    text_cols = [
        "fleet_id", "driver_name", "origin", "destination",
        "vehicleType", "Current_Location", "GpsProvider",
        "Market_Regular", "Material_Shipped", "supplierNameCode",
    ]
    for text_col in text_cols:
        df[text_col] = (
            df[text_col]
            .fillna("Unknown")
            .astype(str)
            .str.strip()
            .replace("", "Unknown")
        )

    return df


@st.cache_data(ttl=15)
def get_latest_fleet_data(prefer_hbase=True):
    start_time = time.perf_counter()
    synced_at = datetime.now().strftime("%H:%M:%S")

    if prefer_hbase:
        try:
            df = load_from_hbase()
            if not df.empty:
                latency_ms = round((time.perf_counter() - start_time) * 1000, 1)
                return prepare_df(df), "LIVE_HBASE", latency_ms, synced_at
        except Exception:
            pass

    if os.path.exists(PARQUET_FALLBACK):
        df = pd.read_parquet(PARQUET_FALLBACK)
        latency_ms = round((time.perf_counter() - start_time) * 1000, 1)
        return prepare_df(df), "PARQUET_SNAPSHOT", latency_ms, synced_at

    return pd.DataFrame(), "NONE", 0.0, synced_at


def to_csv_bytes(dataframe: pd.DataFrame) -> bytes:
    return dataframe.to_csv(index=False).encode("utf-8-sig")


ALERT_COLUMN_CONFIG = {
    "vehicle_no": st.column_config.TextColumn("Vehicle"),
    "alert_reason": st.column_config.TextColumn("Alert Reason", width="medium"),
    "driver_name": st.column_config.TextColumn("Driver"),
    "destination": st.column_config.TextColumn("Destination"),
    "distance_km": st.column_config.NumberColumn("Distance", format="%.0f km"),
    "eta_diff_hrs": st.column_config.NumberColumn("ETA Diff", format="%+.1f h"),
}

FLEET_COLUMN_CONFIG = {
    "vehicle_no": st.column_config.TextColumn("Vehicle"),
    "delay_status": st.column_config.TextColumn("Status"),
    "alert_reason": st.column_config.TextColumn("Alert Reason", width="medium"),
    "fleet_id": st.column_config.TextColumn("Fleet"),
    "driver_name": st.column_config.TextColumn("Driver"),
    "vehicleType": st.column_config.TextColumn("Vehicle Type"),
    "Material_Shipped": st.column_config.TextColumn("Material"),
    "origin": st.column_config.TextColumn("Origin"),
    "destination": st.column_config.TextColumn("Destination"),
    "distance_km": st.column_config.NumberColumn("Distance", format="%.0f km"),
    "eta_diff_hrs": st.column_config.NumberColumn("ETA Diff", format="%+.1f h"),
    "GpsProvider": st.column_config.TextColumn("GPS Provider"),
    "Market_Regular": st.column_config.TextColumn("Trip Type"),
    "Current_Location": st.column_config.TextColumn("Current Location"),
    "event_time": st.column_config.DatetimeColumn("Last Event", format="YYYY-MM-DD HH:mm:ss"),
}

# --- Sidebar: Connection Controls ---
with st.sidebar:
    st.markdown('<div class="ops-kicker">Dispatch & Telemetry</div>', unsafe_allow_html=True)
    st.title("Fleet Control")
    use_hbase = st.toggle("Connect Live to HBase (9090)", value=True)
    if st.button("Refresh Data Now", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

df, source, latency_ms, synced_at = get_latest_fleet_data(prefer_hbase=use_hbase)

with st.sidebar:
    st.divider()
    if source == "LIVE_HBASE":
        st.success(f"Live HBase • {latency_ms} ms")
    elif source == "PARQUET_SNAPSHOT":
        st.warning(f"Parquet Snapshot • {latency_ms} ms")
    else:
        st.error("No data found. Start HBase or run export_dwh.py")
        st.stop()

    st.subheader("Alert Thresholds")
    sla_target = st.slider(
        "SLA On-Time Target (%)",
        min_value=50,
        max_value=100,
        value=95,
        step=1,
    )
    eta_threshold = st.slider(
        "ETA Overrun Alert (hrs)",
        min_value=0.0,
        max_value=24.0,
        value=2.0,
        step=0.5,
    )
    max_dist_raw = float(df["distance_km"].max()) if not df.empty else 2000.0
    slider_max_dist = max(500, int(((max_dist_raw // 100) + 1) * 100))
    default_dist_alert = min(1500, slider_max_dist)
    long_haul_threshold = st.slider(
        "Long-Haul Watch Alert (km)",
        min_value=100,
        max_value=slider_max_dist,
        value=default_dist_alert,
        step=50,
    )

    st.divider()

    st.subheader("Operational Filters")
    statuses = ["All"] + sorted(df["delay_status"].unique().tolist())
    selected_status = st.selectbox("Delivery Status", statuses)

    selected_fleets = st.multiselect(
        "Fleet / Customer",
        options=sorted(df["fleet_id"].unique().tolist()),
        placeholder="All Fleets",
    )
    selected_vehicle_types = st.multiselect(
        "Vehicle Type",
        options=sorted(df["vehicleType"].unique().tolist()),
        placeholder="All Vehicle Types",
    )
    selected_gps = st.multiselect(
        "GPS Provider",
        options=sorted(df["GpsProvider"].unique().tolist()),
        placeholder="All Providers",
    )
    market_options = ["All"] + sorted(df["Market_Regular"].unique().tolist())
    selected_market = st.selectbox("Trip Type (Market / Regular)", market_options)

# --- Apply Filters ---
filtered = df.copy()
if selected_status != "All":
    filtered = filtered[filtered["delay_status"] == selected_status]
if selected_fleets:
    filtered = filtered[filtered["fleet_id"].isin(selected_fleets)]
if selected_vehicle_types:
    filtered = filtered[filtered["vehicleType"].isin(selected_vehicle_types)]
if selected_gps:
    filtered = filtered[filtered["GpsProvider"].isin(selected_gps)]
if selected_market != "All":
    filtered = filtered[filtered["Market_Regular"] == selected_market]

filtered = filtered.copy()

# --- Compute Core Metrics & Dynamic Alerts ---
total_vehicles = len(filtered)
ontime_count = int((filtered["delay_status"] == "On time").sum())
delayed_count = int((filtered["delay_status"] == "Delayed").sum())
known_status = ontime_count + delayed_count
ontime_rate = (ontime_count / known_status * 100) if known_status > 0 else 0.0
total_distance = filtered["distance_km"].sum()
active_fleets = filtered["fleet_id"].nunique()

valid_gps_mask = (
    filtered["lat"].notna()
    & filtered["lon"].notna()
    & filtered["lat"].between(-90, 90)
    & filtered["lon"].between(-180, 180)
    & ~((filtered["lat"] == 0) & (filtered["lon"] == 0))
)
valid_gps_count = int(valid_gps_mask.sum())
gps_coverage_pct = (valid_gps_count / total_vehicles * 100) if total_vehicles > 0 else 0.0

is_delayed = filtered["delay_status"] == "Delayed"
is_eta_overrun = filtered["eta_diff_hrs"].notna() & (filtered["eta_diff_hrs"] >= eta_threshold)
is_long_haul = filtered["distance_km"] >= long_haul_threshold

alert_reasons = []
for d_flag, e_flag, l_flag in zip(is_delayed, is_eta_overrun, is_long_haul):
    reasons = []
    if d_flag:
        reasons.append("Delayed")
    if e_flag:
        reasons.append(f"ETA ≥ {eta_threshold:g}h")
    if l_flag:
        reasons.append(f"Long-Haul ≥ {long_haul_threshold}km")
    alert_reasons.append(" | ".join(reasons))

filtered["alert_reason"] = alert_reasons
alerts_df = (
    filtered[filtered["alert_reason"] != ""][
        ["vehicle_no", "alert_reason", "driver_name", "destination", "distance_km", "eta_diff_hrs"]
    ]
    .sort_values(by=["distance_km"], ascending=False)
    .copy()
)

# --- Header ---
st.markdown('<div class="ops-kicker">Supply Chain & Fleet Operations</div>', unsafe_allow_html=True)
st.title("Real-Time Fleet Telemetry & Logistics Monitor")
st.caption("Hot serving layer (HBase) • Live vehicle tracking, threshold alerts, and system health")

st.write("")

# --- System Health Bar ---
with st.container(border=True):
    h1, h2, h3, h4, h5 = st.columns(5)
    if source == "LIVE_HBASE":
        source_html = f"{status_dot('ok')}HBase Live"
    else:
        source_html = f"{status_dot('warn')}Parquet Snapshot"
    stat_block(h1, "Data Source", source_html)
    stat_block(h2, "Query Latency", f"{latency_ms} ms")
    stat_block(h3, "Last Sync (TTL 15s)", f"{synced_at}")
    stat_block(
        h4,
        "GPS Signal Health",
        f"{valid_gps_count}/{total_vehicles} ({gps_coverage_pct:.0f}%)",
    )
    if total_vehicles == 0:
        sla_html = f"{status_dot('idle')}No Data"
    elif ontime_rate >= sla_target:
        sla_html = f"{status_dot('ok')}Healthy (≥ {sla_target}%)"
    else:
        sla_html = f"{status_dot('bad')}Breach (&lt; {sla_target}%)"
    stat_block(h5, "SLA Compliance", sla_html)

st.write("")

# --- Executive KPIs ---
k1, k2, k3, k4, k5 = st.columns(5)
k1.metric("Active Vehicles", f"{total_vehicles:,}")
k2.metric(
    "On-Time Rate",
    f"{ontime_rate:.1f}%",
    f"{ontime_rate - sla_target:+.1f}% vs SLA",
)
k3.metric(
    "Active Alerts",
    f"{len(alerts_df):,}",
    f"{delayed_count:,} Delayed",
    delta_color="inverse",
)
k4.metric("Total Distance", f"{total_distance:,.0f} km")
k5.metric("Active Fleets", f"{active_fleets:,}")

st.divider()

# --- Live Map + Dynamic Alerts Feed ---
map_col, alert_col = st.columns([1.618, 1], gap="large")

with map_col:
    st.subheader("Live Fleet Positions")
    map_df = filtered[valid_gps_mask].copy()

    if not map_df.empty:
        fig = px.scatter_map(
            map_df,
            lat="lat",
            lon="lon",
            color="delay_status",
            color_discrete_map=STATUS_COLORS,
            category_orders={"delay_status": ["On time", "Delayed", "Unknown"]},
            hover_name="vehicle_no",
            hover_data={
                "fleet_id": True,
                "driver_name": True,
                "vehicleType": True,
                "Material_Shipped": True,
                "Current_Location": True,
                "destination": True,
                "distance_km": True,
                "eta_diff_hrs": True,
                "delay_status": True,
                "alert_reason": True,
                "lat": False,
                "lon": False,
            },
            labels={
                "fleet_id": "Fleet",
                "driver_name": "Driver",
                "vehicleType": "Vehicle Type",
                "Material_Shipped": "Material",
                "Current_Location": "Current Location",
                "destination": "Destination",
                "distance_km": "Distance (km)",
                "eta_diff_hrs": "ETA Diff (h)",
                "delay_status": "Status",
                "alert_reason": "Alert",
            },
            zoom=4.2,
            center={
                "lat": float(map_df["lat"].mean()),
                "lon": float(map_df["lon"].mean()),
            },
            map_style="open-street-map",
            height=520,
        )
        fig.update_traces(marker=dict(size=10, opacity=0.92))
        fig.update_layout(
            margin=dict(l=0, r=0, t=0, b=0),
            font=dict(family=FONT_STACK, size=12, color="#0F172A"),
            paper_bgcolor=THEME["surface"],
            legend=dict(
                title=dict(text="Status", font=dict(size=11, color="#334155")),
                orientation="v",
                yanchor="top",
                y=0.97,
                xanchor="left",
                x=0.015,
                bgcolor="rgba(255, 255, 255, 0.92)",
                bordercolor="#CBD5E1",
                borderwidth=1,
                font=dict(size=11.5, color="#0F172A"),
            ),
            hoverlabel=dict(
                bgcolor=THEME["surface_alt"],
                bordercolor=THEME["border_strong"],
                font=dict(family=FONT_STACK, size=12, color=THEME["text"]),
            ),
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No valid GPS coordinates available for the current filter.")

with alert_col:
    st.subheader("Live Alerts Feed")
    if alerts_df.empty:
        st.success("No vehicles match the current alert thresholds.")
    else:
        st.error(f"{len(alerts_df)} vehicle(s) triggered alert rules")
        st.dataframe(
            alerts_df,
            hide_index=True,
            height=385,
            use_container_width=True,
            column_config=ALERT_COLUMN_CONFIG,
        )
        st.download_button(
            label="Export Alerts (CSV)",
            data=to_csv_bytes(alerts_df),
            file_name=f"fleet_alerts_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
            mime="text/csv",
            use_container_width=True,
        )

st.divider()

# --- Full Live Fleet Table + CSV Export ---
tbl_header_col, tbl_export_col = st.columns([3, 1], vertical_alignment="center")
with tbl_header_col:
    st.subheader("Live Fleet Status Table")
    st.caption(f"Showing {len(filtered):,} of {len(df):,} vehicles")

table_cols = [
    "vehicle_no", "delay_status", "alert_reason", "fleet_id", "driver_name",
    "vehicleType", "Material_Shipped", "origin", "destination", "distance_km",
    "eta_diff_hrs", "GpsProvider", "Market_Regular", "Current_Location", "event_time",
]

with tbl_export_col:
    st.download_button(
        label="Export Filtered Table (CSV)",
        data=to_csv_bytes(filtered[table_cols]),
        file_name=f"fleet_status_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
        mime="text/csv",
        use_container_width=True,
    )

st.dataframe(
    filtered[table_cols],
    hide_index=True,
    use_container_width=True,
    column_config=FLEET_COLUMN_CONFIG,
)