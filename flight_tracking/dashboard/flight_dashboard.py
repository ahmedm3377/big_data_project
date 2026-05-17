from pathlib import Path
import time

import pandas as pd
import plotly.express as px
import streamlit as st


PROJECT_DIR = Path(__file__).resolve().parents[1]
OUTPUT_DIR = PROJECT_DIR / "output"
COUNTRY_SUMMARY_PATH = OUTPUT_DIR / "country_summary_enriched"
ALERTS_PATH = OUTPUT_DIR / "alerts"
LATEST_AIRCRAFT_PATH = OUTPUT_DIR / "latest_aircraft"


st.set_page_config(
    page_title="Flight Tracking Analytics",
    layout="wide",
)


def read_parquet_dir(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()

    files = [file for file in path.glob("*.parquet") if not file.name.startswith(".")]
    if not files:
        return pd.DataFrame()

    return pd.concat((pd.read_parquet(file) for file in files), ignore_index=True)


@st.cache_data(ttl=10)
def load_data():
    summary = read_parquet_dir(COUNTRY_SUMMARY_PATH)
    alerts = read_parquet_dir(ALERTS_PATH)
    latest_aircraft = read_parquet_dir(LATEST_AIRCRAFT_PATH)
    return summary, alerts, latest_aircraft


def clean_summary(summary: pd.DataFrame) -> pd.DataFrame:
    if summary.empty:
        return summary

    summary = summary.copy()
    for column in ["window_start", "window_end", "processed_at"]:
        if column in summary.columns:
            summary[column] = pd.to_datetime(summary[column], errors="coerce")

    numeric_columns = [
        "aircraft_count",
        "avg_baro_altitude_m",
        "avg_geo_altitude_m",
        "avg_velocity_mps",
        "max_velocity_mps",
        "min_baro_altitude_m",
    ]
    for column in numeric_columns:
        if column in summary.columns:
            summary[column] = pd.to_numeric(summary[column], errors="coerce")

    return summary


def clean_alerts(alerts: pd.DataFrame) -> pd.DataFrame:
    if alerts.empty:
        return alerts

    alerts = alerts.copy()
    for column in ["api_event_time", "last_contact_time", "processed_at"]:
        if column in alerts.columns:
            alerts[column] = pd.to_datetime(alerts[column], errors="coerce")
    return alerts


def clean_latest_aircraft(aircraft: pd.DataFrame) -> pd.DataFrame:
    if aircraft.empty:
        return aircraft

    aircraft = aircraft.copy()
    for column in ["api_event_time", "last_contact_time", "ingested_at", "processed_at"]:
        if column in aircraft.columns:
            aircraft[column] = pd.to_datetime(aircraft[column], errors="coerce")

    numeric_columns = [
        "latitude",
        "longitude",
        "baro_altitude",
        "geo_altitude",
        "velocity",
        "true_track",
        "vertical_rate",
    ]
    for column in numeric_columns:
        if column in aircraft.columns:
            aircraft[column] = pd.to_numeric(aircraft[column], errors="coerce")

    aircraft["display_callsign"] = aircraft["callsign"].fillna(aircraft["icao24"])
    return aircraft


def latest_window(summary: pd.DataFrame) -> pd.DataFrame:
    if summary.empty or "window_start" not in summary.columns:
        return summary
    latest = summary["window_start"].max()
    return summary[summary["window_start"] == latest].copy()


def metric_value(value, suffix=""):
    if pd.isna(value):
        return "0"
    if isinstance(value, float):
        return f"{value:,.2f}{suffix}"
    return f"{value:,}{suffix}"


with st.sidebar:
    auto_refresh = st.checkbox("Auto refresh", value=False)
    refresh_seconds = st.slider("Refresh seconds", min_value=5, max_value=60, value=15, step=5)
    if st.button("Refresh data", use_container_width=True):
        st.cache_data.clear()
        st.rerun()


summary_df, alerts_df, latest_aircraft_df = load_data()
summary_df = clean_summary(summary_df)
alerts_df = clean_alerts(alerts_df)
latest_aircraft_df = clean_latest_aircraft(latest_aircraft_df)
current_df = latest_window(summary_df)

st.title("Flight Tracking Analytics")

if current_df.empty:
    st.warning("No processed flight analytics found yet.")
    st.stop()

total_aircraft = int(current_df["aircraft_count"].sum())
country_count = int(current_df["origin_country"].nunique())
avg_velocity = current_df["avg_velocity_mps"].mean()
alert_count = len(alerts_df)
latest_aircraft_count = len(latest_aircraft_df)

metric_cols = st.columns(4)
metric_cols[0].metric("Active Aircraft", metric_value(total_aircraft))
metric_cols[1].metric("Countries", metric_value(country_count))
metric_cols[2].metric("Avg Speed", metric_value(avg_velocity, " m/s"))
metric_cols[3].metric("Alerts", metric_value(alert_count))

st.divider()

tabs = st.tabs(["Overview", "Country Analytics", "Aircraft Map", "Latest Aircraft", "Alerts"])

with tabs[0]:
    overview_cols = st.columns([1.2, 1])
    with overview_cols[0]:
        top_countries = current_df.sort_values("aircraft_count", ascending=False).head(12)
        fig = px.bar(
            top_countries,
            x="origin_country",
            y="aircraft_count",
            color="continent",
            labels={
                "origin_country": "Country",
                "aircraft_count": "Aircraft",
                "continent": "Continent",
            },
        )
        fig.update_layout(height=420, margin=dict(l=10, r=10, t=20, b=10), legend_title_text="")
        st.plotly_chart(fig, use_container_width=True)

    with overview_cols[1]:
        continent_df = (
            current_df.groupby("continent", dropna=False)["aircraft_count"]
            .sum()
            .reset_index()
            .sort_values("aircraft_count", ascending=False)
        )
        fig = px.pie(
            continent_df,
            names="continent",
            values="aircraft_count",
            hole=0.35,
        )
        fig.update_layout(height=420, margin=dict(l=10, r=10, t=20, b=10), legend_title_text="")
        st.plotly_chart(fig, use_container_width=True)

    trend_df = summary_df.dropna(subset=["window_start"]).copy()
    if not trend_df.empty:
        trend = (
            trend_df.groupby(["window_start", "continent"], dropna=False)["aircraft_count"]
            .sum()
            .reset_index()
            .sort_values("window_start")
        )
        fig = px.line(
            trend,
            x="window_start",
            y="aircraft_count",
            color="continent",
            markers=True,
            labels={
                "window_start": "Window",
                "aircraft_count": "Aircraft",
                "continent": "Continent",
            },
        )
        fig.update_layout(height=340, margin=dict(l=10, r=10, t=20, b=10), legend_title_text="")
        st.plotly_chart(fig, use_container_width=True)

with tabs[1]:
    analytics_cols = st.columns([1, 1])
    with analytics_cols[0]:
        velocity_df = current_df.sort_values("avg_velocity_mps", ascending=False).head(12)
        fig = px.bar(
            velocity_df,
            x="avg_velocity_mps",
            y="origin_country",
            orientation="h",
            color="region",
            labels={
                "avg_velocity_mps": "Avg Speed (m/s)",
                "origin_country": "Country",
                "region": "Region",
            },
        )
        fig.update_layout(height=420, margin=dict(l=10, r=10, t=20, b=10), yaxis=dict(autorange="reversed"), legend_title_text="")
        st.plotly_chart(fig, use_container_width=True)

    with analytics_cols[1]:
        altitude_df = current_df.dropna(subset=["avg_baro_altitude_m"]).sort_values("avg_baro_altitude_m", ascending=False).head(12)
        fig = px.bar(
            altitude_df,
            x="origin_country",
            y="avg_baro_altitude_m",
            color="continent",
            labels={
                "origin_country": "Country",
                "avg_baro_altitude_m": "Avg Altitude (m)",
                "continent": "Continent",
            },
        )
        fig.update_layout(height=420, margin=dict(l=10, r=10, t=20, b=10), legend_title_text="")
        st.plotly_chart(fig, use_container_width=True)

    display_cols = [
        "origin_country",
        "region",
        "continent",
        "aircraft_count",
        "avg_baro_altitude_m",
        "avg_velocity_mps",
        "max_velocity_mps",
    ]
    display_df = current_df[[column for column in display_cols if column in current_df.columns]].copy()
    display_df = display_df.sort_values("aircraft_count", ascending=False)
    st.dataframe(display_df, use_container_width=True, hide_index=True)

with tabs[2]:
    if latest_aircraft_df.empty:
        st.info("No latest aircraft records found.")
    else:
        map_df = latest_aircraft_df.dropna(subset=["latitude", "longitude"]).copy()
        map_df["speed_label"] = map_df["velocity"].fillna(0).round(1).astype(str) + " m/s"
        map_df["map_size"] = map_df["velocity"].fillna(10).clip(lower=10, upper=350)
        fig = px.scatter_map(
            map_df,
            lat="latitude",
            lon="longitude",
            color="origin_country",
            size="map_size",
            hover_name="display_callsign",
            hover_data={
                "origin_country": True,
                "baro_altitude": ":.1f",
                "velocity": ":.1f",
                "latitude": ":.4f",
                "longitude": ":.4f",
            },
            zoom=1,
            height=620,
        )
        fig.update_layout(map_style="open-street-map", margin=dict(l=0, r=0, t=0, b=0))
        st.plotly_chart(fig, use_container_width=True)

with tabs[3]:
    aircraft_metric_cols = st.columns(3)
    aircraft_metric_cols[0].metric("Detailed Records", metric_value(latest_aircraft_count))
    aircraft_metric_cols[1].metric("Fastest Speed", metric_value(latest_aircraft_df["velocity"].max() if not latest_aircraft_df.empty else 0, " m/s"))
    aircraft_metric_cols[2].metric("Airborne", metric_value(int((latest_aircraft_df["on_ground"] == False).sum()) if not latest_aircraft_df.empty else 0))

    if latest_aircraft_df.empty:
        st.info("No latest aircraft records found.")
    else:
        detail_cols = st.columns([1, 1])
        with detail_cols[0]:
            fastest_cols = ["callsign", "origin_country", "velocity", "baro_altitude", "latitude", "longitude", "last_contact_time"]
            fastest_df = latest_aircraft_df[[column for column in fastest_cols if column in latest_aircraft_df.columns]]
            st.dataframe(
                fastest_df.sort_values("velocity", ascending=False).head(15),
                use_container_width=True,
                hide_index=True,
            )
        with detail_cols[1]:
            low_cols = ["callsign", "origin_country", "baro_altitude", "velocity", "vertical_rate", "last_contact_time"]
            low_df = latest_aircraft_df.dropna(subset=["baro_altitude"])
            low_df = low_df[[column for column in low_cols if column in low_df.columns]]
            st.dataframe(
                low_df.sort_values("baro_altitude", ascending=True).head(15),
                use_container_width=True,
                hide_index=True,
            )

        all_cols = [
            "icao24",
            "callsign",
            "origin_country",
            "latitude",
            "longitude",
            "baro_altitude",
            "geo_altitude",
            "velocity",
            "true_track",
            "vertical_rate",
            "on_ground",
            "last_contact_time",
        ]
        st.dataframe(
            latest_aircraft_df[[column for column in all_cols if column in latest_aircraft_df.columns]].sort_values(
                "last_contact_time", ascending=False
            ),
            use_container_width=True,
            hide_index=True,
        )

with tabs[4]:
    if alerts_df.empty:
        st.info("No current alerts.")
    else:
        alert_cols = [
            "alert_type",
            "callsign",
            "origin_country",
            "baro_altitude",
            "velocity",
            "api_event_time",
        ]
        st.dataframe(
            alerts_df[[column for column in alert_cols if column in alerts_df.columns]]
            .sort_values("api_event_time", ascending=False)
            .head(25),
            use_container_width=True,
            hide_index=True,
        )

if auto_refresh:
    time.sleep(refresh_seconds)
    st.cache_data.clear()
    st.rerun()
