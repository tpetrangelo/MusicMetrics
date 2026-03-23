import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from google.cloud import bigquery
from google.oauth2 import service_account

# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="MusicMetrics",
    page_icon="",
    layout="wide",
)

# ── Styling ───────────────────────────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=DM+Mono:wght@300;400;500&family=Syne:wght@400;600;800&display=swap');

html, body, [class*="css"] {
    font-family: 'DM Mono', monospace;
    background-color: #0a0a0a;
    color: #e8e8e8;
}

h1, h2, h3 {
    font-family: 'Syne', sans-serif !important;
    letter-spacing: -0.02em;
}

.block-container {
    padding-top: 2rem;
    padding-bottom: 2rem;
    max-width: 1200px;
}

div[data-testid="stMetric"] {
    background: #111;
    border: 1px solid #1e1e1e;
    border-radius: 4px;
    padding: 1rem 1.2rem;
}

div[data-testid="stMetricValue"] {
    font-family: 'Syne', sans-serif !important;
    color: #c8f060 !important;
}

div[data-testid="stMetricLabel"] {
    color: #555 !important;
    font-size: 0.7rem !important;
    text-transform: uppercase;
    letter-spacing: 0.08em;
}

.section-label {
    font-size: 0.65rem;
    color: #555;
    text-transform: uppercase;
    letter-spacing: 0.15em;
    margin-bottom: 0.75rem;
    border-bottom: 1px solid #1a1a1a;
    padding-bottom: 0.5rem;
}

footer {visibility: hidden;}
#MainMenu {visibility: hidden;}
header {visibility: hidden;}
</style>
""", unsafe_allow_html=True)

# ── BQ client ─────────────────────────────────────────────────────────────────
def get_bq_client():
    credentials = service_account.Credentials.from_service_account_info(
        st.secrets["gcp_service_account"],
        scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )
    return bigquery.Client(
        credentials=credentials,
        project=st.secrets["BIGQUERY_PROJECT"],
    )

# ── Data loading ──────────────────────────────────────────────────────────────
@st.cache_data(ttl=300)
def load_data():
    client = get_bq_client()
    p = st.secrets["BIGQUERY_PROJECT"]

    artists = client.query(f"""
        SELECT * FROM `{p}.gold.mart_plays_by_artist`
        ORDER BY total_plays DESC
    """).to_dataframe()

    weather = client.query(f"""
        SELECT * FROM `{p}.gold.mart_plays_by_weather`
        ORDER BY play_count DESC
    """).to_dataframe()

    location = client.query(f"""
        SELECT * FROM `{p}.gold.mart_plays_by_location`
        ORDER BY play_count DESC
    """).to_dataframe()

    albums = client.query(f"""
        SELECT * FROM `{p}.gold.mart_new_album_rotation`
        ORDER BY total_plays DESC
    """).to_dataframe()

    enriched = client.query(f"""
        SELECT
            played_at,
            track_name,
            artist_name,
            album_name,
            duration_ms,
            temperature_f,
            weather_description,
            time_of_day,
            day_of_week,
            days_since_release,
            latitude,
            longitude
        FROM `{p}.silver.silver_plays_enriched`
        ORDER BY played_at DESC
    """).to_dataframe()

    return artists, weather, location, albums, enriched

artists, weather, location, albums, enriched = load_data()

# ── Plotly theme ──────────────────────────────────────────────────────────────
PLOT_BG    = "#0a0a0a"
PAPER_BG   = "#0a0a0a"
GRID_COLOR = "#1a1a1a"
TEXT_COLOR = "#888"
ACCENT     = "#c8f060"
ACCENT2    = "#60c8f0"
ACCENT3    = "#f060c8"

def base_layout(title=""):
    return dict(
        title=dict(text=title, font=dict(family="Syne", size=13, color="#555"), x=0),
        plot_bgcolor=PLOT_BG,
        paper_bgcolor=PAPER_BG,
        font=dict(family="DM Mono", color=TEXT_COLOR, size=11),
        xaxis=dict(gridcolor=GRID_COLOR, linecolor="#1a1a1a", tickcolor="#333"),
        yaxis=dict(gridcolor=GRID_COLOR, linecolor="#1a1a1a", tickcolor="#333"),
        margin=dict(l=10, r=10, t=40, b=10),
        showlegend=False,
    )

# ── Header ────────────────────────────────────────────────────────────────────
st.markdown("""
<div style="margin-bottom: 2.5rem;">
    <div style="font-family: Syne, sans-serif; font-size: 2.8rem; font-weight: 800; letter-spacing: -0.03em; color: #e8e8e8; line-height: 1;">
        Music<span style="color: #c8f060;">Metrics</span>
    </div>
    <div style="font-size: 0.7rem; color: #444; letter-spacing: 0.15em; text-transform: uppercase; margin-top: 0.4rem;">
        Personal listening analytics — Apple Music + GPS + Weather
    </div>
</div>
""", unsafe_allow_html=True)

# ── Summary metrics ───────────────────────────────────────────────────────────
total_plays   = int(artists["total_plays"].sum())
total_minutes = round(float(artists["total_minutes"].sum()), 1)
total_artists = len(artists)
total_tracks  = len(enriched["track_name"].unique()) if len(enriched) > 0 else 0

c1, c2, c3, c4 = st.columns(4)
c1.metric("Total Plays",      total_plays)
c2.metric("Minutes Listened", f"{total_minutes:,.0f}")
c3.metric("Artists",          total_artists)
c4.metric("Unique Tracks",    total_tracks)

st.markdown("<div style='height: 2rem'></div>", unsafe_allow_html=True)

# ── Row 1: Artists + Weather ──────────────────────────────────────────────────
col1, col2 = st.columns([3, 2])

with col1:
    st.markdown('<div class="section-label">Top Artists by Plays</div>', unsafe_allow_html=True)
    top_n = min(15, len(artists))
    df_a = artists.head(top_n).copy()
    df_a["total_plays"] = df_a["total_plays"].astype(int)
    df_a = df_a.sort_values("total_plays")

    fig = go.Figure(go.Bar(
        x=df_a["total_plays"],
        y=df_a["artist_name"],
        orientation="h",
        marker=dict(
            color=df_a["total_plays"],
            colorscale=[[0, "#1a2a0a"], [1, ACCENT]],
            showscale=False,
        ),
        text=df_a["total_plays"],
        textposition="outside",
        textfont=dict(color="#555", size=10),
    ))
    layout = base_layout()
    layout["height"] = 380
    layout["yaxis"]["tickfont"] = dict(size=10)
    layout["xaxis"]["showgrid"] = False
    layout["yaxis"]["showgrid"] = False
    fig.update_layout(**layout)
    st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})

with col2:
    st.markdown('<div class="section-label">Plays by Weather</div>', unsafe_allow_html=True)
    df_w = weather.copy()
    df_w["play_count"] = df_w["play_count"].astype(int)

    colors = {
        "Clear":        ACCENT,
        "Cloudy":       ACCENT2,
        "Rain":         ACCENT3,
        "Snow":         "#f0e060",
        "Fog":          "#888",
        "Thunderstorm": "#f06060",
    }
    df_w["color"] = df_w["weather_category"].map(colors).fillna("#444")

    fig2 = go.Figure(go.Pie(
        labels=df_w["weather_category"],
        values=df_w["play_count"],
        hole=0.65,
        marker=dict(colors=df_w["color"].tolist(), line=dict(color=PLOT_BG, width=3)),
        textfont=dict(family="DM Mono", size=10, color="#888"),
        textinfo="label+percent",
        hovertemplate="<b>%{label}</b><br>%{value} plays<extra></extra>",
    ))
    layout2 = base_layout()
    layout2["height"] = 380
    layout2["annotations"] = [dict(
        text=f"<b>{int(df_w['play_count'].sum())}</b><br>plays",
        x=0.5, y=0.5,
        font=dict(family="Syne", size=18, color="#e8e8e8"),
        showarrow=False,
    )]
    fig2.update_layout(**layout2)
    st.plotly_chart(fig2, use_container_width=True, config={"displayModeBar": False})

# ── Row 2: Map + Time of day ──────────────────────────────────────────────────
col3, col4 = st.columns([2, 3])

with col3:
    st.markdown('<div class="section-label">Listening Locations</div>', unsafe_allow_html=True)
    df_loc = location.copy()
    df_loc["play_count"] = df_loc["play_count"].astype(int)
    df_loc["lat"] = df_loc["lat_bucket"].astype(float)
    df_loc["lon"] = df_loc["lon_bucket"].astype(float)

    fig3 = px.scatter_mapbox(
        df_loc,
        lat="lat",
        lon="lon",
        size="play_count",
        color="play_count",
        hover_name="top_artist",
        hover_data={"play_count": True, "minutes_listened": True, "lat": False, "lon": False},
        color_continuous_scale=[[0, "#1a2a0a"], [1, ACCENT]],
        size_max=30,
        zoom=9,
        mapbox_style="carto-darkmatter",
    )
    fig3.update_layout(
        height=360,
        paper_bgcolor=PLOT_BG,
        plot_bgcolor=PLOT_BG,
        margin=dict(l=0, r=0, t=0, b=0),
        coloraxis_showscale=False,
        font=dict(family="DM Mono", color=TEXT_COLOR),
    )
    st.plotly_chart(fig3, use_container_width=True, config={"displayModeBar": False})

with col4:
    st.markdown('<div class="section-label">When You Listen</div>', unsafe_allow_html=True)

    if len(enriched) > 0:
        tod_order = ["morning", "afternoon", "evening", "night"]
        tod = enriched["time_of_day"].value_counts().reindex(tod_order).fillna(0).reset_index()
        tod.columns = ["time_of_day", "count"]

        dow_map = {1: "Sun", 2: "Mon", 3: "Tue", 4: "Wed", 5: "Thu", 6: "Fri", 7: "Sat"}
        dow = enriched["day_of_week"].value_counts().sort_index().reset_index()
        dow.columns = ["day_num", "count"]
        dow["day"] = dow["day_num"].map(dow_map)

        t1, t2 = st.columns(2)

        with t1:
            fig4 = go.Figure(go.Bar(
                x=tod["time_of_day"],
                y=tod["count"],
                marker=dict(
                    color=tod["count"],
                    colorscale=[[0, "#0a1a2a"], [1, ACCENT2]],
                    showscale=False,
                ),
                text=tod["count"].astype(int),
                textposition="outside",
                textfont=dict(color="#555", size=10),
            ))
            layout4 = base_layout("Time of Day")
            layout4["height"] = 300
            layout4["xaxis"]["showgrid"] = False
            fig4.update_layout(**layout4)
            st.plotly_chart(fig4, use_container_width=True, config={"displayModeBar": False})

        with t2:
            fig5 = go.Figure(go.Bar(
                x=dow["day"],
                y=dow["count"],
                marker=dict(
                    color=dow["count"],
                    colorscale=[[0, "#1a0a2a"], [1, ACCENT3]],
                    showscale=False,
                ),
                text=dow["count"].astype(int),
                textposition="outside",
                textfont=dict(color="#555", size=10),
            ))
            layout5 = base_layout("Day of Week")
            layout5["height"] = 300
            layout5["xaxis"]["showgrid"] = False
            fig5.update_layout(**layout5)
            st.plotly_chart(fig5, use_container_width=True, config={"displayModeBar": False})
    else:
        st.info("No enriched play data available yet.")

# ── Row 3: Album rotation ─────────────────────────────────────────────────────
st.markdown("<div style='height: 1rem'></div>", unsafe_allow_html=True)
st.markdown('<div class="section-label">Album Rotation — Days Since Release at First Play</div>', unsafe_allow_html=True)

df_alb = albums.copy()
df_alb["days_until_first_play"] = df_alb["days_until_first_play"].astype(int)
df_alb["total_plays"] = df_alb["total_plays"].astype(int)
df_alb["label"] = df_alb["artist_name"] + " — " + df_alb["album_name"]
df_alb = df_alb.sort_values("days_until_first_play", ascending=True)

fig6 = go.Figure(go.Bar(
    x=df_alb["label"],
    y=df_alb["days_until_first_play"],
    marker=dict(
        color=df_alb["days_until_first_play"],
        colorscale=[[0, ACCENT], [0.3, ACCENT2], [1, "#333"]],
        showscale=False,
    ),
    text=df_alb["days_until_first_play"].astype(str) + "d",
    textposition="outside",
    textfont=dict(color="#555", size=10),
    customdata=df_alb[["total_plays", "total_minutes"]],
    hovertemplate="<b>%{x}</b><br>%{y} days after release<br>%{customdata[0]} plays<extra></extra>",
))
layout6 = base_layout()
layout6["height"] = 300
layout6["xaxis"]["tickangle"] = -20
layout6["xaxis"]["tickfont"] = dict(size=9)
layout6["xaxis"]["showgrid"] = False
fig6.update_layout(**layout6)
st.plotly_chart(fig6, use_container_width=True, config={"displayModeBar": False})

# ── Row 4: Recent plays ───────────────────────────────────────────────────────
st.markdown("<div style='height: 1rem'></div>", unsafe_allow_html=True)
st.markdown('<div class="section-label">Recent Plays</div>', unsafe_allow_html=True)

if len(enriched) > 0:
    df_recent = enriched.head(20)[["played_at", "track_name", "artist_name", "album_name", "temperature_f", "weather_description", "time_of_day"]].copy()
    df_recent["played_at"] = pd.to_datetime(df_recent["played_at"]).dt.tz_convert("America/New_York").dt.strftime("%b %d, %Y %H:%M")
    df_recent.columns = ["Played At", "Track", "Artist", "Album", "Temp (F)", "Weather", "Time of Day"]
    st.dataframe(df_recent, use_container_width=True, hide_index=True)
else:
    st.info("No play data available yet.")

# ── Footer ────────────────────────────────────────────────────────────────────
st.markdown("""
<div style="margin-top: 3rem; padding-top: 1rem; border-top: 1px solid #1a1a1a; font-size: 0.65rem; color: #333; letter-spacing: 0.1em; text-transform: uppercase;">
    MusicMetrics — iOS + Lambda + S3 + Airflow + BigQuery + dbt
</div>
""", unsafe_allow_html=True)