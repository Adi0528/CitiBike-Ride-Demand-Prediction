from __future__ import annotations

import os

import pandas as pd
import plotly.graph_objects as go
import requests
import streamlit as st

st.set_page_config(
    page_title="Citi Bike Demand Dashboard",
    page_icon="🚲",
    layout="wide",
)

st.title("🚲 Citi Bike Demand Operational Performance Portal")
st.caption(
    "Live streaming metrics and continuous financial validation "
    "served through FastAPI and PostgreSQL."
)

API_BASE_URL = os.getenv("API_BASE_URL", "http://fastapi:8000")
DEFAULT_STATION_ID = os.getenv("CITIBIKE_STATION_ID", "6140.05")

OFFICIAL_TEST_SET_COST_REDUCTION_PCT = 49.1


def fetch_json(url: str) -> dict | None:
    try:
        cache_buster = int(pd.Timestamp.now().timestamp())
        separator = "&" if "?" in url else "?"
        bust_url = f"{url}{separator}_cb={cache_buster}"
        response = requests.get(bust_url, timeout=5)
        if response.status_code == 404:
            return None
        response.raise_for_status()
        return response.json()
    except requests.RequestException as exc:
        st.error(f"API request failed: {exc}")
        return None
    except ValueError as exc:
        st.error(f"API returned invalid JSON: {exc}")
        return None


def class_label(value) -> str:
    if value is None or pd.isna(value):
        return "Pending Ground Truth"
    try:
        return {0: "Low Demand", 1: "Normal Demand", 2: "High Demand"}.get(
            int(value), str(value)
        )
    except (ValueError, TypeError):
        return str(value)


def format_penalty(value) -> str:
    if value is None or pd.isna(value):
        return "Awaiting Settlement"
    try:
        return f"${float(value):,.2f}"
    except (ValueError, TypeError):
        return "Awaiting Settlement"


def make_washboard(df: pd.DataFrame) -> go.Figure:
    visible = df.copy()
    visible["hour_ts"] = pd.to_datetime(visible["hour_ts"], errors="coerce")
    visible["actual_class_numeric"] = pd.to_numeric(visible["actual_class"], errors="coerce")
    visible["predicted_class_numeric"] = pd.to_numeric(visible["predicted_class"], errors="coerce")
    visible["model_penalty_numeric"] = pd.to_numeric(visible["model_penalty"], errors="coerce")

    visible["underprediction_penalty"] = 0.0
    visible["overprediction_penalty"] = 0.0

    has_actual = (
        visible["actual_class_numeric"].notna()
        & visible["predicted_class_numeric"].notna()
        & visible["model_penalty_numeric"].notna()
    )
    underprediction = has_actual & (visible["actual_class_numeric"] > visible["predicted_class_numeric"])
    overprediction = has_actual & (visible["predicted_class_numeric"] > visible["actual_class_numeric"])

    visible.loc[underprediction, "underprediction_penalty"] = visible.loc[underprediction, "model_penalty_numeric"]
    visible.loc[overprediction, "overprediction_penalty"] = -visible.loc[overprediction, "model_penalty_numeric"]

    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=visible["hour_ts"], y=visible["underprediction_penalty"],
        name="Underprediction Cost (Missed Ride)", marker_color="#d62728",
        hovertemplate="<b>%{x|%b %d, %Y %I:%M %p}</b><br>Missed-Ride Penalty: $%{y:,.2f}<extra></extra>",
    ))
    fig.add_trace(go.Bar(
        x=visible["hour_ts"], y=visible["overprediction_penalty"],
        name="Overprediction Cost (Wasted Rebalancing)", marker_color="#ff7f0e",
        hovertemplate="<b>%{x|%b %d, %Y %I:%M %p}</b><br>Rebalancing Penalty: $%{y:,.2f}<extra></extra>",
    ))
    fig.add_trace(go.Scatter(
        x=visible["hour_ts"], y=visible["actual_class_numeric"],
        name="Actual Demand (Truth)", mode="lines+markers", yaxis="y2",
        line=dict(color="#1f77b4", width=3), line_shape="hv",
        hovertemplate="<b>%{x|%b %d, %Y %I:%M %p}</b><br>Actual Demand Tier: %{y:.0f}<extra></extra>",
    ))
    fig.add_trace(go.Scatter(
        x=visible["hour_ts"], y=visible["predicted_class_numeric"],
        name="Model Prediction", mode="lines+markers", yaxis="y2",
        line=dict(color="#9467bd", width=2, dash="dot"), line_shape="hv",
        hovertemplate="<b>%{x|%b %d, %Y %I:%M %p}</b><br>Predicted Demand Tier: %{y:.0f}<extra></extra>",
    ))

    fig.update_layout(
        template="plotly_white",
        title="Hourly Operational Demand Log — Rush Hour Spike Visibility",
        barmode="relative",
        xaxis_title="Hour",
        yaxis=dict(title="Penalty ($)", zeroline=True),
        yaxis2=dict(
            title="Demand Tier (0=Low, 1=Normal, 2=High)", overlaying="y", side="right",
            range=[-0.3, 2.3], tickmode="array", tickvals=[0, 1, 2], ticktext=["Low", "Normal", "High"],
        ),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0),
        margin=dict(t=100, r=80),
        hovermode="x unified",
        height=500,
    )
    return fig


def make_cumulative_cost(df: pd.DataFrame) -> go.Figure:
    visible = df.copy()
    visible["hour_ts"] = pd.to_datetime(visible["hour_ts"], errors="coerce")
    visible["model_penalty_numeric"] = pd.to_numeric(visible["model_penalty"], errors="coerce").fillna(0.0)
    visible["baseline_penalty_numeric"] = pd.to_numeric(visible["baseline_penalty"], errors="coerce").fillna(0.0)

    final_model_cost = float(visible["model_penalty_numeric"].sum())
    final_baseline_cost = float(visible["baseline_penalty_numeric"].sum())
    net_savings = final_baseline_cost - final_model_cost

    if final_baseline_cost > 0:
        cost_change_pct = (net_savings / final_baseline_cost) * 100
        if cost_change_pct > 0:
            subtitle = f"🔥 The ML model reduced operational cost by <b>{cost_change_pct:.1f}%</b> vs. the naive baseline over this window."
            color = "#2ca02c"
        elif cost_change_pct < 0:
            subtitle = f"⚠️ The ML model increased operational cost by <b>{abs(cost_change_pct):.1f}%</b> vs. the naive baseline over this window."
            color = "#d62728"
        else:
            subtitle = "⚖️ The ML model matched the naive baseline over this window."
            color = "grey"
    else:
        subtitle = "⚖️ No baseline cost incurred over this window."
        color = "grey"

    visible["model_cumulative"] = visible["model_penalty_numeric"].cumsum()
    visible["baseline_cumulative"] = visible["baseline_penalty_numeric"].cumsum()

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=visible["hour_ts"], y=visible["baseline_cumulative"], name="Naive Baseline",
        mode="lines", line=dict(color="grey", width=2, dash="dash"),
        hovertemplate="<b>%{x|%b %d, %Y %I:%M %p}</b><br>Baseline Cost: $%{y:,.2f}<extra></extra>",
    ))
    fig.add_trace(go.Scatter(
        x=visible["hour_ts"], y=visible["model_cumulative"], name="ML Model",
        mode="lines", line=dict(color="#d62728", width=3),
        hovertemplate="<b>%{x|%b %d, %Y %I:%M %p}</b><br>ML Model Cost: $%{y:,.2f}<extra></extra>",
    ))
    fig.update_layout(
        template="plotly_white",
        title=dict(
            text=f"Cumulative Operational Cost: ML Model vs. Naive Baseline<br><span style='font-size:13px; color:{color};'>{subtitle}</span>",
            x=0, xanchor="left", y=0.96,
        ),
        xaxis_title="Hour", yaxis_title="Total Penalty ($)",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0),
        margin=dict(t=110), hovermode="x unified", height=440,
    )
    return fig


st.sidebar.header("🎛️ Evaluation Controls")

station_id = st.sidebar.text_input("Station ID", value=DEFAULT_STATION_ID)

if not station_id:
    st.info("Enter the station ID used by the trained model.")
    st.stop()

summary = fetch_json(f"{API_BASE_URL}/citibike/summary/{station_id}")

st.markdown("### 💰 The Financial Case")
col1, col2, col3 = st.columns(3)

col1.metric(
    "Official Test-Set Cost Reduction",
    f"{OFFICIAL_TEST_SET_COST_REDUCTION_PCT:.1f}%",
    help="Computed on ~9,600 held-out test hours never seen during training or tuning.",
)

if summary:
    col2.metric("Live Demo Window Cost Reduction", f"{summary['pct_cost_reduction_vs_baseline']:.1f}%",
                 help=f"Based on the last {summary['total_hours_scored']} scored hours — a small sample, shown live for demo purposes.")
    col3.metric("Hours Scored (Live Demo)", f"{summary['total_hours_scored']:,}")
else:
    col2.metric("Live Demo Window Cost Reduction", "—")
    col3.metric("Hours Scored (Live Demo)", "—")

st.markdown("---")

payload = fetch_json(f"{API_BASE_URL}/citibike/predictions/{station_id}")

if not payload or payload.get("count", 0) == 0:
    st.warning("No predictions were returned. Check the scoring pipeline and FastAPI service.")
    st.stop()

df = pd.DataFrame.from_records(payload.get("predictions", []))

if df.empty:
    st.warning("The API returned an empty prediction timeline.")
    st.stop()

df["hour_ts"] = pd.to_datetime(df["hour_ts"], errors="coerce")
df = df.dropna(subset=["hour_ts"]).sort_values("hour_ts").reset_index(drop=True)

row = df.iloc[-1]
st.subheader(f"Latest Prediction: {row['hour_ts']:%B %d, %Y at %I:%M %p}")
st.caption(f"Showing {len(df):,} prediction records for station {station_id}.")

c1, c2, c3, c4 = st.columns(4)
c1.metric("Predicted State", class_label(row.get("predicted_class")))
c2.metric("Actual Observed State", class_label(row.get("actual_class")))
c3.metric("ML Model Loss (this hour)", format_penalty(row.get("model_penalty")))
c4.metric("Naive Baseline Loss (this hour)", format_penalty(row.get("baseline_penalty")))

st.markdown("---")

st.plotly_chart(make_washboard(df), width="stretch", key="citibike_washboard")
st.plotly_chart(make_cumulative_cost(df), width="stretch", key="citibike_cumulative_cost")

with st.expander("🔍 Latest Raw Data Audit Record"):
    st.json(row.to_dict())
