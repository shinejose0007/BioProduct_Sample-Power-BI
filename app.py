# app.py
import os
import sys
import sqlite3
import hashlib
import secrets
from datetime import datetime

import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go

# Fix PySpark on Windows: ensure Spark worker python matches current interpreter
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession

# Import user modules from src
from src.synthetic import generate_multiple_runs
from src.etl import save_parquet, save_csv_for_powerbi
# optional run_etl in src.etl; we don't require it
try:
    from src.etl import run_etl_pipeline as _run_etl_src
    HAVE_RUN_ETL = True
except Exception:
    _run_etl_src = None
    HAVE_RUN_ETL = False

from src.quality import compute_quality_metrics
from src.models import train_anomaly_detector, detect_anomalies

# Provide fallback run_etl_pipeline if src doesn't export it
def run_etl_pipeline(df=None, input_csv=None, out_parquet_dir="data/parquet", out_csv_file="data/powerbi_export.csv",
                     partition_cols=None, limit_rows_for_output=None):
    if HAVE_RUN_ETL and _run_etl_src is not None:
        # Try to call the provided implementation; catch unexpected signatures
        try:
            if df is not None:
                return _run_etl_src(df=df, out_parquet_dir=out_parquet_dir, out_csv_file=out_csv_file,
                                    partition_cols=partition_cols, limit_rows_for_output=limit_rows_for_output)
            else:
                return _run_etl_src(input_csv=input_csv, out_parquet_dir=out_parquet_dir, out_csv_file=out_csv_file,
                                    partition_cols=partition_cols)
        except Exception:
            pass
    # minimal fallback: write parquet (if possible) and csv
    if df is None and input_csv is not None:
        df = pd.read_csv(input_csv, parse_dates=["timestamp", "ingest_time"], low_memory=False)
    if df is None:
        raise ValueError("Either df or input_csv must be provided")
    df_to_save = df if limit_rows_for_output is None else df.head(limit_rows_for_output)
    try:
        save_parquet(df_to_save, out_dir=out_parquet_dir, partition_cols=partition_cols)
    except Exception:
        pass
    save_csv_for_powerbi(df_to_save, out_file=out_csv_file)
    return df_to_save

# ------------------------
# Spark session
# ------------------------
spark = SparkSession.builder \
    .appName("Bioproduction Data Platform") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

# ------------------------
# Paths & DB helpers
# ------------------------
DATA_DIR = "data"
CSV_PBI = os.path.join(DATA_DIR, "powerbi_export.csv")
PARQUET_DIR = os.path.join(DATA_DIR, "parquet")
DB_PATH = os.path.join(DATA_DIR, "users.db")
os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(PARQUET_DIR, exist_ok=True)

def get_db_connection():
    return sqlite3.connect(DB_PATH, check_same_thread=False)

def init_user_db():
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS users (
            username TEXT PRIMARY KEY,
            email TEXT,
            salt TEXT,
            pwd_hash TEXT,
            created_at TEXT
        )
    """)
    conn.commit()
    conn.close()

def generate_salt():
    return secrets.token_bytes(16)

def hash_password(password: str, salt: bytes) -> str:
    return hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, 150000).hex()

def add_user(username: str, email: str, password: str) -> bool:
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT username FROM users WHERE username = ?", (username,))
    if cur.fetchone():
        conn.close()
        return False
    salt = generate_salt()
    pwd_hash = hash_password(password, salt)
    cur.execute(
        "INSERT INTO users (username, email, salt, pwd_hash, created_at) VALUES (?, ?, ?, ?, ?)",
        (username, email, salt.hex(), pwd_hash, datetime.utcnow().isoformat())
    )
    conn.commit()
    conn.close()
    return True

def verify_user(username: str, password: str) -> bool:
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT salt, pwd_hash FROM users WHERE username = ?", (username,))
    row = cur.fetchone()
    conn.close()
    if not row:
        return False
    salt_hex, stored_hash = row
    salt = bytes.fromhex(salt_hex)
    check_hash = hash_password(password, salt)
    return secrets.compare_digest(check_hash, stored_hash)

# Initialize user DB
init_user_db()

# ------------------------
# Streamlit UI setup
# ------------------------
st.set_page_config(page_title="Bioproduction Data Platform", layout="wide")
st.title("Bioproduction Data Platform")

# session state
if "user" not in st.session_state:
    st.session_state.user = None

# top-right user info
cols = st.columns([3, 1])
with cols[1]:
    if st.session_state.user:
        st.markdown(f"**Logged in as:** {st.session_state.user}")
        if st.button("Logout"):
            st.session_state.user = None
            st.experimental_rerun()
    else:
        st.markdown("**Not logged in**")

# ------------------------
# Sidebar: auth + controls
# ------------------------
st.sidebar.header("User")
auth_mode = st.sidebar.radio("Action", ["Login", "Register", "Guest"], index=0)

if st.session_state.user is None:
    if auth_mode == "Login":
        st.sidebar.subheader("Login")
        login_user = st.sidebar.text_input("Username", key="login_user")
        login_pw = st.sidebar.text_input("Password", type="password", key="login_pw")
        if st.sidebar.button("Login"):
            if verify_user(login_user.strip(), login_pw):
                st.session_state.user = login_user.strip()
                st.success(f"Welcome, {st.session_state.user}!")
                st.experimental_rerun()
            else:
                st.error("Invalid username or password.")
    elif auth_mode == "Register":
        st.sidebar.subheader("Register New User")
        reg_user = st.sidebar.text_input("Username", key="reg_user")
        reg_email = st.sidebar.text_input("Email", key="reg_email")
        reg_pw = st.sidebar.text_input("Password", type="password", key="reg_pw")
        reg_pw2 = st.sidebar.text_input("Repeat Password", type="password", key="reg_pw2")
        if st.sidebar.button("Register"):
            if not reg_user or not reg_pw:
                st.error("Please provide username and password.")
            elif reg_pw != reg_pw2:
                st.error("Passwords do not match.")
            else:
                ok = add_user(reg_user.strip(), reg_email.strip(), reg_pw)
                if ok:
                    st.success("Registration successful. Please login.")
                else:
                    st.error("User exists. Choose another username.")
    else:
        st.sidebar.write("Continue as guest (restricted access).")
        if st.sidebar.button("Continue as Guest"):
            st.session_state.user = "guest"
            st.experimental_rerun()

# Controls for ETL and plotting
st.sidebar.header("ETL & Visualization Controls")
n_runs = st.sidebar.slider("Number of synthetic runs", 1, 10, 3)
points_per_run = st.sidebar.selectbox("Points per run (time-series length)", [60, 360, 720, 1440], index=1)
seed = st.sidebar.number_input("Random seed", value=42, step=1)

# aggregation + plot options
agg_option = st.sidebar.selectbox("Aggregation (used in aggregated plot & daily)", ["mean", "min", "max", "std"], index=0)
rolling_window = st.sidebar.slider("Rolling window (points)", 1, 120, 10)

# how many rows to pull into Pandas for plotting (tunable)
max_rows_for_pandas = int(st.sidebar.number_input("Max rows for Pandas (plots)", value=20000, step=1000, min_value=1000))

# ------------------------
# Main app (visible after login/guest)
# ------------------------
if st.session_state.user:
    st.success(f"Signed in as: {st.session_state.user}")
    st.markdown("Demonstration: ETL → Data Quality → Anomaly Detection → Dashboard")

    # ETL / generate data button
    if st.sidebar.button("Generate data & run ETL"):
        with st.spinner("Generating synthetic data..."):
            df_new = generate_multiple_runs(n_runs=n_runs, points_per_run=points_per_run, seed=seed)
            # save parquet partitioned by date if available, else simple save
            try:
                if "date" in df_new.columns:
                    save_parquet(df_new, out_dir=PARQUET_DIR, partition_cols=["date"])
                else:
                    save_parquet(df_new, out_dir=PARQUET_DIR, partition_cols=None)
            except Exception:
                # ignore PARQUET failures
                pass
            save_csv_for_powerbi(df_new, out_file=CSV_PBI)
        st.success(f"Generated {len(df_new)} rows and saved to data/")

    # load dataset (Spark) if no upload, but we will avoid toPandas for uploaded files
    csv_path = CSV_PBI  # consistent name

    # file upload (optional) — read directly into pandas (avoid Spark -> toPandas issues)
    uploaded = st.file_uploader("Upload CSV (optional) — will replace current dataset", type=["csv"])
    pdf = None
    used_upload = False
    if uploaded:
        try:
            # read uploaded directly into pandas, limit rows if user configured
            df_uploaded = pd.read_csv(uploaded, parse_dates=["timestamp", "ingest_time"], low_memory=False)
            st.success(f"Uploaded {uploaded.name} ({len(df_uploaded)} rows)")
            if len(df_uploaded) > max_rows_for_pandas:
                st.warning(f"Large upload; truncating to {max_rows_for_pandas} rows for plotting/speed.")
                df_uploaded = df_uploaded.head(max_rows_for_pandas)
            # persist uploaded as the powerbi_export.csv (optional)
            save_csv_for_powerbi(df_uploaded, out_file=csv_path)
            pdf = df_uploaded
            used_upload = True
        except Exception as e:
            st.error(f"Error reading uploaded file into pandas: {e}")
            st.stop()
    else:
        # No upload — try to read existing powerbi CSV via Spark, convert to pandas with fallback
        if os.path.exists(csv_path):
            try:
                spark_df = spark.read.option("header", True).option("inferSchema", True).csv(csv_path)
                try:
                    pdf = spark_df.limit(max_rows_for_pandas).toPandas()
                except Exception as e:
                    st.warning("Spark → Pandas conversion failed; falling back to pandas.read_csv with nrows.")
                    st.warning(str(e))
                    try:
                        pdf = pd.read_csv(csv_path, parse_dates=["timestamp", "ingest_time"], nrows=max_rows_for_pandas, low_memory=False)
                    except Exception as e2:
                        st.error(f"Fallback read_csv failed: {e2}")
                        st.stop()
            except Exception as e:
                st.warning(f"Spark failed to read CSV; falling back to pandas.read_csv: {e}")
                try:
                    pdf = pd.read_csv(csv_path, parse_dates=["timestamp", "ingest_time"], nrows=max_rows_for_pandas, low_memory=False)
                except Exception as e2:
                    st.error(f"Failed to read {csv_path}: {e2}")
                    st.stop()
        else:
            # No CSV present — generate a small sample automatically
            pdf = generate_multiple_runs(n_runs=2, points_per_run=360, seed=1)
            save_csv_for_powerbi(pdf, out_file=csv_path)

    # At this point pdf is a pandas DataFrame limited to max_rows_for_pandas
    if pdf is None:
        st.error("No dataframe could be loaded.")
        st.stop()

    # Normalize timestamp column
    if "timestamp" in pdf.columns:
        pdf["timestamp"] = pd.to_datetime(pdf["timestamp"], errors="coerce")
    else:
        st.error("Dataset missing 'timestamp' column.")
        st.stop()

    # compute quality metrics
    try:
        metrics = compute_quality_metrics(pdf)
    except Exception:
        metrics = {"rows": len(pdf), "qc_failures": int((pdf.get("qc_pass") == 0).sum() if "qc_pass" in pdf.columns else 0)}

    # anomaly detection
    try:
        clf = train_anomaly_detector(pdf)
        pdf_anom = detect_anomalies(clf, pdf)
        if "anomaly" not in pdf_anom.columns:
            pdf_anom["anomaly"] = 0
    except Exception:
        pdf_anom = pdf.copy()
        pdf_anom["anomaly"] = 0

    # ensure main pdf has anomaly column merged
    if "anomaly" not in pdf.columns:
        pdf["anomaly"] = pdf_anom["anomaly"]

    # sensor list detection
    candidate_sensors = [c for c in pdf.columns if c not in ("timestamp", "batch_id", "ingest_time", "qc_pass", "qc_reason", "anomaly", "date")]
    if not candidate_sensors:
        candidate_sensors = [s for s in ("temp_c", "pH", "conductivity") if s in pdf.columns]
    if not candidate_sensors:
        st.error("No sensor columns found (expected temp_c, pH, conductivity).")
        st.stop()

    selected_sensor = st.sidebar.selectbox("Select sensor for plots", candidate_sensors, index=0)

    # display quick metrics
    st.header("Quick Metrics")
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Rows", metrics.get("rows", len(pdf)))
    col2.metric("QC Failures", int(metrics.get("qc_failures", 0)))
    col3.metric("Batches", int(len(pdf["batch_id"].unique()) if "batch_id" in pdf.columns else 0))
    overall_fail_rate = float(metrics.get("qc_failures", 0) / max(metrics.get("rows", len(pdf)), 1) * 100)
    col4.metric("Failure rate (%)", f"{overall_fail_rate:.2f}")

    st.subheader("Data Quality Summary")
    st.json(metrics)

    # ------------------------
    # Single-batch plots
    # ------------------------
    st.header("Single-Batch Analysis")
    batch_sel = st.selectbox("Select batch", pdf["batch_id"].unique())
    df_batch = pdf[pdf["batch_id"] == batch_sel].copy()
    if "anomaly" not in df_batch:
        df_batch["anomaly"] = 0
    df_batch = df_batch.set_index("timestamp").sort_index()

    # time-series with anomaly overlay
    fig_ts = px.line(df_batch, y=[selected_sensor], title=f"Batch {batch_sel} — {selected_sensor} (time-series)")
    if df_batch["anomaly"].any():
        anomalies_idx = df_batch[df_batch["anomaly"] == 1].index
        if selected_sensor in df_batch.columns and len(anomalies_idx) > 0:
            fig_ts.add_scatter(x=anomalies_idx, y=df_batch.loc[anomalies_idx, selected_sensor],
                               mode="markers", marker=dict(color="red", size=8), name="Anomaly")
    st.plotly_chart(fig_ts, use_container_width=True)
    st.download_button(f"Download Single-Batch ({batch_sel}, {selected_sensor})",
                       df_batch[[selected_sensor, "anomaly"]].reset_index().to_csv(index=False).encode("utf-8"),
                       file_name=f"batch_{batch_sel}_{selected_sensor}.csv")

    # rolling average (window from sidebar)
    roll_col = f"{selected_sensor}_roll"
    df_batch[roll_col] = df_batch[selected_sensor].rolling(rolling_window, min_periods=1).mean()
    fig_roll = px.line(df_batch, y=[selected_sensor, roll_col], title=f"Rolling ({rolling_window}) — {selected_sensor}")
    st.plotly_chart(fig_roll, use_container_width=True)
    st.download_button(f"Download Rolling ({batch_sel})", df_batch[[selected_sensor, roll_col]].reset_index().to_csv(index=False).encode("utf-8"),
                       file_name=f"batch_{batch_sel}_{selected_sensor}_rolling.csv")

    # ------------------------
    # Multi-batch multi-sensor comparison
    # ------------------------
    st.header("Multi-Batch / Multi-Sensor Comparison")
    batches_default = list(pdf["batch_id"].unique())[:3]
    selected_batches = st.multiselect("Select batches to compare", pdf["batch_id"].unique(), default=batches_default)
    selected_sensors = st.multiselect("Select sensors to compare", candidate_sensors, default=[selected_sensor])

    if selected_batches and selected_sensors:
        fig_multi = go.Figure()
        combined_records = []
        for b in selected_batches:
            df_sub = pdf[pdf["batch_id"] == b].set_index("timestamp").sort_index()
            for s in selected_sensors:
                if s in df_sub.columns:
                    fig_multi.add_trace(go.Scatter(x=df_sub.index, y=df_sub[s], mode="lines", name=f"{s} - {b}"))
                    tmp = df_sub[[s]].copy().reset_index()
                    tmp["batch_id"] = b
                    tmp["sensor"] = s
                    combined_records.append(tmp)
        fig_multi.update_layout(title="Multi-Batch Multi-Sensor Comparison", xaxis_title="Time", yaxis_title="Sensor Value")
        st.plotly_chart(fig_multi, use_container_width=True)
        if combined_records:
            combined_df = pd.concat(combined_records, ignore_index=True)
            st.download_button("Download Multi-batch sensor data", combined_df.to_csv(index=False).encode("utf-8"),
                               file_name="multi_batch_sensor_data.csv")

    # ------------------------
    # Additional visualizations
    # ------------------------
    st.header("Additional Visualizations (dynamic)")

    # Histogram / distribution
    fig_hist = px.histogram(pdf, x=selected_sensor, nbins=30, title=f"Distribution: {selected_sensor}")
    st.plotly_chart(fig_hist, use_container_width=True)
    st.download_button("Download distribution data", pdf[[selected_sensor]].to_csv(index=False).encode("utf-8"),
                       file_name=f"{selected_sensor}_distribution.csv")

    # Scatter vs another sensor (choose first other)
    other_sensors = [s for s in candidate_sensors if s != selected_sensor]
    if other_sensors:
        other = other_sensors[0]
        fig_scatter = px.scatter(pdf, x=selected_sensor, y=other, color="batch_id", title=f"{selected_sensor} vs {other}")
        st.plotly_chart(fig_scatter, use_container_width=True)
        st.download_button("Download scatter data", pdf[[selected_sensor, other, "batch_id"]].to_csv(index=False).encode("utf-8"),
                           file_name=f"{selected_sensor}_vs_{other}_scatter.csv")

    # Daily aggregated trend (uses agg_option)
    pdf["date"] = pd.to_datetime(pdf["timestamp"]).dt.date
    if agg_option == "mean":
        daily = pdf.groupby("date")[selected_sensor].mean().reset_index()
    elif agg_option == "min":
        daily = pdf.groupby("date")[selected_sensor].min().reset_index()
    elif agg_option == "max":
        daily = pdf.groupby("date")[selected_sensor].max().reset_index()
    else:
        daily = pdf.groupby("date")[selected_sensor].std().reset_index()
    fig_daily = px.line(daily, x="date", y=selected_sensor, title=f"Daily {agg_option} — {selected_sensor}")
    st.plotly_chart(fig_daily, use_container_width=True)
    st.download_button("Download daily aggregated", daily.to_csv(index=False).encode("utf-8"),
                       file_name=f"{selected_sensor}_daily_{agg_option}.csv")

    # Boxplot per batch
    fig_box = px.box(pdf, x="batch_id", y=selected_sensor, points="all", title=f"Boxplot per batch — {selected_sensor}")
    st.plotly_chart(fig_box, use_container_width=True)
    st.download_button("Download boxplot data", pdf[["batch_id", selected_sensor]].to_csv(index=False).encode("utf-8"),
                       file_name=f"{selected_sensor}_boxplot.csv")

    # Anomaly counts per batch
    anom_counts = pdf.groupby("batch_id")["anomaly"].sum().reset_index()
    fig_anom = px.bar(anom_counts, x="batch_id", y="anomaly", title="Anomaly counts per batch")
    st.plotly_chart(fig_anom, use_container_width=True)
    st.download_button("Download anomaly counts", anom_counts.to_csv(index=False).encode("utf-8"), file_name="anomaly_counts.csv")

    # Heatmap (batch vs time) for selected sensor
    try:
        df_pivot = pdf.pivot(index="timestamp", columns="batch_id", values=selected_sensor)
        max_cols = 50
        if df_pivot.shape[1] > max_cols:
            df_pivot = df_pivot.iloc[:, :max_cols]
        fig_heat = go.Figure(data=go.Heatmap(
            z=df_pivot.T.values,
            x=df_pivot.index,
            y=df_pivot.columns.astype(str),
            colorscale="Viridis"
        ))
        fig_heat.update_layout(title=f"{selected_sensor} Heatmap (batch vs time)", xaxis_title="Time", yaxis_title="Batch")
        st.plotly_chart(fig_heat, use_container_width=True)
        st.download_button("Download heatmap data", df_pivot.reset_index().to_csv(index=False).encode("utf-8"),
                           file_name=f"{selected_sensor}_heatmap.csv")
    except Exception as e:
        st.warning(f"Could not create pivot/heatmap: {e}")

    # Power BI export (full CSV written earlier)
    st.header("Export / Reporting")
    if st.session_state.user != "guest":
        if os.path.exists(csv_path):
            with open(csv_path, "rb") as f:
                st.download_button("Download CSV for Power BI", f, file_name="powerbi_export.csv", mime="text/csv")
        else:
            st.info("No Power BI CSV present.")
    else:
        st.info("Export only for registered users.")

    st.markdown("---")
    st.write("Generated on:", datetime.utcnow().isoformat())

else:
    st.info("Please login, register or continue as guest from the sidebar to use the dashboard.")
