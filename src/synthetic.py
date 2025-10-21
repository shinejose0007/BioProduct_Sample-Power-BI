
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
import uuid

def generate_run(batch_id=None, start_time=None, n_points=1440, freq_seconds=60, seed=None):
    """Generate synthetic telemetry for a single batch/run.
    n_points = number of time-series points (default 1440 -> 24h @ 1min)
    Returns DataFrame with timestamp, batch_id, sensor readings, qc_pass, reagent_lot
    """
    rng = np.random.RandomState(seed)
    if start_time is None:
        start_time = datetime.utcnow()
    if batch_id is None:
        batch_id = str(uuid.uuid4())[:8]

    timestamps = [start_time + timedelta(seconds=i*freq_seconds) for i in range(n_points)]
    # base signals
    temp = 37 + 0.5 * np.sin(np.linspace(0, 6.28, n_points)) + rng.normal(0, 0.1, n_points)
    pH = 7.2 + 0.05 * np.sin(np.linspace(0, 12.56, n_points)) + rng.normal(0, 0.02, n_points)
    conductivity = 5 + 0.2 * np.cos(np.linspace(0, 3.14, n_points)) + rng.normal(0, 0.05, n_points)
    # occasional spikes / anomalies
    anomalies = rng.rand(n_points) < 0.002
    temp[anomalies] += rng.normal(2.5, 0.5, anomalies.sum())
    # QC pass/fail simulated by simple rule (fail if temp out of range or pH out of range)
    qc_pass = (~((temp < 36.5) | (temp > 38.5) | (pH < 6.9) | (pH > 7.5))).astype(int)
    reagent_lot = "LOT-" + str(rng.randint(1000, 1010))

    df = pd.DataFrame({
        "timestamp": timestamps,
        "batch_id": batch_id,
        "temp_c": temp,
        "pH": pH,
        "conductivity": conductivity,
        "qc_pass": qc_pass,
        "reagent_lot": reagent_lot
    })
    # Add metadata
    df["ingest_time"] = datetime.utcnow()
    return df

def generate_multiple_runs(n_runs=5, points_per_run=1440, seed=42):
    dfs = []
    rng = np.random.RandomState(seed)
    base = datetime.utcnow() - pd.Timedelta(days=2)
    for i in range(n_runs):
        st = base + pd.Timedelta(hours=i*6)
        dfs.append(generate_run(batch_id=f"BATCH_{100+i}", start_time=st, n_points=points_per_run, seed=rng.randint(0,10000)))
    return pd.concat(dfs, ignore_index=True)
