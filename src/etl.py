
import pandas as pd
import os
from datetime import datetime

def save_parquet(df, out_dir='data/parquet', partition_cols=None):
    os.makedirs(out_dir, exist_ok=True)
    try:
        # simple partition by date if requested
        if partition_cols and 'date' in partition_cols and 'timestamp' in df.columns:
            df = df.copy()
            df['date'] = pd.to_datetime(df['timestamp']).dt.date.astype(str)
            for d, group in df.groupby('date'):
                path = os.path.join(out_dir, f"date={d}")
                os.makedirs(path, exist_ok=True)
                group.to_parquet(os.path.join(path, f"data_{d}.parquet"), index=False)
        else:
            df.to_parquet(os.path.join(out_dir, f"data_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.parquet"), index=False)
        return True
    except Exception as e:
        # fallback: write CSV instead and return False
        try:
            fallback = os.path.join(out_dir, f"data_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.csv")
            df.to_csv(fallback, index=False)
            return False
        except Exception as e2:
            raise

def save_csv_for_powerbi(df, out_file='data/powerbi_export.csv'):
    os.makedirs(os.path.dirname(out_file), exist_ok=True)
    df.to_csv(out_file, index=False)
    return out_file
