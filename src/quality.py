
import pandas as pd

def check_nulls(df, column):
    return int(df[column].isnull().sum())

def check_range(df, column, min_val, max_val):
    s = df[column]
    return int((~s.between(min_val, max_val)).sum())

def compute_quality_metrics(df):
    metrics = {}
    metrics['rows'] = int(len(df))
    metrics['null_temp'] = check_nulls(df, 'temp_c')
    metrics['temp_out_of_range'] = check_range(df, 'temp_c', 36.5, 38.5)
    metrics['pH_out_of_range'] = check_range(df, 'pH', 6.9, 7.5)
    metrics['qc_failures'] = int((df['qc_pass'] == 0).sum())
    # per-batch failure rates (dict)
    batch_rates = df.groupby('batch_id')['qc_pass'].agg(lambda s: 1 - s.mean()).to_dict()
    metrics['batch_qc_rate'] = {str(k): float(v) for k,v in batch_rates.items()}
    return metrics
