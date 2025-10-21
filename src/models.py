
import numpy as np
import pandas as pd
from sklearn.ensemble import IsolationForest

def train_anomaly_detector(df, features=['temp_c','pH','conductivity']):
    X = df[features].values
    clf = IsolationForest(contamination=0.01, random_state=42)
    clf.fit(X)
    return clf

def detect_anomalies(clf, df, features=['temp_c','pH','conductivity']):
    X = df[features].values
    preds = clf.predict(X)  # -1 anomaly, 1 normal
    df2 = df.copy()
    df2['anomaly'] = (preds == -1).astype(int)
    return df2
