# Bioproduction-data-platform
Proof-of-concept data platform for bioproduction telemetry. Includes:
- synthetic data generation
- ETL pipelines (extract, transform, load into Parquet/CSV)
- data quality checks and simple rules
- anomaly detection model (IsolationForest)
- Streamlit dashboard for monitoring, QA and reporting
- Export of Power BI-ready CSVs and instructions to build Power BI dashboards

Run locally:
```bash
pip install -r requirements.txt
streamlit run app.py
```

Or using Docker (simple):
```bash
docker build -t bioprod-poc:latest .
docker run -p 8501:8501 bioprod-poc:latest
```

Project structure:
```
bioproduction-data-platform-poc/
├─ app.py                  # Streamlit app (single-file entrypoint)
├─ requirements.txt
├─ Dockerfile
├─ src/
│  ├─ synthetic.py         # synthetic data generator
│  ├─ etl.py               # extract-transform-load
│  ├─ quality.py           # data quality checks
│  └─ models.py            # anomaly detector
├─ data/                   # generated sample data and exported CSVs
└─ docs/
   └─ power_bi_Report_Package-Folder

