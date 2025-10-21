
Power BI report package
=======================

Files included:
- powerbi_export.csv            : Data file (already included)
- DAX_measures.txt               : DAX measures to paste as new measures
- PowerQuery_M.txt               : Power Query (M) script to use in Advanced Editor (Get Data -> Text/CSV -> Transform Data -> Advanced Editor)
- theme.json                     : Power BI theme to import (View -> Themes -> Browse for themes)
- kpi_summary.png                : Preview KPI image
- timeseries_batch.png           : Preview time-series plot for first batch
- batch_table.png                : Snapshot of per-batch QC table
- README_steps.txt               : This file

Step-by-step to build .pbix in Power BI Desktop:
1) Open Power BI Desktop.
2) Get Data -> Text/CSV -> select 'powerbi_export.csv' from this folder.
3) Click 'Transform Data' to open Power Query. In Home -> Advanced Editor, replace content with the contents of PowerQuery_M.txt and click Done. Close & Apply.
4) In Report view, import theme.json: View -> Themes -> Browse for themes -> select theme.json.
5) Create visuals:
   - KPI cards: create measures by copying contents of DAX_measures.txt and creating New Measure for each.
   - Line chart (Temperature): Axis = timestamp, Values = temp_c (use slicer for batch_id).
   - Line chart (pH & Conductivity): Axis = timestamp, Values = pH, conductivity.
   - Table: fields batch_id, rows (use 'Group By' in Power Query or create measures), qc_failures.
   - Trend of Failure Rate: use FailureRate measure and a date axis.
6) Add slicers for batch_id and reagent_lot.
7) Apply conditional formatting on Failure Rate visuals (e.g., > 0.5%).

Notes:
- The sample CSV contains 4320 rows, 3 batches, 373 QC failures (~8.63%).
