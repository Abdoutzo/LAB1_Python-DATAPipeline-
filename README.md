# Lab 1 — Python Data Pipeline

This repository contains a small, end‑to‑end data pipeline built in Python.  
The goal is to collect raw data from Google Play (AI note‑taking apps), clean it, and produce analytics‑ready outputs plus a lightweight dashboard.

The pipeline follows the lab steps:
1) ingestion (raw data),
2) transformation (cleaned tables),
3) serving layer (KPIs),
4) dashboard (consumer view).


## Project structure

```
data/
  raw/          # raw JSONL files from the source
  processed/    # cleaned CSVs + KPI outputs + dashboard image
src/
  ingest.py     # data acquisition from Google Play
  transform.py  # cleaning + structuring into tables
  serve.py      # KPI generation (app-level + daily)
  dashboard.py  # simple visualization
```


## What the pipeline produces

Raw data (as‑is):
- `data/raw/apps.jsonl`
- `data/raw/reviews.jsonl`

Cleaned tables:
- `data/processed/apps.csv`
- `data/processed/reviews.csv`

Serving layer outputs:
- `data/processed/app_kpis.csv`
- `data/processed/daily_kpis.csv`

Dashboard:
- `data/processed/dashboard.png`
  <img width="839" height="728" alt="image" src="https://github.com/user-attachments/assets/14a5567f-16f1-463b-a69e-e55428b01b17" />



## How to run

Create and activate a virtual environment, then:

```powershell
python -m pip install google-play-scraper matplotlib
```

Run the pipeline step by step:

```powershell
python src/ingest.py
python src/transform.py
python src/serve.py
python src/dashboard.py
```


## Notes

- The raw files are kept untouched to preserve lineage and reproducibility.
- The cleaned tables are designed to be joinable (`appId` / `app_id`) and analytics‑ready.
- The KPI layer answers the lab questions: best/worst apps, trend over time, and review volume.

## Feedback
- You might want to look at pagination for your reviews so you can get more data.
- If you do so, writing with append in the loop is always better to prevent data loss if code crashes
- Computing the KPIs is a bit too complicated, you can use pandas aggregations, which will optimize the code significantly
- Please add screenshots of your dashboard to the readme file
