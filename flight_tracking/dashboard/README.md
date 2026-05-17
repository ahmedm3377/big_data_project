# Dashboard

This step will provide a Streamlit dashboard for live or frequently refreshed flight analytics.

Main file to implement:

```text
flight_dashboard.py
```

Run from the project root:

```powershell
.\.venv\Scripts\streamlit.exe run flight_tracking\dashboard\flight_dashboard.py
```

The dashboard reads the same Parquet output directories registered as Hive external tables:

- `flight_tracking/output/country_summary_enriched`
- `flight_tracking/output/latest_aircraft`
- `flight_tracking/output/alerts`

The sidebar has a manual refresh button and an optional auto-refresh control for recorded demos.
