# Clickstream Log Processing with PySpark

This project demonstrates sessionization of web server logs (JSON) using PySpark.

Files:
- `src/sessionize.py` - Main PySpark script. Reads JSON logs, sessionizes by user, outputs session-level stats and top pages.
- `data/sample_logs.json` - Example input (JSON lines).
- `requirements.txt` - Python deps.

Quick run (PowerShell):

```powershell
# create a venv, install dependencies
python -m venv .venv; .\.venv\Scripts\Activate.ps1; pip install -r requirements.txt

# run locally reading the sample file and output to local folders
python src/sessionize.py --input data/sample_logs.json --out_sessions out/sessions --out_top out/top_pages --session_gap_minutes 30
```

Outputs:
- `out/sessions` (parquet) — one row per session: user_id, session_id, start_ts, end_ts, session_duration_seconds, page_count, pages_visited
- `out/top_pages` (parquet) — page_id and view counts, ordered by views

Notes:
- The script uses window functions (`lag`, cumulative `sum`) to assign sessions.
- Adjust `--session_gap_minutes` to define session inactivity threshold.
