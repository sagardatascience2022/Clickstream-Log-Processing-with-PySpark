# PowerShell helper to run locally (assumes venv activated)
param(
    [string]$input = "data/sample_logs.json",
    [string]$out_sessions = "out/sessions",
    [string]$out_top = "out/top_pages",
    [int]$gap = 30
)

python src/sessionize.py --input $input --out_sessions $out_sessions --out_top $out_top --session_gap_minutes $gap
