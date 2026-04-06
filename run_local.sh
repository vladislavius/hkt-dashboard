#!/bin/bash
# Local GTT collector — runs on residential IP, bypasses Turnstile
# Called by launchd every 2 hours
set -e

cd "$(dirname "$0")"

# Load env from .env file if present
[ -f .env ] && export $(grep -v '^#' .env | xargs)

# Pull latest data from GitHub first
git pull --rebase origin main 2>/dev/null || true

# Run collector (Playwright will get real Turnstile token)
python3 collector.py

# Push updated data
git add data/dashboard.json data/accumulated_*.json
if ! git diff --staged --quiet; then
    git commit -m "auto: update flight data (local) $(date '+%Y-%m-%d %H:%M ICT')"
    git push origin main
fi
