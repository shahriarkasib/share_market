#!/bin/bash
# DSE Daily Analysis — Cron Entry Point
# Runs at 09:00 UTC (15:00 BST) Sun-Thu after DSE market close.
# Auto-deploys latest code via git pull on each run.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
BACKEND_DIR="${SCRIPT_DIR}/backend"
VENV="${SCRIPT_DIR}/venv/bin/python"
TIMESTAMP=$(date -u +%Y%m%d_%H%M%S)
LOG_DIR="${SCRIPT_DIR}/logs"
LOG_FILE="${LOG_DIR}/analysis_${TIMESTAMP}.log"

mkdir -p "${LOG_DIR}"

echo "=== DSE Daily Analysis Started: $(date -u) ===" | tee "${LOG_FILE}"

# Auto-deploy: pull latest code
echo "Pulling latest code..." | tee -a "${LOG_FILE}"
git -C "${SCRIPT_DIR}" pull origin main 2>&1 | tee -a "${LOG_FILE}"

# Update dependencies if requirements changed
echo "Checking dependencies..." | tee -a "${LOG_FILE}"
"${SCRIPT_DIR}/venv/bin/pip" install -r "${BACKEND_DIR}/requirements.txt" openpyxl -q 2>&1 | tee -a "${LOG_FILE}"

# Run the analysis
echo "Running daily analysis..." | tee -a "${LOG_FILE}"
cd "${BACKEND_DIR}"
"${VENV}" -c "
import sys
sys.path.insert(0, '.')
from analysis.daily_report import run_daily_analysis
run_daily_analysis()
" 2>&1 | tee -a "${LOG_FILE}"

EXIT_CODE=$?

if [ ${EXIT_CODE} -ne 0 ]; then
    echo "ERROR: Algo analysis exited with code ${EXIT_CODE}" | tee -a "${LOG_FILE}"
else
    echo "Algo analysis completed successfully" | tee -a "${LOG_FILE}"
fi

# Source bashrc for Claude CLI auth token (CLAUDE_CODE_OAUTH_TOKEN)
# Also provides email env vars: EMAIL_SENDER, EMAIL_PASSWORD, EMAIL_RECIPIENT
if [ -f "${HOME}/.bashrc" ]; then
    source "${HOME}/.bashrc" 2>/dev/null || true
fi

# Run LLM analysis (3-stage: LLM → Judge → Snapshot predictions)
echo "" | tee -a "${LOG_FILE}"
echo "=== LLM Analysis Pipeline ===" | tee -a "${LOG_FILE}"
"${VENV}" -c "
import sys
sys.path.insert(0, '.')
from scripts.llm_daily_analyzer import run
run()
" 2>&1 | tee -a "${LOG_FILE}" || echo "WARNING: LLM analysis failed (non-fatal)" | tee -a "${LOG_FILE}"

# Verify past predictions
echo "" | tee -a "${LOG_FILE}"
echo "=== Prediction Verification ===" | tee -a "${LOG_FILE}"
"${VENV}" -c "
import sys
sys.path.insert(0, '.')
from scripts.verify_predictions import run
run()
" 2>&1 | tee -a "${LOG_FILE}" || echo "WARNING: Prediction verification failed (non-fatal)" | tee -a "${LOG_FILE}"

echo "" | tee -a "${LOG_FILE}"
echo "=== All Done: $(date -u) ===" | tee -a "${LOG_FILE}"

# Cleanup logs older than 30 days
find "${LOG_DIR}" -name "analysis_*.log" -mtime +30 -delete 2>/dev/null || true

exit ${EXIT_CODE}
