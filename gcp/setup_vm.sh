#!/bin/bash
# DSE Daily Analysis — One-time VM Setup
# Run this once on data-audit-vm to set up the analysis environment.
set -euo pipefail

DSE_DIR="/home/shariarsourav/dse_analysis"

echo "=== DSE Analysis VM Setup ==="
echo "Target: ${DSE_DIR}"

# 1. Clone repo (skip if already cloned)
if [ -d "${DSE_DIR}/.git" ]; then
    echo "Repo already cloned, pulling latest..."
    git -C "${DSE_DIR}" pull origin main
else
    echo "Cloning share_market repo..."
    git clone https://github.com/shahriarkasib/share_market.git "${DSE_DIR}"
fi

# 2. Install build dependencies (scipy needs gcc)
echo "Installing system dependencies..."
sudo apt-get update -qq
sudo apt-get install -y -qq python3-dev gcc g++ libpq-dev

# 3. Create Python venv
echo "Creating Python venv..."
python3 -m venv "${DSE_DIR}/venv"
"${DSE_DIR}/venv/bin/pip" install --upgrade pip -q

# 4. Install Python packages
echo "Installing Python dependencies..."
cd "${DSE_DIR}/backend"
"${DSE_DIR}/venv/bin/pip" install -r requirements.txt openpyxl -q

# 5. Create logs directory
mkdir -p "${DSE_DIR}/logs"

# 6. Smoke test — verify imports work
echo "Running smoke test..."
"${DSE_DIR}/venv/bin/python" -c "
import sys
sys.path.insert(0, '.')
from analysis.daily_report import run_daily_analysis
from analysis.excel_generator import generate_analysis_excel
from database import init_database
print('All imports OK')
"

echo ""
echo "=== Setup Complete ==="
echo "Next steps:"
echo "  1. Run: ${DSE_DIR}/gcp/update_crontab.sh"
echo "  2. Test: ${DSE_DIR}/gcp/run_analysis.sh"
