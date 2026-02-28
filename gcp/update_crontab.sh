#!/bin/bash
# DSE Daily Analysis — Add Cron Entry
# Adds the analysis cron job alongside existing audit crons.
set -euo pipefail

MARKER="# DSE Daily Analysis"
CRON_LINE="0 9 * * 0-4 /home/shariarsourav/dse_analysis/gcp/run_analysis.sh >> /home/shariarsourav/dse_analysis/logs/cron.log 2>&1"

# Check if already present
if crontab -l 2>/dev/null | grep -q "DSE Daily Analysis"; then
    echo "DSE cron entry already exists. Current crontab:"
    crontab -l
    exit 0
fi

# Append to existing crontab
echo "Adding DSE analysis cron job..."
(
    crontab -l 2>/dev/null
    echo ""
    echo "${MARKER} — 09:00 UTC (15:00 BST) Sun-Thu"
    echo "${CRON_LINE}"
) | crontab -

echo "Cron entry added. Current crontab:"
crontab -l
echo ""
echo "Next run: 09:00 UTC on the next Sun-Thu"
