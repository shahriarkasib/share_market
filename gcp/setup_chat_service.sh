#!/bin/bash
# Setup chat service as a systemd service on GCP VM.
# Run once: bash gcp/setup_chat_service.sh
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
USER=$(whoami)
PYTHON="${SCRIPT_DIR}/venv/bin/python3"

echo "=== Setting up DSE Chat Service ==="
echo "Project dir: ${SCRIPT_DIR}"
echo "User: ${USER}"
echo "Python: ${PYTHON}"

# Extract OAuth token from bashrc
OAUTH_TOKEN=$(grep -oP 'CLAUDE_CODE_OAUTH_TOKEN="\K[^"]+' ~/.bashrc 2>/dev/null || true)
if [ -z "${OAUTH_TOKEN}" ]; then
    echo "WARNING: CLAUDE_CODE_OAUTH_TOKEN not found in ~/.bashrc"
fi

# Create systemd service
sudo tee /etc/systemd/system/dse-chat.service > /dev/null <<UNIT
[Unit]
Description=DSE Trading Chat Service (Claude CLI)
After=network.target

[Service]
Type=simple
User=${USER}
WorkingDirectory=${SCRIPT_DIR}
Environment=CLAUDE_CODE_OAUTH_TOKEN=${OAUTH_TOKEN}
Environment=CLAUDE_MODEL=sonnet
Environment=CHAT_PORT=8787
Environment=PATH=/usr/local/bin:/usr/bin:/bin:/usr/local/lib/nodejs/node-v20.11.1-linux-x64/bin
ExecStart=${PYTHON} ${SCRIPT_DIR}/gcp/chat_service.py
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
UNIT

# Enable and start
sudo systemctl daemon-reload
sudo systemctl enable dse-chat
sudo systemctl restart dse-chat

echo ""
echo "=== Chat service started ==="
sudo systemctl status dse-chat --no-pager -l

echo ""
echo "Test: curl http://localhost:8787/health"

# Open firewall port 8787
echo ""
echo "=== Opening firewall port 8787 ==="
gcloud compute firewall-rules create allow-chat-8787 \
    --direction=INGRESS \
    --priority=1000 \
    --network=default \
    --action=ALLOW \
    --rules=tcp:8787 \
    --source-ranges=0.0.0.0/0 \
    --description="Allow DSE chat service" \
    2>/dev/null || echo "Firewall rule already exists"

echo ""
echo "=== DONE ==="
echo "Chat service URL: http://$(curl -s ifconfig.me):8787"
echo ""
echo "Set this on Render as env var:"
echo "  GCP_CHAT_URL=http://$(curl -s ifconfig.me):8787"
