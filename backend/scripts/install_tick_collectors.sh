#!/usr/bin/env bash
# Install tick collectors as systemd services on the GCP VM.
set -e

VENV=/home/shariarsourav/dse_analysis/venv

"$VENV/bin/pip" install --quiet ib_insync requests beautifulsoup4 lxml psycopg2-binary

sudo tee /etc/systemd/system/dse-tape.service > /dev/null <<'EOF'
[Unit]
Description=DSE Tape Scraper (LankaBD time-and-sales)
After=network.target postgresql.service

[Service]
Type=simple
User=shariarsourav
WorkingDirectory=/home/shariarsourav/dse_analysis/backend
Environment=DATABASE_URL=postgresql://dse:dse_trading_2026@localhost/dse_trading
Environment=DSE_TAPE_POLL_SEC=20
ExecStart=/home/shariarsourav/dse_analysis/venv/bin/python3 -m data.dse_tape_scraper
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
EOF

sudo tee /etc/systemd/system/nasdaq-ticks.service > /dev/null <<'EOF'
[Unit]
Description=NASDAQ IBKR Tick Collector
After=network.target postgresql.service

[Service]
Type=simple
User=shariarsourav
WorkingDirectory=/home/shariarsourav/dse_analysis/backend
Environment=DATABASE_URL=postgresql://dse:dse_trading_2026@localhost/dse_trading
Environment=IBKR_HOST=127.0.0.1
Environment=IBKR_PORT=4002
Environment=IBKR_CLIENT_ID=23
Environment=WATCHLIST=NVDA,AAPL,MSFT,GOOGL,AMZN,META,TSLA,AMD,AVGO,TSM,ORCL,CRM
ExecStart=/home/shariarsourav/dse_analysis/venv/bin/python3 tick_collector.py
Restart=always
RestartSec=15

[Install]
WantedBy=multi-user.target
EOF

sudo systemctl daemon-reload
sudo systemctl enable dse-tape.service
sudo systemctl restart dse-tape.service
sleep 3
sudo systemctl status dse-tape.service --no-pager -l | head -10
