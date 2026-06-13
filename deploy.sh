#!/bin/bash
set -e

echo "Deploying Manus Webhook Service..."

# Ensure directory exists
mkdir -p /opt/webhook-dashboard-updater
cp main.py /opt/webhook-dashboard-updater/
cp requirements.txt /opt/webhook-dashboard-updater/

# Install dependencies
pip3 install -r /opt/webhook-dashboard-updater/requirements.txt

# Create log file if it doesn't exist
touch /var/log/manus-webhook.log
chmod 644 /var/log/manus-webhook.log

# Copy service file
cp manus-webhook.service /etc/systemd/system/

# Reload systemd and start service
systemctl daemon-reload
systemctl enable manus-webhook
systemctl restart manus-webhook

echo "Deployment complete. Service status:"
systemctl status manus-webhook --no-pager
