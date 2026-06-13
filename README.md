# Manus Webhook Dashboard Updater

A lightweight FastAPI service that automatically updates the Google Sheets Command Center dashboard whenever a Manus task completes.

## Architecture

- **Runtime**: Python 3 / FastAPI + Uvicorn
- **Host**: Hetzner server at 5.161.189.93
- **Port**: 8089
- **Process manager**: systemd (`manus-webhook.service`)
- **Logs**: `/var/log/manus-webhook.log`

## How It Works

1. Manus sends a `task_stopped` webhook event to `http://5.161.189.93:8089/webhook/manus`
2. The service checks `stop_reason == "finish"` and skips "Dashboard Update" tasks (loop prevention)
3. The task title is parsed to determine the project (Calamari, Carmuvr, KeZ, or DMI)
4. A new Manus task is spawned via `POST /v2/task.create` with instructions to update the relevant Google Sheets row

## Project Routing Rules

| Keywords in Task Title | Project Row |
|---|---|
| Calamari, MT5, trading | Calamari |
| Carmuvr, routing | Carmuvr |
| KeZ, DriveKeZ, Keyguard | KeZ |
| DMI, F2M, Turo | DMI |

## Endpoints

- `GET /health` — Health check
- `POST /webhook/manus` — Manus webhook receiver

## Deployment

```bash
# On the Hetzner server, as root:
cd /opt/webhook-dashboard-updater
bash deploy.sh
```

## Environment Variables

| Variable | Description |
|---|---|
| `MANUS_API_KEY` | Manus API key for spawning dashboard update tasks |

## Testing

```bash
# Health check
curl http://5.161.189.93:8089/health

# Fake webhook payload (KeZ project)
curl -X POST http://5.161.189.93:8089/webhook/manus \
  -H "Content-Type: application/json" \
  -d '{
    "event_id": "task_stopped_test123",
    "event_type": "task_stopped",
    "task_detail": {
      "task_id": "test123",
      "task_title": "KeZ - Test task completion",
      "task_url": "https://manus.im/app/test123",
      "message": "Test task completed successfully.",
      "stop_reason": "finish"
    }
  }'
```

## Registering the Webhook with Manus

```bash
curl -X POST https://api.manus.ai/v2/webhook.create \
  -H "x-manus-api-key: $MANUS_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"url": "http://5.161.189.93:8089/webhook/manus"}'
```
