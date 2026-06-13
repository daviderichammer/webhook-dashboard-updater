import os
import logging
import requests
from fastapi import FastAPI, Request, HTTPException
from pydantic import BaseModel
from typing import Optional, List, Dict, Any

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("/var/log/manus-webhook.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("manus_webhook")

app = FastAPI(title="Manus Webhook Receiver")

MANUS_API_KEY = os.environ.get("MANUS_API_KEY")
if not MANUS_API_KEY:
    logger.warning("MANUS_API_KEY environment variable is not set. API calls to Manus will fail.")

MANUS_API_BASE = "https://api.manus.ai/v2"

# -----------------------------------------------------------------------
# LOOP PREVENTION: Any task whose title contains one of these strings
# will be silently ignored — no dashboard update will be spawned.
# This list must cover ALL titles that this service itself generates,
# plus any "fix" or "restore" tasks that should never trigger updates.
# -----------------------------------------------------------------------
BLOCKED_TITLE_SUBSTRINGS = [
    "Dashboard Update",
    "Dashboard Fix",
    "Dashboard Restore",
    "Restore KeZ",
    "Restore DMI",
    "Restore Calamari",
    "Restore Carmuvr",
    "Audit VM",
    "Extract",
]

class TaskDetail(BaseModel):
    task_id: str
    task_title: str
    task_url: str
    message: Optional[str] = None
    stop_reason: Optional[str] = None
    attachments: Optional[List[Dict[str, Any]]] = None
    structured_output: Optional[Dict[str, Any]] = None

class WebhookPayload(BaseModel):
    event_id: str
    event_type: str
    task_detail: TaskDetail

@app.get("/health")
def health_check():
    return {"status": "healthy"}

@app.post("/webhook/manus")
async def manus_webhook(payload: WebhookPayload):
    logger.info(f"Received webhook event: {payload.event_type} for task: {payload.task_detail.task_id}")

    # GATE 1: Only act on task_stopped with stop_reason=finish
    if payload.event_type != "task_stopped":
        logger.info(f"Ignoring event type: {payload.event_type}")
        return {"status": "ignored", "reason": "Not a task_stopped event"}

    if payload.task_detail.stop_reason != "finish":
        logger.info(f"Ignoring task stopped with reason: {payload.task_detail.stop_reason}")
        return {"status": "ignored", "reason": "Task did not finish"}

    task_title = payload.task_detail.task_title

    # GATE 2: Block any title that contains a blocked substring (loop prevention)
    for blocked in BLOCKED_TITLE_SUBSTRINGS:
        if blocked.lower() in task_title.lower():
            logger.info(f"Blocked title match '{blocked}' — ignoring task: {task_title}")
            return {"status": "ignored", "reason": f"Blocked title substring: {blocked}"}

    # GATE 3: Minimum message length — must have real content
    message_content = payload.task_detail.message or ""
    if len(message_content) < 20:
        logger.info(f"Skipping task due to short message length ({len(message_content)} chars): {task_title}")
        return {"status": "ignored", "reason": "Message too short"}

    # GATE 4: Match to a known project via title keywords
    title_lower = task_title.lower()
    project_name = None

    if any(keyword in title_lower for keyword in ["calamari", "mt5", "trading"]):
        project_name = "Calamari"
    elif any(keyword in title_lower for keyword in ["carmuvr", "routing"]):
        project_name = "Carmuvr"
    elif any(keyword in title_lower for keyword in ["kez", "drivekez", "keyguard"]):
        project_name = "KeZ"
    elif any(keyword in title_lower for keyword in ["dmi", "f2m", "turo"]):
        project_name = "DMI"
    else:
        logger.info(f"No project match for task: {task_title}")
        return {"status": "ignored", "reason": "Unknown project"}

    logger.info(f"Matched project: {project_name} for task: {task_title}")

    # Use VERBATIM task summary — never infer or hallucinate content
    current_focus = message_content.replace('\n', ' ').strip()

    instruction = (
        f"Update the Google Sheets TV dashboard (Command Center tab) at spreadsheet ID "
        f"1kDBFSnfpTUWKW7bQcPTGsiQ7uZQO1KU2lCdgvm3KoVI.\n"
        f"Use GCP_SERVICE_ACCOUNT_JSON env var to authenticate.\n"
        f"Update {project_name} row with:\n"
        f"- Current Focus: {current_focus}\n"
        f"- Next Action: Review completed task results\n"
        f"- Waiting On: Steward review\n"
        f"Update timestamp in row 18 col A."
    )

    headers = {
        "x-manus-api-key": MANUS_API_KEY,
        "Content-Type": "application/json"
    }

    new_task_title = f"Dashboard Update: {project_name} - {task_title[:50]}"
    payload_data = {
        "title": new_task_title,
        "project_id": "GmzyukShqfLTa8DYphnfAd",
        "message": {
            "content": instruction,
            "connectors": ["1fde121a-88ef-4f66-88a1-2862739d1c79"]
        }
    }

    logger.info(f"Spawning Manus task: {new_task_title}")
    try:
        response = requests.post(
            f"{MANUS_API_BASE}/task.create",
            headers=headers,
            json=payload_data,
            timeout=30
        )
        response.raise_for_status()
        result = response.json()
        logger.info(f"Successfully spawned dashboard update task: {result.get('task_id', 'unknown')}")
        return {"status": "success", "spawned_task": result}
    except Exception as e:
        logger.error(f"Failed to spawn dashboard update task: {str(e)}")
        if isinstance(e, requests.exceptions.HTTPError) and e.response is not None:
            logger.error(f"Response: {e.response.text}")
        raise HTTPException(status_code=500, detail=f"Failed to spawn Manus task: {str(e)}")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8089)
