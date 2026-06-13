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

    if payload.event_type != "task_stopped":
        logger.info(f"Ignoring event type: {payload.event_type}")
        return {"status": "ignored", "reason": "Not a task_stopped event"}

    if payload.task_detail.stop_reason != "finish":
        logger.info(f"Ignoring task stopped with reason: {payload.task_detail.stop_reason}")
        return {"status": "ignored", "reason": "Task did not finish"}

    task_title = payload.task_detail.task_title

    # 1. Skip if the task title contains "Dashboard Update" or "Dashboard Fix"
    if "Dashboard Update" in task_title or "Dashboard Fix" in task_title:
        logger.info(f"Skipping dashboard update/fix task to avoid infinite loop: {task_title}")
        return {"status": "ignored", "reason": "Dashboard update or fix task"}

    # 2. Skip if the task title contains "Audit VM" or "Extract" (infrastructure tasks)
    if "Audit VM" in task_title or "Extract" in task_title:
        logger.info(f"Skipping infrastructure task: {task_title}")
        return {"status": "ignored", "reason": "Infrastructure task"}

    message_content = payload.task_detail.message or ""
    
    # 3. Add a minimum message length check
    if len(message_content) < 20:
        logger.info(f"Skipping task due to short message length ({len(message_content)} chars)")
        return {"status": "ignored", "reason": "Message too short"}

    # 4. Parse the task title to determine which project it belongs to
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
        logger.info(f"Unknown project for task: {task_title}")
        return {"status": "ignored", "reason": "Unknown project"}

    logger.info(f"Matched project: {project_name} for task: {task_title}")

    # 5. Spawn a Manus task to update the dashboard
    # Use verbatim task summary (task_detail.message)
    current_focus = message_content.replace('\n', ' ')

    instruction = f"""
Update the Google Sheets TV dashboard (Command Center tab) at spreadsheet ID 1kDBFSnfpTUWKW7bQcPTGsiQ7uZQO1KU2lCdgvm3KoVI.
Use GCP_SERVICE_ACCOUNT_JSON env var to authenticate.
Update {project_name} row with:
- Current Focus: {current_focus}
- Next Action: Review completed task results
- Waiting On: Steward review
Update timestamp in row 18 col A.
"""

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
