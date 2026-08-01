"""
Manus Webhook Dashboard Updater + Steward Forwarder
====================================================
Listens on port 8089 for Manus webhook events.

Key behaviours
--------------
1. Accepts ANY JSON body (raw dict) — avoids 422 errors from verification pings
   or future schema additions.
2. Verifies RSA-SHA256 signatures using the Manus public key (cached 1 h).
   NOTE: The Manus docs example has a double-hashing bug. The correct approach
   is to pass `signed_content` directly to key.verify() with hashes.SHA256(),
   which performs a single SHA256 hash internally (standard RSA-SHA256).
3. On paused task_stopped events: forwards a compact summary to the Steward
   agent via task.sendMessage. Finished events stay on the cooldown-controlled path.
4. On task_stopped / stop_reason=finish: spawns a FULL dashboard-update task
   via the Manus API — identical to the hourly scheduled run. The spawned task
   reads all project go-forward plans from GitHub and rewrites the entire sheet
   with accurate, plan-derived content. This handler does NOT write to the
   sheet directly.
5. 600-second cooldown/deduplication: if multiple tasks finish within 600 s (10 min),
   only ONE dashboard update task is spawned (prevents cascade loops).
6. Loop prevention: Dashboard/Refresh completions are excluded before any
   downstream action; other blocked titles are excluded from dashboard spawning.
7. Structured logging to /var/log/manus-webhook.log via systemd.
8. Credits balance: fetches available credits from usage.availableCredits on
   EVERY task_stopped event (both finished and paused) and includes the balance
   in the Steward notification and the dashboard refresh prompt. The credit
   balance is formatted as "Credits remaining: X / Y" where X is total_credits
   and Y is pro_monthly_credits. Also exposes a /credits endpoint for
   on-demand balance queries.
"""
import base64
import fcntl
import hashlib
import json
import logging
import os
import time
from datetime import datetime, timezone
from typing import Any, Dict, Optional

import requests
from fastapi import FastAPI, Request, Response

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("manus-webhook")

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
MANUS_API_KEY = os.environ.get("MANUS_API_KEY", "")
if not MANUS_API_KEY:
    logger.warning("MANUS_API_KEY env variable is not set. API calls will fail.")

MANUS_API_BASE = "https://api.manus.ai/v2"

# Steward agent task_id — used for task.sendMessage forwarding
STEWARD_TASK_ID = "PfG6WuVPozpFfyNhBDXuGh"

# Google Sheets dashboard project_id (used when spawning the full update task)
DASHBOARD_PROJECT_ID = "KyouqUkPZoyWqcg9WyCFgF"

# Connector for GCP access (Google Sheets) — MUST be included in spawned task
GCP_CONNECTOR_ID = "1fde121a-88ef-4f66-88a1-2862739d1c79"

# Manus API key for the spawned dashboard task (same key used by this service)
DASHBOARD_MANUS_API_KEY = os.environ.get("DASHBOARD_MANUS_API_KEY", MANUS_API_KEY)

# GitHub PAT for reading go-forward plans
GITHUB_PAT = os.environ.get("GITHUB_PAT", "")

# Case-insensitive prefixes used by this service and the scheduled dashboard
# refresh task. A stopped refresh task must never trigger another refresh.
DASHBOARD_REFRESH_TITLE_PREFIXES = (
    "dashboard full refresh",
    "refresh google sheets tv dashboard command center",
)

# Titles containing any of these words (lowercased) are silently ignored
# for dashboard spawning (loop prevention).
BLOCKED_TITLE_WORDS = [
    "dashboard",
    "refresh",
    "audit vm",
    "extract",
    "restore",
    "steward",
]

# Cooldown: only one dashboard update task per 600 seconds (10 minutes)
DASHBOARD_COOLDOWN_SECONDS = 600
DASHBOARD_COOLDOWN_FILE = "/tmp/last_dashboard_refresh"

# ---------------------------------------------------------------------------
# The full dashboard update instruction — identical to the hourly scheduled run
# ---------------------------------------------------------------------------
FULL_DASHBOARD_INSTRUCTION = """Update the Google Sheets TV Dashboard (Command Center tab) at https://docs.google.com/spreadsheets/d/1kDBFSnfpTUWKW7bQcPTGsiQ7uZQO1KU2lCdgvm3KoVI/edit

REQUIRED FORMAT — NEVER CHANGE THIS:
- Column A: Project Name
- Column B: Subprojects
- Column C: Current (what is actively being worked on RIGHT NOW)
- Column D: Next (the next task per the go-forward plan)
- Column E: Waiting On (what is blocking progress — MOST IMPORTANT)

REQUIRED ORDER:
- Projects MUST be in LEXICOGRAPHIC (alphabetical) order. Always. Never reorder.
- Row 1 is the header row. Always add "Last Updated" timestamp at the bottom.

CONTENT QUALITY REQUIREMENTS — MANDATORY:
1. Fetch the full list of active projects from the Manus API (GET /v2/project.list with x-manus-api-key header). Ignore all projects whose name starts with "ZZ".
2. For EACH project, fetch its go-forward plan from GitHub: https://raw.githubusercontent.com/daviderichammer/the-project-project/main/{folder-name}/go-forward-plan.md (use Authorization: token """ + GITHUB_PAT + """). Convert project name to lowercase-with-hyphens for folder name.
3. READ THE FULL PLAN CONTENT. Extract SPECIFIC, MEANINGFUL information:
   - Column B (Subprojects): List the actual NAMED subprojects/workstreams from the plan (e.g., "Shell Redesign, USPTO Filing, v2 Prototype" — NOT generic task titles)
   - Column C (Current): What task is actively running or was most recently completed? Be specific. Quote or paraphrase from the plan.
   - Column D (Next): What does the go-forward plan say the NEXT CONCRETE ACTION is? Quote or paraphrase the plan directly.
   - Column E (Waiting On): What is ACTUALLY blocking this project? "Nothing" if unblocked. Be specific about what the user needs to do or decide.
4. If a project has no plan file or the plan says "Plan not yet created", write exactly that in all content columns.
5. Every cell must contain current, accurate, project-specific information. Generic filler is UNACCEPTABLE.

GOOGLE SHEETS CREDENTIALS:
- Sheet URL: https://docs.google.com/spreadsheets/d/1kDBFSnfpTUWKW7bQcPTGsiQ7uZQO1KU2lCdgvm3KoVI/edit
- Service Account: djini-sheets@cryptodjinni.iam.gserviceaccount.com
- GCP credentials: GCP_SERVICE_ACCOUNT_JSON environment variable
- Tab name: Command Center
- CLEAR the sheet and rewrite from scratch to ensure correct format every time.

MANUS API:
- API Key: """ + DASHBOARD_MANUS_API_KEY + """
- Base URL: https://api.manus.ai
- Endpoint: GET /v2/project.list

GITHUB:
- Repo: daviderichammer/the-project-project
- PAT: """ + GITHUB_PAT + """
- Plans path: {project-name}/go-forward-plan.md (lowercase-with-hyphens folder names)

RULES:
- Ignore all projects whose name starts with "ZZ"
- Tasks not connected to any project should be ignored
- Always add "Last Updated" timestamp at the bottom
- NEVER delete the GCP credential file after use"""

# ---------------------------------------------------------------------------
# Public key cache
# ---------------------------------------------------------------------------
_pubkey_cache: Dict[str, Any] = {"key": None, "fetched_at": 0}
_PUBKEY_TTL = 3600  # 1 hour


def _get_public_key() -> Optional[str]:
    """Return the cached Manus webhook public key, refreshing if stale."""
    now = time.time()
    if _pubkey_cache["key"] and (now - _pubkey_cache["fetched_at"]) < _PUBKEY_TTL:
        return _pubkey_cache["key"]
    try:
        resp = requests.get(
            f"{MANUS_API_BASE}/webhook.publicKey",
            headers={"x-manus-api-key": MANUS_API_KEY},
            timeout=10,
        )
        resp.raise_for_status()
        data = resp.json()
        pem = data.get("public_key")
        if pem:
            _pubkey_cache["key"] = pem
            _pubkey_cache["fetched_at"] = now
            logger.info("Webhook public key refreshed.")
            return pem
    except Exception as exc:
        logger.warning(f"Could not fetch webhook public key: {exc}")
    return _pubkey_cache.get("key")  # return stale if available


def _verify_signature(url: str, body: bytes, sig_b64: str, timestamp: str) -> bool:
    """
    Verify RSA-SHA256 webhook signature from Manus.
    Signed content format: {timestamp}.{url}.{sha256_hex(body)}
    Manus signs: RSA-SHA256(signed_content)  — i.e. sign(sha256(signed_content))
    IMPORTANT: Do NOT pre-hash signed_content before calling key.verify().
    Pass signed_content directly with hashes.SHA256() so the library does
    exactly one SHA256 hash (standard RSA-SHA256). Pre-hashing + SHA256 algo
    would double-hash, causing verification to always fail.
    """
    try:
        from cryptography.hazmat.primitives import hashes, serialization
        from cryptography.hazmat.primitives.asymmetric import padding
        from cryptography.exceptions import InvalidSignature

        # Reject stale timestamps (5-minute window)
        if abs(time.time() - int(timestamp)) > 300:
            logger.warning("Webhook timestamp too old — possible replay attack.")
            return False

        pem = _get_public_key()
        if not pem:
            logger.warning("No public key available — failing open.")
            return True  # fail-open if key unavailable

        # Reconstruct the signed content string
        body_hash = hashlib.sha256(body).hexdigest()
        signed_content = f"{timestamp}.{url}.{body_hash}".encode()

        # Verify: pass signed_content directly (single SHA256 hash by RSA-SHA256)
        key = serialization.load_pem_public_key(pem.encode())
        key.verify(
            base64.b64decode(sig_b64),
            signed_content,          # <-- NOT pre-hashed
            padding.PKCS1v15(),
            hashes.SHA256(),         # library does SHA256 internally
        )
        return True
    except Exception as exc:
        logger.warning(f"Signature verification failed: {exc}")
        return False


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _api_headers() -> Dict[str, str]:
    return {
        "x-manus-api-key": MANUS_API_KEY,
        "Content-Type": "application/json",
    }


def _fetch_credits_balance() -> Optional[Dict[str, Any]]:
    """
    Fetch the current available credits balance from the Manus API.
    Returns a dict with total_credits, pro_monthly_credits, free_credits,
    periodic_credits, and a formatted 'credits_label' string, or None if
    the call fails.
    Response schema: {"free_credits": N, "ok": True, "periodic_credits": N,
                      "pro_monthly_credits": N, "total_credits": N}
    """
    try:
        resp = requests.get(
            f"{MANUS_API_BASE}/usage.availableCredits",
            headers={"x-manus-api-key": MANUS_API_KEY},
            timeout=10,
        )
        resp.raise_for_status()
        data = resp.json()
        total = (
            data.get("total_credits")
            or data.get("data", {}).get("total_credits")
            or data.get("available_credits")
            or data.get("data", {}).get("available_credits")
            or data.get("credits")
            or data.get("balance")
        )
        if total is not None:
            total = int(total)
            pro_monthly = data.get("pro_monthly_credits") or data.get("data", {}).get("pro_monthly_credits")
            free = data.get("free_credits") or data.get("data", {}).get("free_credits")
            periodic = data.get("periodic_credits") or data.get("data", {}).get("periodic_credits")
            if pro_monthly is not None:
                credits_label = f"Credits remaining: {total:,} / {int(pro_monthly):,}"
            else:
                credits_label = f"Credits remaining: {total:,}"
            logger.info(f"Credits balance fetched: {credits_label}")
            return {
                "total_credits": total,
                "pro_monthly_credits": int(pro_monthly) if pro_monthly is not None else None,
                "free_credits": int(free) if free is not None else None,
                "periodic_credits": int(periodic) if periodic is not None else None,
                "credits_label": credits_label,
            }
        # Log the raw response so we can adapt if the schema differs
        logger.warning(f"Credits balance key not found in response: {data}")
        return None
    except Exception as exc:
        logger.error(f"Failed to fetch credits balance: {exc}")
        return None


def _forward_to_steward(task_id: str, task_title: str, task_url: str,
                         message: str, stop_reason: str,
                         credits_info: Optional[Dict[str, Any]] = None) -> None:
    """Send a compact completion/pause notice to the Steward via task.sendMessage."""
    now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    status_emoji = "✅" if stop_reason == "finish" else "⏸️"
    status_label = "COMPLETED" if stop_reason == "finish" else "PAUSED (needs input)"

    credits_line = ""
    if credits_info is not None:
        credits_line = f"\n**{credits_info['credits_label']}**"
    else:
        credits_line = "\n**Credits remaining:** (unavailable)"

    content = (
        f"{status_emoji} **Task {status_label}** — {now_utc}\n\n"
        f"**Title:** {task_title}\n"
        f"**Task ID:** {task_id}\n"
        f"**Link:** {task_url}"
        f"{credits_line}\n\n"
        f"**Summary:**\n{message[:800]}{'...' if len(message) > 800 else ''}"
    )
    payload = {
        "task_id": STEWARD_TASK_ID,
        "message": {"content": content},
    }
    logger.info(f"Steward notification content preview: {content[:300]}")
    try:
        resp = requests.post(
            f"{MANUS_API_BASE}/task.sendMessage",
            headers=_api_headers(),
            json=payload,
            timeout=30,
        )
        resp.raise_for_status()
        logger.info(f"Forwarded {stop_reason!r} notice to Steward for task: {task_id}")
    except Exception as exc:
        logger.error(f"Failed to forward to Steward: {exc}")
        if hasattr(exc, "response") and exc.response is not None:
            logger.error(f"Steward API response: {exc.response.text}")


def _is_dashboard_refresh_task(task_title: str) -> bool:
    """Return True when the webhook task title matches a refresh naming prefix."""
    normalized_title = (task_title or "").strip().casefold()
    return any(normalized_title.startswith(prefix) for prefix in DASHBOARD_REFRESH_TITLE_PREFIXES)


def _spawn_full_dashboard_update(triggering_task_title: str,
                                  credits_info: Optional[Dict[str, Any]] = None) -> str:
    """
    Spawn a full Manus dashboard-update task — identical to the hourly scheduled run.
    Reads all project go-forward plans from GitHub and rewrites the entire sheet.
    Does NOT write to the sheet directly from this handler.

    A 600-second cooldown prevents multiple spawns when several tasks finish
    in quick succession.

    If credits_info is provided, it is appended to the prompt so the refresh
    task can include it in the sheet or the Steward can read it.
    """
    # Build the credits context suffix
    if credits_info is not None:
        credits_context = (
            f"\n\nCREDITS BALANCE CONTEXT:\n"
            f"At the time this refresh was triggered, the available Manus credits balance "
            f"was {credits_info['credits_label']}. Please include this figure in the dashboard "
            f"(e.g., in a footer row or a dedicated 'Credits Remaining' cell below the "
            f"'Last Updated' timestamp)."
        )
    else:
        credits_context = ""

    new_title = f"Dashboard Full Refresh (triggered by: {triggering_task_title[:60]})"
    payload = {
        "title": new_title,
        "project_id": DASHBOARD_PROJECT_ID,
        "message": {
            "content": FULL_DASHBOARD_INSTRUCTION + credits_context,
            "connectors": [GCP_CONNECTOR_ID],
        },
    }

    # The timestamp file also serves as the inter-process lock. Holding LOCK_EX
    # across the check and task.create call prevents concurrent webhook requests
    # from racing. The timestamp is committed only after a successful API call.
    with open(DASHBOARD_COOLDOWN_FILE, "a+", encoding="utf-8") as lock_file:
        fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
        lock_file.seek(0)
        raw_timestamp = lock_file.read().strip()
        try:
            last_refresh_time = float(raw_timestamp) if raw_timestamp else 0.0
        except ValueError:
            logger.warning("Invalid dashboard cooldown timestamp; treating it as expired.")
            last_refresh_time = 0.0

        now = time.time()
        elapsed = now - last_refresh_time
        if 0 <= elapsed < DASHBOARD_COOLDOWN_SECONDS:
            remaining = max(1, int(DASHBOARD_COOLDOWN_SECONDS - elapsed + 0.999))
            logger.info(
                f"Dashboard refresh skipped: cooldown active ({remaining}s remaining) "
                f"for {triggering_task_title!r}"
            )
            return "cooldown"

        try:
            resp = requests.post(
                f"{MANUS_API_BASE}/task.create",
                headers=_api_headers(),
                json=payload,
                timeout=30,
            )
            resp.raise_for_status()
            result = resp.json()
            if result.get("ok") is False:
                raise RuntimeError("Manus task.create returned ok=false")

            spawned_id = result.get("data", {}).get("task_id") or result.get("task_id", "unknown")
            success_time = time.time()
            lock_file.seek(0)
            lock_file.truncate()
            lock_file.write(f"{success_time:.6f}\n")
            lock_file.flush()
            os.fsync(lock_file.fileno())
            logger.info(
                f"Dashboard refresh triggered successfully: {spawned_id} "
                f"(triggered by: {triggering_task_title!r})"
            )
            return "triggered"
        except Exception as exc:
            logger.error(f"Dashboard refresh API failure: {exc}")
            return "failed"


# ---------------------------------------------------------------------------
# FastAPI app
# ---------------------------------------------------------------------------
app = FastAPI(title="Manus Webhook Listener")


@app.get("/health")
def health_check():
    return {"status": "healthy", "timestamp": int(time.time())}


@app.get("/credits")
def credits_endpoint():
    """
    Option B: On-demand credits balance endpoint.
    The Steward (or any caller) can GET /credits at any time to retrieve
    the current available Manus credits balance with a full breakdown.
    """
    now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    try:
        resp = requests.get(
            f"{MANUS_API_BASE}/usage.availableCredits",
            headers={"x-manus-api-key": MANUS_API_KEY},
            timeout=10,
        )
        resp.raise_for_status()
        data = resp.json()
        total = data.get("total_credits")
        free = data.get("free_credits")
        periodic = data.get("periodic_credits")
        pro_monthly = data.get("pro_monthly_credits")
        if total is not None:
            logger.info(f"Credits endpoint: total={total}, free={free}, periodic={periodic}, pro_monthly={pro_monthly}")
            return {
                "status": "ok",
                "total_credits": total,
                "free_credits": free,
                "periodic_credits": periodic,
                "pro_monthly_credits": pro_monthly,
                "fetched_at": now_utc,
            }
        logger.warning(f"Credits endpoint: unexpected response schema: {data}")
        return {
            "status": "error",
            "total_credits": None,
            "fetched_at": now_utc,
            "message": f"Unexpected API response schema: {data}",
        }
    except Exception as exc:
        logger.error(f"Credits endpoint failed: {exc}")
        return {
            "status": "error",
            "total_credits": None,
            "fetched_at": now_utc,
            "message": f"Failed to fetch credits balance: {exc}",
        }


@app.post("/webhook/manus")
async def manus_webhook(request: Request):
    """
    Accept all Manus webhook POST requests.
    Uses a raw dict body to avoid 422 errors from verification pings or
    schema changes.
    """
    body_bytes = await request.body()

    # --- Signature verification ---
    sig = request.headers.get("x-webhook-signature")
    ts = request.headers.get("x-webhook-timestamp")
    if sig and ts:
        full_url = str(request.url)
        if not _verify_signature(full_url, body_bytes, sig, ts):
            logger.warning(f"Invalid webhook signature from {request.client.host} — rejecting.")
            return Response(content="Unauthorized", status_code=401)
        logger.info("Webhook signature verified OK.")
    else:
        logger.debug("No signature headers — proceeding without verification (test/manual request).")

    # --- Parse body ---
    try:
        data: Dict[str, Any] = json.loads(body_bytes)
    except Exception:
        logger.warning("Received non-JSON body — ignoring.")
        return {"status": "ignored", "reason": "Non-JSON body"}

    event_type = data.get("event_type", "")
    task_detail = data.get("task_detail") or {}
    task_id = task_detail.get("task_id", "unknown")
    task_title = task_detail.get("task_title", "")
    task_url = task_detail.get("task_url", "")
    stop_reason = task_detail.get("stop_reason", "")
    message_content = task_detail.get("message") or ""

    logger.info(
        f"Event: {event_type!r} | task: {task_id} | "
        f"title: {task_title!r} | stop_reason: {stop_reason!r}"
    )

    # --- Gate 1: Only act on task_stopped ---
    if event_type != "task_stopped":
        logger.info(f"Ignoring event type: {event_type!r}")
        return {"status": "ignored", "reason": "Not a task_stopped event"}

    # --- Gate 2: Self-exclusion before ANY downstream action ---
    # Dashboard/refresh completions must not be forwarded to Steward because
    # that creates a second, independent refresh path outside this cooldown.
    normalized_title = task_title.casefold()
    if stop_reason == "finish" and (
        "dashboard" in normalized_title or "refresh" in normalized_title
    ):
        logger.info(f"Dashboard/refresh self-exclusion: {task_title!r}")
        return {"status": "ignored", "reason": "Dashboard/refresh self-exclusion"}

    # --- Fetch credits balance on ALL task_stopped events ---
    # Credits are fetched for both finished and paused tasks so every
    # notification includes the current balance in "Credits remaining: X / Y" format.
    credits_info: Optional[Dict[str, Any]] = _fetch_credits_balance()
    if credits_info:
        logger.info(f"Credits fetched for event: {credits_info['credits_label']}")
    else:
        logger.warning("Credits fetch returned None — balance will show as unavailable")

    # --- Gate 3: Forward paused tasks only ---
    # Finished-task forwarding previously caused Steward to create separate
    # "Refresh TV Dashboard" tasks that bypassed this service's cooldown.
    # Finished tasks are handled exclusively by the locked task.create path below.
    if stop_reason != "finish" and task_id and task_id != "unknown":
        _forward_to_steward(task_id, task_title, task_url, message_content,
                            stop_reason, credits_info)
    elif stop_reason == "finish":
        logger.info("Completion forwarding to Steward suppressed; cooldown-controlled refresh owns this event")

    # --- Gate 4: Dashboard update only for finish events ---
    if stop_reason != "finish":
        logger.info(f"Skipping dashboard update — stop_reason: {stop_reason!r}")
        credits_label = credits_info["credits_label"] if credits_info else "unavailable"
        return {"status": "forwarded_to_steward", "reason": "Task paused, not finished", "credits_remaining": credits_label}

    # --- Gate 4: Explicitly prevent refresh tasks from recursively spawning ---
    # task_detail.task_title is the webhook's actual task identity field.
    if _is_dashboard_refresh_task(task_title):
        logger.info(f"Dashboard self-refresh skipped: {task_title!r}")
        return {"status": "forwarded_to_steward", "reason": "Dashboard self-refresh task"}

    # --- Gate 5: Block title words (loop prevention for dashboard spawn) ---
    title_lower = task_title.lower()
    for word in BLOCKED_TITLE_WORDS:
        if word in title_lower:
            logger.info(f"Blocked title word '{word}' — skipping dashboard: {task_title!r}")
            return {"status": "forwarded_to_steward", "reason": f"Blocked title word: {word}"}

    # --- Gate 6: Spawn full dashboard refresh (with 600s cooldown) ---
    # No project matching needed — the spawned task reads ALL projects from the API.
    # Credits balance is passed into the prompt so the refresh task can include it.
    logger.info(f"Spawning full dashboard refresh for completed task: {task_title!r}")
    refresh_result = _spawn_full_dashboard_update(task_title, credits_info)

    credits_label = credits_info["credits_label"] if credits_info else "unavailable"

    if refresh_result == "cooldown":
        return {
            "status": "skipped",
            "action": "cooldown_active",
            "credits_remaining": credits_label,
        }
    if refresh_result == "triggered":
        return {
            "status": "success",
            "action": "full_dashboard_refresh_spawned",
            "credits_remaining": credits_label,
        }
    return {
        "status": "error",
        "action": "dashboard_refresh_failed",
        "credits_remaining": credits_label,
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8089)
