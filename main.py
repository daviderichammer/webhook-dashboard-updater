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
import re
import sqlite3
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
# A generated refresh stores at most the first 60 characters of its source
# title. Title text is only a candidate identifier; task detail decides type.
DASHBOARD_TRIGGER_TITLE_LIMIT = 60
DASHBOARD_TRIGGER_SCAN_PAGES = 5
DASHBOARD_TRIGGER_SCAN_LIMIT = 100
DASHBOARD_TRIGGER_REFERENCE_RE = re.compile(
    r"^dashboard\s+full\s+refresh\s+\(triggered\s+by:\s*(?P<reference>.+?)\s*\)\s*$",
    re.IGNORECASE,
)

# The received routine Host Leads callbacks use titles such as
# "Verify <host> Turo host/listing and business contact". Their generated
# dashboard follow-ons preserve that title in "triggered by: ...". This rule
# deliberately does not use project IDs or task IDs, which are absent from the
# callback ledger and not stable across the Host Leads queue.
ROUTINE_TURO_TITLE_PREFIXES = (
    "verify ",
    "dashboard full refresh (triggered by: verify ",
)
EXPLICIT_TURO_NOTIFICATION_TITLE_MARKERS = (
    "status",
    "monitor",
    "audit",
)
MATERIAL_TURO_COMPLETION_MESSAGE_MARKERS = (
    "error",
    "failed",
    "failure",
    "exception",
    "unable to",
    "could not",
    "timed out",
    "timeout",
    "blocked",
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

# Delivery evidence and idempotency state. This holds only non-secret metadata
# and a SHA-256 digest of the received body; it never stores webhook contents.
WEBHOOK_EVENT_DB = os.environ.get(
    "MANUS_WEBHOOK_EVENT_DB", "/var/lib/manus-webhook/events.sqlite3"
)
WEBHOOK_EVENT_RETENTION_SECONDS = 30 * 24 * 60 * 60

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
            logger.error("No webhook public key available — rejecting callback.")
            return False

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


def _fetch_task_membership(task_id: str) -> Optional[Dict[str, str]]:
    """Return authenticated task type/status, or None when membership is unresolved.

    Lifecycle callbacks contain no project ID. A lookup failure remains retryable
    and must never become a silent notification decision.
    """
    try:
        response = requests.get(
            f"{MANUS_API_BASE}/task.detail",
            headers=_api_headers(),
            params={"task_id": task_id},
            timeout=20,
        )
        response.raise_for_status()
        data = response.json()
        if not isinstance(data, dict) or data.get("ok") is not True:
            logger.error("Task-detail lookup returned an unsuccessful envelope for task_id=%s", task_id)
            return None
        task = data.get("task")
        if not isinstance(task, dict):
            nested = data.get("data")
            task = nested.get("task") if isinstance(nested, dict) else None
        if not isinstance(task, dict):
            logger.error("Task-detail lookup omitted task metadata for task_id=%s", task_id)
            return None
        task_type = task.get("task_type")
        task_status = task.get("status", "")
        if not isinstance(task_type, str) or not isinstance(task_status, str):
            logger.error("Task-detail lookup returned invalid membership metadata for task_id=%s", task_id)
            return None
        return {"task_type": task_type, "status": task_status}
    except Exception as exc:
        logger.error("Task-detail lookup failed for task_id=%s: %s", task_id, exc)
        return None


def _notification_stop_reason(stop_reason: str, task_status: str) -> str:
    """Promote an authoritative task error state for coordinator visibility."""
    return "error" if task_status == "error" else stop_reason


def _event_db() -> sqlite3.Connection:
    """Open the local, non-secret webhook delivery evidence store."""
    os.makedirs(os.path.dirname(WEBHOOK_EVENT_DB), mode=0o700, exist_ok=True)
    conn = sqlite3.connect(WEBHOOK_EVENT_DB, timeout=5)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS webhook_events (
            event_id TEXT PRIMARY KEY,
            task_id TEXT NOT NULL,
            event_type TEXT NOT NULL,
            stop_reason TEXT NOT NULL,
            body_sha256 TEXT NOT NULL,
            received_at INTEGER NOT NULL,
            processed_at INTEGER,
            status TEXT NOT NULL CHECK(status IN ('processing', 'processed')),
            disposition TEXT NOT NULL DEFAULT 'recorded'
        )
        """
    )
    existing_columns = {row[1] for row in conn.execute("PRAGMA table_info(webhook_events)")}
    if "disposition" not in existing_columns:
        try:
            conn.execute(
                "ALTER TABLE webhook_events "
                "ADD COLUMN disposition TEXT NOT NULL DEFAULT 'recorded'"
            )
        except sqlite3.OperationalError as exc:
            if "duplicate column name" not in str(exc).casefold():
                raise
        conn.commit()
    conn.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS webhook_task_event_once
        ON webhook_events(task_id, event_type, stop_reason)
        """
    )
    return conn


def _reserve_event(
    event_id: str, task_id: str, event_type: str, stop_reason: str, body_sha256: str
) -> str:
    """Atomically reserve an event; return reserved, duplicate, or in_progress."""
    now = int(time.time())
    conn = _event_db()
    try:
        conn.execute("BEGIN IMMEDIATE")
        conn.execute(
            """
            INSERT INTO webhook_events
            (event_id, task_id, event_type, stop_reason, body_sha256, received_at, status)
            VALUES (?, ?, ?, ?, ?, ?, 'processing')
            """,
            (event_id, task_id, event_type, stop_reason, body_sha256, now),
        )
        conn.commit()
        return "reserved"
    except sqlite3.IntegrityError:
        conn.rollback()
        row = conn.execute(
            "SELECT status FROM webhook_events WHERE event_id = ? OR "
            "(task_id = ? AND event_type = ? AND stop_reason = ?) "
            "ORDER BY received_at DESC LIMIT 1",
            (event_id, task_id, event_type, stop_reason),
        ).fetchone()
        return "duplicate" if row and row[0] == "processed" else "in_progress"
    finally:
        conn.close()


def _complete_event(event_id: str, disposition: str = "recorded") -> None:
    conn = _event_db()
    try:
        conn.execute(
            "UPDATE webhook_events SET status = 'processed', processed_at = ?, disposition = ? "
            "WHERE event_id = ?",
            (int(time.time()), disposition, event_id),
        )
        conn.commit()
    finally:
        conn.close()


def _release_event(event_id: str) -> None:
    """Release a failed delivery so Manus may retry it safely."""
    conn = _event_db()
    try:
        conn.execute(
            "DELETE FROM webhook_events WHERE event_id = ? AND status = 'processing'",
            (event_id,),
        )
        conn.commit()
    finally:
        conn.close()


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
                         credits_info: Optional[Dict[str, Any]] = None) -> bool:
    """Send a compact completion/pause notice to the Steward via task.sendMessage."""
    now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    status_emoji, status_label = {
        "finish": ("✅", "COMPLETED"),
        "ask": ("⏸️", "PAUSED (needs input)"),
        "error": ("❌", "FAILED"),
    }.get(stop_reason, ("ℹ️", "STATUS UPDATE"))

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
        return True
    except Exception as exc:
        logger.error(f"Failed to forward to Steward: {exc}")
        return False


def _is_dashboard_refresh_task(task_title: str) -> bool:
    """Return True when the webhook task title matches a refresh naming prefix."""
    normalized_title = (task_title or "").strip().casefold()
    return any(normalized_title.startswith(prefix) for prefix in DASHBOARD_REFRESH_TITLE_PREFIXES)


def _dashboard_refresh_trigger_membership(
    refresh_task_id: str, task_title: str
) -> Optional[Dict[str, str]]:
    """Return membership only for one authenticated title-matched source task.

    The embedded title is a candidate lookup key only. Errors or ambiguity fail
    open so ordinary project refresh notifications are never suppressed.
    """
    match = DASHBOARD_TRIGGER_REFERENCE_RE.match((task_title or "").strip())
    if not match:
        return None
    reference = match.group("reference").strip()
    if not reference:
        return None

    candidates: Dict[str, str] = {}
    cursor: Optional[str] = None
    try:
        for _ in range(DASHBOARD_TRIGGER_SCAN_PAGES):
            params: Dict[str, Any] = {
                "scope": "all",
                "order": "desc",
                "limit": DASHBOARD_TRIGGER_SCAN_LIMIT,
            }
            if cursor:
                params["cursor"] = cursor
            response = requests.get(
                f"{MANUS_API_BASE}/task.list",
                headers=_api_headers(),
                params=params,
                timeout=20,
            )
            response.raise_for_status()
            data = response.json()
            tasks = data.get("data")
            if not isinstance(tasks, list):
                logger.warning("Task-list lookup returned invalid data while resolving dashboard trigger.")
                return None
            for task in tasks:
                if not isinstance(task, dict):
                    continue
                candidate_id = task.get("id")
                candidate_title = task.get("title")
                if (
                    not isinstance(candidate_id, str)
                    or candidate_id == refresh_task_id
                    or not isinstance(candidate_title, str)
                    or _is_dashboard_refresh_task(candidate_title)
                ):
                    continue
                if candidate_title == reference or (
                    len(reference) == DASHBOARD_TRIGGER_TITLE_LIMIT
                    and candidate_title.startswith(reference)
                ):
                    candidates[candidate_id] = candidate_title
            if not data.get("has_more"):
                break
            cursor = data.get("next_cursor")
            if not isinstance(cursor, str) or not cursor:
                logger.warning("Task-list lookup omitted cursor while resolving dashboard trigger.")
                return None
    except Exception as exc:
        logger.warning("Could not resolve dashboard refresh trigger reference: %s", exc)
        return None

    if len(candidates) != 1:
        logger.info(
            "Dashboard refresh trigger reference is not uniquely resolvable "
            "refresh_task_id=%s candidate_count=%s",
            refresh_task_id,
            len(candidates),
        )
        return None

    source_task_id = next(iter(candidates))
    membership = _fetch_task_membership(source_task_id)
    if membership is None:
        logger.warning(
            "Dashboard refresh trigger task-detail lookup failed refresh_task_id=%s source_task_id=%s",
            refresh_task_id,
            source_task_id,
        )
        return None
    return membership


def _is_routine_turo_completion(
    task_title: str, stop_reason: str, message_content: str
) -> bool:
    """Return True only for routine successful Turo Host Leads work.

    The test is intentionally conservative and based only on callback fields:
    a successful stopped event, a title beginning with the received routine
    verification or generated-refresh patterns, and a Turo marker. Explicit
    status/monitor/audit titles and completion messages containing failure
    signals remain coordinator-visible.
    """
    if stop_reason != "finish":
        return False
    normalized_title = (task_title or "").strip().casefold()
    normalized_message = (message_content or "").casefold()
    if "turo" not in normalized_title:
        return False
    if any(marker in normalized_title for marker in EXPLICIT_TURO_NOTIFICATION_TITLE_MARKERS):
        return False
    if any(marker in normalized_message for marker in MATERIAL_TURO_COMPLETION_MESSAGE_MARKERS):
        return False
    return normalized_title.startswith(ROUTINE_TURO_TITLE_PREFIXES)


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
    """Authenticate, validate, de-duplicate, and process Manus lifecycle events."""
    body_bytes = await request.body()
    sig = request.headers.get("x-webhook-signature")
    ts = request.headers.get("x-webhook-timestamp")
    if not sig or not ts:
        logger.warning("Rejected webhook with missing signature headers.")
        return Response(content="Unauthorized", status_code=401)
    if not _verify_signature(str(request.url), body_bytes, sig, ts):
        logger.warning("Rejected webhook with invalid signature.")
        return Response(content="Unauthorized", status_code=401)

    try:
        data: Dict[str, Any] = json.loads(body_bytes)
    except (TypeError, ValueError, json.JSONDecodeError):
        logger.warning("Rejected malformed webhook JSON.")
        return Response(content="Bad Request", status_code=400)
    if not isinstance(data, dict):
        logger.warning("Rejected webhook JSON that is not an object.")
        return Response(content="Bad Request", status_code=400)

    event_id = data.get("event_id")
    event_type = data.get("event_type")
    task_detail = data.get("task_detail")
    if (
        not isinstance(event_id, str)
        or not event_id
        or event_type not in {"task_created", "task_stopped"}
        or not isinstance(task_detail, dict)
    ):
        logger.warning("Rejected webhook with unsupported event envelope.")
        return Response(content="Bad Request", status_code=400)

    task_id = task_detail.get("task_id")
    task_title = task_detail.get("task_title")
    task_url = task_detail.get("task_url")
    if not all(isinstance(value, str) and value for value in (task_id, task_title, task_url)):
        logger.warning("Rejected webhook with incomplete task detail.")
        return Response(content="Bad Request", status_code=400)

    stop_reason = ""
    message_content = ""
    if event_type == "task_stopped":
        stop_reason = task_detail.get("stop_reason")
        message_content = task_detail.get("message")
        if stop_reason not in {"finish", "ask", "error"} or not isinstance(message_content, str):
            logger.warning("Rejected webhook with invalid stopped-task detail.")
            return Response(content="Bad Request", status_code=400)

    body_hash = hashlib.sha256(body_bytes).hexdigest()
    reservation = _reserve_event(event_id, task_id, event_type, stop_reason, body_hash)
    logger.info(
        "Webhook received event_id=%s task_id=%s event_type=%s stop_reason=%s "
        "body_sha256=%s reservation=%s",
        event_id,
        task_id,
        event_type,
        stop_reason or "none",
        body_hash[:16],
        reservation,
    )
    if reservation == "duplicate":
        return {"status": "duplicate"}
    if reservation == "in_progress":
        return Response(content="Accepted", status_code=202)

    if event_type == "task_created":
        _complete_event(event_id)
        logger.info("Webhook recorded task creation event_id=%s task_id=%s", event_id, task_id)
        return {"status": "recorded"}

    # Preserve credit retrieval and dashboard behavior for all stopped events.
    credits_info: Optional[Dict[str, Any]] = _fetch_credits_balance()

    # The lifecycle callback has no project ID. Routing is based only on the
    # authenticated task-detail response; unresolved lookup failures are retried.
    membership = _fetch_task_membership(task_id)
    if membership is None:
        _release_event(event_id)
        logger.error("Coordinator routing lookup failed event_id=%s task_id=%s", event_id, task_id)
        return Response(content="Service Unavailable", status_code=503)

    is_project_task = membership["task_type"] == "project"
    notification_reason = _notification_stop_reason(stop_reason, membership["status"])
    disposition = "suppressed_orphan_task"
    suppress_orphan_triggered_dashboard_refresh = False
    if is_project_task and _is_dashboard_refresh_task(task_title):
        trigger_membership = _dashboard_refresh_trigger_membership(task_id, task_title)
        suppress_orphan_triggered_dashboard_refresh = (
            trigger_membership is not None and trigger_membership["task_type"] != "project"
        )

    if not is_project_task:
        logger.info(
            "Orphan task event recorded without coordinator forwarding "
            "event_id=%s task_id=%s task_type=%s", event_id, task_id, membership["task_type"],
        )
    elif suppress_orphan_triggered_dashboard_refresh:
        disposition = "suppressed_dashboard_refresh_orphan_trigger"
        logger.info(
            "Orphan-triggered dashboard refresh recorded without coordinator forwarding "
            "event_id=%s task_id=%s",
            event_id,
            task_id,
        )
    else:
        if not _forward_to_steward(
            task_id, task_title, task_url, message_content, notification_reason, credits_info
        ):
            _release_event(event_id)
            logger.error("Coordinator notification failed event_id=%s task_id=%s", event_id, task_id)
            return Response(content="Service Unavailable", status_code=503)
        disposition = f"coordinator_notified_project_{notification_reason}"
        logger.info(
            "Project coordinator notification processed event_id=%s task_id=%s task_status=%s",
            event_id, task_id, membership["status"],
        )

    if stop_reason != "finish":
        _complete_event(event_id, disposition)
        return {
            "status": "processed",
            "action": (
                "orphan_triggered_dashboard_refresh_recorded"
                if suppress_orphan_triggered_dashboard_refresh
                else "coordinator_notified" if is_project_task else "orphan_task_recorded"
            ),
        }

    # Preserve the existing dashboard loop-prevention and cooldown behavior.
    if _is_dashboard_refresh_task(task_title):
        _complete_event(event_id, disposition)
        logger.info("Dashboard self-refresh not re-triggered event_id=%s", event_id)
        action = (
            "orphan_triggered_dashboard_refresh_recorded"
            if suppress_orphan_triggered_dashboard_refresh
            else "coordinator_notified" if is_project_task else "orphan_task_recorded"
        )
        return {"status": "processed", "action": action}

    title_lower = task_title.lower()
    for word in BLOCKED_TITLE_WORDS:
        if word in title_lower:
            _complete_event(event_id, disposition)
            logger.info("Dashboard update skipped for blocked-title event_id=%s", event_id)
            action = (
            "orphan_triggered_dashboard_refresh_recorded"
            if suppress_orphan_triggered_dashboard_refresh
            else "coordinator_notified" if is_project_task else "orphan_task_recorded"
        )
            return {"status": "processed", "action": action}

    logger.info("Spawning dashboard refresh after completion event_id=%s", event_id)
    refresh_result = _spawn_full_dashboard_update(task_title, credits_info)
    _complete_event(event_id, disposition)
    if refresh_result == "cooldown":
        return {"status": "processed", "action": "cooldown_active"}
    if refresh_result == "triggered":
        return {"status": "processed", "action": "dashboard_refresh_spawned"}
    return {"status": "processed", "action": "dashboard_refresh_failed"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8089)
