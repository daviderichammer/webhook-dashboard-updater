#!/usr/bin/env python3
"""Durable, read-only monitor for the eight fixed KeZ implementation lanes.

The monitor reads task metadata and recent task messages, sends one compact
coordinator notification only for material state changes, and stores only
non-secret fingerprints/metadata in its durable state file. It never creates,
resumes, stops, or otherwise mutates a lane task.
"""

from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import logging
import os
import re
import sys
import tempfile
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Iterable

import requests
from archer_control import enqueue_notice

API_BASE = "https://api.manus.ai/v2"
STATE_PATH = Path(os.environ.get("KEZ_LANE_MONITOR_STATE", "/var/lib/kez-lane-monitor/state.json"))
LOCK_PATH = Path(os.environ.get("KEZ_LANE_MONITOR_LOCK", "/var/lib/kez-lane-monitor/monitor.lock"))
CHECK_INTERVAL_SECONDS = 10 * 60
STALE_AFTER_SECONDS = 2 * CHECK_INTERVAL_SECONDS
REQUEST_TIMEOUT_SECONDS = 30
API_RETRY_ATTEMPTS = 3

LANES: tuple[tuple[str, str], ...] = (
    ("Lane 1", "4p6kLsEogjCsrGRcjJUpUc"),
    ("Lane 2", "XhmARnoydBrLNWvFA3cPj4"),
    ("Lane 3", "hW8mYSvvtHpvFev2XKHayD"),
    ("Lane 4", "RAFHdQseufxHrTb6nPVSqQ"),
    ("Lane 5", "hmgAWFPiCAsfYBV9sEbH5d"),
    ("Lane 6", "ZNwHpuLK2vTMDZwkSxWk3p"),
    ("Lane 7", "mV7EQjkQGgpXmHX58BXxpf"),
    ("Lane 8", "ALUgh3uJchgLr8PbeVfrxQ"),
)

MATERIAL_STATUSES = {"completed", "stopped", "idle", "waiting", "paused", "error", "failed"}
QUESTION_RE = re.compile(
    r"(?:\?|\b(?:need|needs|please provide|clarif(?:y|ication)|decision|confirm|which|can you|could you)\b)",
    re.IGNORECASE,
)
BLOCKER_RE = re.compile(
    r"\b(?:blocked|blocker|cannot|can't|unable|failure|failed|error|exception|missing|permission|access denied|requires? (?:a |an )?(?:decision|input|approval))\b",
    re.IGNORECASE,
)
RESULT_RE = re.compile(
    r"\b(?:result|artifact|deliverable|report|summary|implemented|deployed|attached|created|available|output|export|file)\b",
    re.IGNORECASE,
)
SECRET_PATTERNS = (
    re.compile(r"\bsk-[A-Za-z0-9_-]{12,}\b"),
    re.compile(r"\b(?:AKIA|ASIA)[A-Z0-9]{16}\b"),
    re.compile(r"(?i)\b(?:bearer\s+)[A-Za-z0-9._~+/-]{12,}"),
    re.compile(r"(?i)\b(?:api[_ -]?key|token|secret|password|authorization)\s*([=:])\s*[^\s,;]+"),
)


class MonitorError(RuntimeError):
    """Raised for monitor failures that should make systemd retry."""


@dataclass(frozen=True)
class Observation:
    lane: str
    task_id: str
    title: str
    task_status: str
    agent_status: str
    event_id: str
    event_type: str
    event_timestamp: str
    event_summary: str
    event_hash: str
    material_kind: str | None

    @property
    def effective_status(self) -> str:
        return self.agent_status or self.task_status or "unknown"

    @property
    def state_key(self) -> str:
        raw = "|".join(
            (self.task_status, self.agent_status, self.event_id, self.event_type, self.event_hash)
        )
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()

    @property
    def condition_key(self) -> str:
        """Fingerprint only the lane status, independent of routine event churn."""
        return hashlib.sha256(
            f"{self.task_status}|{self.agent_status}".encode("utf-8")
        ).hexdigest()


def utc_now() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S UTC")


def safe_text(value: Any, limit: int = 700) -> str:
    """Normalize text for notices/logs and redact common credential forms."""
    if value is None:
        return ""
    if isinstance(value, str):
        text = value
    elif isinstance(value, (int, float, bool)):
        text = str(value)
    elif isinstance(value, list):
        text = " ".join(safe_text(item, limit=limit) for item in value)
    elif isinstance(value, dict):
        # Message APIs sometimes return content blocks. Only retain display-safe leaves.
        preferred = ["text", "content", "message", "brief", "description", "value"]
        values = [value[key] for key in preferred if key in value]
        text = " ".join(safe_text(item, limit=limit) for item in values)
    else:
        text = ""
    text = re.sub(r"\s+", " ", text).strip()
    for pattern in SECRET_PATTERNS:
        text = pattern.sub("[REDACTED]", text)
    return text[:limit]


def json_log(event: str, **fields: Any) -> None:
    payload = {"timestamp": utc_now(), "event": event, **fields}
    logging.info(json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str))


def api_headers() -> dict[str, str]:
    key = os.environ.get("MANUS_API_KEY", "")
    if not key:
        raise MonitorError("MANUS_API_KEY is unavailable to the monitor service")
    return {"x-manus-api-key": key, "Content-Type": "application/json"}


def api_get(path: str, params: dict[str, Any]) -> dict[str, Any]:
    last_error: Exception | None = None
    for attempt in range(1, API_RETRY_ATTEMPTS + 1):
        try:
            response = requests.get(
                f"{API_BASE}/{path}", headers=api_headers(), params=params, timeout=REQUEST_TIMEOUT_SECONDS
            )
            response.raise_for_status()
            data = response.json()
            if not isinstance(data, dict) or data.get("ok") is not True:
                raise MonitorError(f"Manus API GET {path} returned an unsuccessful envelope")
            return data
        except (requests.RequestException, ValueError, MonitorError) as exc:
            last_error = exc
            if attempt < API_RETRY_ATTEMPTS:
                time.sleep(attempt)
    raise MonitorError(
        f"Manus API GET {path} failed after {API_RETRY_ATTEMPTS} attempts: "
        f"{safe_text(str(last_error), 220)}"
    ) from last_error


def try_enqueue_coordinator_notice(
    content: str, notice_id: str, source_task_id: str | None = None
) -> bool:
    """Attempt durable queue acceptance without failing the interval.

    Failed queue writes remain in the local durable outbox and are retried by
    later intervals. A True result means the shared queue accepted the notice;
    routing and coordinator delivery happen later.
    """
    last_error: Exception | None = None
    for attempt in range(1, API_RETRY_ATTEMPTS + 1):
        try:
            accepted = enqueue_notice(
                notice_id,
                content,
                source_task_id=source_task_id,
                kind="kez_lane_monitor",
            )
            if not accepted:
                raise MonitorError("Coordinator queue declined notice")
            json_log("coordinator_notice_enqueued", notice_id=notice_id, source_task_id=source_task_id)
            return True
        except Exception as exc:
            last_error = exc
            if attempt < API_RETRY_ATTEMPTS:
                time.sleep(attempt)
    json_log(
        "coordinator_notice_deferred",
        notice_id=notice_id,
        error=safe_text(str(last_error), 220),
    )
    return False


def task_from_detail(data: dict[str, Any]) -> dict[str, Any]:
    task = data.get("task")
    if not isinstance(task, dict):
        nested = data.get("data")
        task = nested.get("task") if isinstance(nested, dict) else None
    if not isinstance(task, dict):
        raise MonitorError("task.detail omitted task metadata")
    return task


def get_path(container: Any, path: Iterable[str]) -> Any:
    value = container
    for key in path:
        if not isinstance(value, dict):
            return None
        value = value.get(key)
    return value


def event_text(event: dict[str, Any]) -> str:
    """Extract a short, display-safe summary from known task event fields."""
    candidates = (
        ("status_update", "brief"),
        ("status_update", "description"),
        ("error_message", "content"),
        ("error_message", "message"),
        ("assistant_message", "content"),
        ("assistant_message", "message"),
        ("assistant_message", "text"),
        ("user_message", "content"),
        ("user_message", "message"),
        ("structured_output_result", "message"),
    )
    chunks: list[str] = []
    for path in candidates:
        text = safe_text(get_path(event, path), limit=350)
        if text and text not in chunks:
            chunks.append(text)
    return safe_text(" — ".join(chunks), limit=700)


def classify_material(task_status: str, agent_status: str, event_type: str, text: str) -> str | None:
    normalized_status = f"{task_status} {agent_status}".casefold()
    normalized_event = event_type.casefold()
    if any(word in normalized_status for word in ("error", "failed")) or "error" in normalized_event:
        return "error"
    if QUESTION_RE.search(text):
        return "question_or_blocker"
    if BLOCKER_RE.search(text):
        return "question_or_blocker"
    if any(status in normalized_status.split() for status in MATERIAL_STATUSES):
        return "state"
    if "assistant_message" in normalized_event and RESULT_RE.search(text):
        return "result_or_artifact"
    return None


def newest_event(messages: Any) -> dict[str, Any]:
    if not isinstance(messages, list):
        return {}
    for event in messages:
        if isinstance(event, dict):
            return event
    return {}


def observe_lane(lane: str, task_id: str) -> Observation:
    detail = api_get("task.detail", {"task_id": task_id})
    task = task_from_detail(detail)
    title = safe_text(task.get("title"), limit=300) or "Untitled task"
    task_status = safe_text(task.get("status"), limit=80).casefold() or "unknown"
    message_data = api_get("task.listMessages", {"task_id": task_id, "order": "desc", "limit": 10})
    event = newest_event(message_data.get("messages"))
    event_id = safe_text(event.get("id"), limit=160) or "no-message"
    event_type = safe_text(event.get("type"), limit=100) or "no-message"
    event_timestamp = safe_text(event.get("timestamp"), limit=100) or "unknown"
    agent_status = safe_text(get_path(event, ("status_update", "agent_status")), limit=80).casefold()
    summary = event_text(event) or f"{event_type} ({agent_status or task_status})"
    event_hash = hashlib.sha256(summary.encode("utf-8")).hexdigest()
    return Observation(
        lane=lane,
        task_id=task_id,
        title=title,
        task_status=task_status,
        agent_status=agent_status,
        event_id=event_id,
        event_type=event_type,
        event_timestamp=event_timestamp,
        event_summary=summary,
        event_hash=event_hash,
        material_kind=classify_material(task_status, agent_status, event_type, summary),
    )


def load_state() -> dict[str, Any]:
    if not STATE_PATH.exists():
        return {"version": 2, "lanes": {}, "notification_outbox": [], "last_success_at": None}
    try:
        data = json.loads(STATE_PATH.read_text(encoding="utf-8"))
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        raise MonitorError("monitor state is unreadable; refusing to overwrite evidence") from exc
    if not isinstance(data, dict) or not isinstance(data.get("lanes"), dict):
        raise MonitorError("monitor state has an invalid schema")
    if "notification_outbox" not in data:
        data["notification_outbox"] = []
    if not isinstance(data["notification_outbox"], list):
        raise MonitorError("monitor notification outbox has an invalid schema")
    return data


def write_state(state: dict[str, Any]) -> None:
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    payload = json.dumps(state, sort_keys=True, indent=2) + "\n"
    try:
        with tempfile.NamedTemporaryFile(
            mode="w", encoding="utf-8", dir=STATE_PATH.parent, prefix=".state-", delete=False
        ) as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
            staged = Path(handle.name)
        os.chmod(staged, 0o600)
        os.replace(staged, STATE_PATH)
        directory_fd = os.open(STATE_PATH.parent, os.O_DIRECTORY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    except OSError as exc:
        raise MonitorError("could not persist monitor state") from exc


def prior_state_entry(observation: Observation, prior: dict[str, Any] | None, now: int) -> dict[str, Any]:
    unchanged = bool(prior and prior.get("state_key") == observation.state_key)
    state_since = int(prior.get("state_since", now)) if unchanged and prior else now
    entry: dict[str, Any] = {
        "lane": observation.lane,
        "task_id": observation.task_id,
        "title": observation.title,
        "task_status": observation.task_status,
        "agent_status": observation.agent_status,
        "event_id": observation.event_id,
        "event_type": observation.event_type,
        "event_timestamp": observation.event_timestamp,
        "event_hash": observation.event_hash,
        "state_key": observation.state_key,
        "condition_key": observation.condition_key,
        "state_since": state_since,
        "last_observed_at": now,
        "last_notified_key": prior.get("last_notified_key") if prior else None,
        "last_notified_at": prior.get("last_notified_at") if prior else None,
        "last_stale_state_key": prior.get("last_stale_state_key") if prior else None,
        "last_error_key": None,
        "last_error_notified_key": None,
    }
    return entry


def notification_reason(observation: Observation, prior: dict[str, Any] | None, entry: dict[str, Any], now: int) -> str | None:
    # The first successful run is a durable baseline, not a retrospective
    # notification burst. Subsequent state transitions are the "new" events.
    if prior is None:
        return None
    # Legacy state files from earlier monitor revisions lack condition_key;
    # derive the equivalent status-only fingerprint rather than alerting just
    # because the durable schema gained a field.
    prior_condition_key = prior.get("condition_key") or hashlib.sha256(
        f"{prior.get('task_status', '')}|{prior.get('agent_status', '')}".encode("utf-8")
    ).hexdigest()
    condition_changed = prior_condition_key != observation.condition_key
    event_changed = (
        prior.get("event_id") != observation.event_id
        or prior.get("event_hash") != observation.event_hash
    )
    if condition_changed and observation.material_kind:
        return observation.material_kind
    if event_changed and observation.material_kind in {"error", "question_or_blocker", "result_or_artifact"}:
        return observation.material_kind
    if now - int(entry["state_since"]) >= STALE_AFTER_SECONDS:
        if entry.get("last_stale_state_key") != observation.state_key:
            return "unchanged_over_one_interval"
    return None


def action_for(reason: str, observation: Observation) -> str:
    if reason in {"error", "question_or_blocker"}:
        return "Yes — review and respond if a decision, access, or clarification is needed."
    if reason == "state":
        if observation.effective_status in {"stopped", "completed", "idle", "waiting", "paused"}:
            return "Review needed; do not dispatch or resume automatically. Monitoring continues for this reusable conversation."
        return "Review status; no automatic action is taken."
    if reason == "unchanged_over_one_interval":
        return "Review if progress is expected; no automatic intervention is taken."
    return "No immediate coordinator action indicated; result is available for review."


def render_notice(notices: list[tuple[Observation, str]]) -> str:
    lines = [
        "🔎 **KeZ lane monitor — material update**",
        "",
        f"Checked: {utc_now()} | Fixed lanes: {len(LANES)} | Read-only monitor",
        "",
    ]
    for observation, reason in notices:
        reason_label = {
            "state": "new material state",
            "error": "error state",
            "question_or_blocker": "question or blocker",
            "result_or_artifact": "new result or artifact",
            "unchanged_over_one_interval": "unchanged for more than one interval",
        }.get(reason, reason)
        lines.extend(
            (
                f"**{observation.lane}** — {observation.title}",
                f"- Status: `{observation.effective_status}` (task: `{observation.task_status}`)",
                f"- Latest meaningful event: {safe_text(observation.event_summary, 500)}",
                f"- Why notified: {reason_label}",
                f"- Coordinator action: {action_for(reason, observation)}",
                "",
            )
        )
    lines.append("The monitor does not infer KeZ completion from a stopped lane and does not create, resume, or alter lane tasks.")
    return "\n".join(lines)[:7000]


def render_query_error_notice(lane: str, task_id: str, error: str) -> str:
    return "\n".join(
        (
            "⚠️ **KeZ lane monitor — lane query issue**",
            "",
            f"Checked: {utc_now()} | The monitor did not alter any lane.",
            "",
            f"**{lane}** (`{task_id}`)",
            f"- Monitor issue: {safe_text(error, 300)}",
            "- Coordinator action: review only if the issue persists; monitoring will retry automatically.",
        )
    )


def queue_notice(outbox: list[dict[str, Any]], item: dict[str, Any]) -> bool:
    """Append one non-secret notification once; bound retained pending evidence."""
    notice_id = item["notice_id"]
    if any(existing.get("notice_id") == notice_id for existing in outbox):
        return False
    if len(outbox) >= 100:
        # Do not silently discard notification evidence. The interval remains
        # healthy and logs the durable delivery backlog for operator action.
        json_log("notification_outbox_full", pending=len(outbox), notice_id=notice_id)
        return False
    outbox.append(item)
    json_log("local_notice_outbox_queued", notice_id=notice_id, lane=item.get("lane"))
    return True


def deliver_outbox(
    outbox: list[dict[str, Any]], lanes: dict[str, dict[str, Any]], now: int
) -> tuple[list[dict[str, Any]], int]:
    """Queue pending notices without making a transient queue fault fatal."""
    remaining: list[dict[str, Any]] = []
    accepted = 0
    for item in outbox:
        notice_id = item.get("notice_id")
        content = item.get("content")
        if not isinstance(notice_id, str) or not isinstance(content, str):
            json_log("invalid_notification_outbox_item", item_type=type(item).__name__)
            remaining.append(item)
            continue
        task_id = item.get("task_id")
        source_task_id = task_id if isinstance(task_id, str) else None
        if not try_enqueue_coordinator_notice(content, notice_id, source_task_id):
            remaining.append(item)
            continue
        accepted += 1
        entry = lanes.get(task_id) if isinstance(task_id, str) else None
        if not isinstance(entry, dict):
            continue
        if item.get("kind") == "material" and entry.get("state_key") == item.get("state_key"):
            entry["last_notified_key"] = item.get("notification_key")
            entry["last_notified_at"] = now
            if item.get("reason") == "unchanged_over_one_interval":
                entry["last_stale_state_key"] = item.get("state_key")
        elif item.get("kind") == "query_error" and entry.get("last_error_key") == item.get("error_key"):
            entry["last_error_notified_key"] = item.get("notification_key")
    return remaining, accepted


def run(dry_run: bool) -> int:
    LOCK_PATH.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    with LOCK_PATH.open("a+", encoding="utf-8") as lock_handle:
        try:
            fcntl.flock(lock_handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            json_log("monitor_already_running")
            return 0
        state = load_state()
        now = int(time.time())
        next_lanes: dict[str, Any] = {}
        outbox: list[dict[str, Any]] = list(state.get("notification_outbox", []))
        enqueued_material = 0
        enqueued_query_errors = 0
        for lane, task_id in LANES:
            prior = state["lanes"].get(task_id)
            try:
                observation = observe_lane(lane, task_id)
            except MonitorError as exc:
                error = safe_text(str(exc), 220)
                error_key = hashlib.sha256(error.encode("utf-8")).hexdigest()
                entry = {
                    **(prior if isinstance(prior, dict) else {"lane": lane, "task_id": task_id}),
                    "last_observed_at": now,
                    "last_error_key": error_key,
                }
                next_lanes[task_id] = entry
                notification_key = f"query_error:{error_key}"
                if entry.get("last_error_notified_key") != notification_key:
                    queued = queue_notice(
                        outbox,
                        {
                            "notice_id": hashlib.sha256(
                                f"{task_id}|{notification_key}".encode("utf-8")
                            ).hexdigest(),
                            "kind": "query_error",
                            "lane": lane,
                            "task_id": task_id,
                            "error_key": error_key,
                            "notification_key": notification_key,
                            "content": render_query_error_notice(lane, task_id, error),
                            "queued_at": now,
                        },
                    )
                    enqueued_query_errors += int(queued)
                json_log("lane_observation_failed", lane=lane, task_id=task_id, error=error)
                continue
            entry = prior_state_entry(observation, prior if isinstance(prior, dict) else None, now)
            reason = notification_reason(observation, prior if isinstance(prior, dict) else None, entry, now)
            notification_key = f"{reason}:{observation.state_key}" if reason else None
            if reason and entry.get("last_notified_key") != notification_key:
                queued = queue_notice(
                    outbox,
                    {
                        "notice_id": hashlib.sha256(
                            f"{task_id}|{notification_key}".encode("utf-8")
                        ).hexdigest(),
                        "kind": "material",
                        "lane": lane,
                        "task_id": task_id,
                        "reason": reason,
                        "state_key": observation.state_key,
                        "notification_key": notification_key,
                        "content": render_notice([(observation, reason)]),
                        "queued_at": now,
                    },
                )
                enqueued_material += int(queued)
            next_lanes[task_id] = entry
            json_log(
                "lane_observed",
                lane=lane,
                task_id=task_id,
                task_status=observation.task_status,
                agent_status=observation.agent_status,
                event_type=observation.event_type,
                material_kind=observation.material_kind,
            )

        accepted_by_queue = 0
        if not dry_run:
            outbox, accepted_by_queue = deliver_outbox(outbox, next_lanes, now)
        next_state = {
            "version": 2,
            "last_success_at": now,
            "lanes": next_lanes,
            "notification_outbox": outbox,
        }
        if not dry_run:
            write_state(next_state)
        summary = {
            "dry_run": dry_run,
            "lanes_checked": len(LANES),
            "material_notices_enqueued": enqueued_material,
            "query_errors_enqueued": enqueued_query_errors,
            "notices_accepted_by_queue": accepted_by_queue,
            "notices_deferred": len(outbox),
            "state_written": not dry_run,
        }
        json_log("monitor_complete", **summary)
        print(json.dumps(summary, sort_keys=True))
        return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Read-only KeZ eight-lane monitor")
    parser.add_argument("--dry-run", action="store_true", help="Query and classify without notification or state writes")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    try:
        return run(args.dry_run)
    except MonitorError as exc:
        json_log("monitor_failed", error=safe_text(str(exc), 300))
        return 1
    except Exception as exc:  # Guard the timer with a non-secret failure record.
        json_log("monitor_failed_unexpected", error=safe_text(f"{type(exc).__name__}: {exc}", 300))
        return 1


if __name__ == "__main__":
    sys.exit(main())
