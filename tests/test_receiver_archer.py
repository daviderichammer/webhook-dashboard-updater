"""Offline regression tests for replaceable Archer coordinator routing."""
from __future__ import annotations

import asyncio
import hashlib
import importlib
import json
import os
import sqlite3
import sys
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import Mock, patch


# Import the production module with no inherited credential/config environment.
# The shared control module is present in the repository; handler tests patch its
# imported functions so they never read host configuration or make API calls.
with patch.dict(os.environ, {}, clear=True):
    receiver = importlib.import_module("main")


class FakeRequest:
    """Minimal signed-request stand-in for direct offline handler tests."""

    def __init__(self, body: bytes) -> None:
        self._body = body
        self.headers = {"x-webhook-signature": "offline", "x-webhook-timestamp": "1"}
        self.url = "http://testserver/webhook/manus"

    async def body(self) -> bytes:
        return self._body


def stopped_event(
    event_id: str,
    task_id: str,
    *,
    title: str = "KeZ project implementation",
    stop_reason: str = "ask",
    message: str = "Need a review.",
) -> dict:
    return {
        "event_id": event_id,
        "event_type": "task_stopped",
        "task_detail": {
            "task_id": task_id,
            "task_title": title,
            "task_url": f"https://example.invalid/tasks/{task_id}",
            "stop_reason": stop_reason,
            "message": message,
        },
    }


class ReceiverArcherTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tempdir.cleanup)
        root = Path(self.tempdir.name)
        self.event_db = root / "events.sqlite3"
        self.patchers = [
            patch.object(receiver, "WEBHOOK_EVENT_DB", str(self.event_db)),
            patch.object(receiver, "_verify_signature", return_value=True),
            # Tests fail closed if a handler accidentally escapes its mocked
            # branch and tries any production network request.
            patch.object(receiver.requests, "get", side_effect=AssertionError("network GET is forbidden")),
            patch.object(receiver.requests, "post", side_effect=AssertionError("network POST is forbidden")),
        ]
        for patcher in self.patchers:
            patcher.start()
            self.addCleanup(patcher.stop)

    def invoke(self, payload: dict):
        body = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
        return asyncio.run(receiver.manus_webhook(FakeRequest(body))), body

    def delivery_disposition(self, payload: dict, body: bytes) -> str:
        key = receiver._delivery_key(payload["event_id"], hashlib.sha256(body).hexdigest())
        conn = sqlite3.connect(self.event_db)
        try:
            row = conn.execute(
                "SELECT disposition FROM webhook_deliveries WHERE delivery_key = ?", (key,)
            ).fetchone()
        finally:
            conn.close()
        self.assertIsNotNone(row)
        return row[0]

    def test_active_and_retired_coordinator_callbacks_skip_before_downstream_work(self) -> None:
        active_id = "active-coordinator-task"
        retired_id = "retired-coordinator-task"
        blocked = Mock(side_effect=AssertionError("downstream work must not run"))
        with (
            patch.object(receiver, "is_coordinator", side_effect=lambda task_id: task_id in {active_id, retired_id}),
            patch.object(receiver, "_fetch_credits_balance", blocked),
            patch.object(receiver, "_fetch_task_membership", blocked),
            patch.object(receiver, "_dashboard_refresh_trigger_membership", blocked),
            patch.object(receiver, "_spawn_full_dashboard_update", blocked),
            patch.object(receiver, "_forward_to_steward", blocked),
        ):
            for label, task_id in (("active", active_id), ("retired", retired_id)):
                with self.subTest(identity=label):
                    payload = stopped_event(f"self-{label}", task_id, stop_reason="finish")
                    result, body = self.invoke(payload)
                    self.assertEqual(
                        result,
                        {"status": "processed", "action": "coordinator_self_callback_suppressed"},
                    )
                    self.assertEqual(
                        self.delivery_disposition(payload, body), "suppressed_coordinator_self_callback"
                    )
        blocked.assert_not_called()

    def test_new_turns_process_but_exact_retry_does_not_forward_twice(self) -> None:
        forwarded = Mock(return_value=True)
        membership = Mock(return_value={"task_type": "project", "status": "stopped"})
        with (
            patch.object(receiver, "is_coordinator", return_value=False),
            patch.object(receiver, "_fetch_credits_balance", return_value=None),
            patch.object(receiver, "_fetch_task_membership", membership),
            patch.object(receiver, "_forward_to_steward", forwarded),
        ):
            first = stopped_event("turn-1", "ordinary-project", message="First paused turn")
            duplicate = stopped_event("turn-1", "ordinary-project", message="First paused turn")
            later_turn = stopped_event("turn-2", "ordinary-project", message="Later paused turn")
            self.assertEqual(self.invoke(first)[0]["status"], "processed")
            self.assertEqual(self.invoke(duplicate)[0], {"status": "duplicate"})
            self.assertEqual(self.invoke(later_turn)[0]["status"], "processed")
        self.assertEqual(forwarded.call_count, 2)
        self.assertEqual(membership.call_count, 2)

    def test_legacy_evidence_exact_retry_is_preserved_but_reused_provider_id_with_new_body_reserves(self) -> None:
        exact_hash = "legacy-exact-body"
        conn = receiver._event_db()
        try:
            conn.execute(
                "INSERT INTO webhook_events "
                "(event_id, task_id, event_type, stop_reason, body_sha256, received_at, processed_at, status, disposition) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    "legacy-event",
                    "legacy-task",
                    "task_stopped",
                    "finish",
                    exact_hash,
                    int(time.time()),
                    int(time.time()),
                    "processed",
                    "legacy_recorded",
                ),
            )
            conn.commit()
            indexes = {row[1] for row in conn.execute("PRAGMA index_list(webhook_events)")}
        finally:
            conn.close()
        self.assertNotIn("webhook_task_event_once", indexes)
        self.assertEqual(
            receiver._reserve_event("legacy-event", "legacy-task", "task_stopped", "finish", exact_hash),
            "duplicate",
        )
        changed_hash = "legacy-event-with-changed-payload"
        self.assertEqual(
            receiver._reserve_event("legacy-event", "legacy-task", "task_stopped", "finish", changed_hash),
            "reserved",
        )
        receiver._complete_event(receiver._delivery_key("legacy-event", changed_hash))
        conn = sqlite3.connect(self.event_db)
        try:
            legacy = conn.execute(
                "SELECT body_sha256, disposition FROM webhook_events WHERE event_id = ?", ("legacy-event",)
            ).fetchone()
        finally:
            conn.close()
        self.assertEqual(legacy, (exact_hash, "legacy_recorded"))

    def test_stale_exact_delivery_lease_is_recovered_and_then_completed_by_key(self) -> None:
        event_id = "stale-delivery"
        body_hash = "stale-body"
        self.assertEqual(
            receiver._reserve_event(event_id, "task-a", "task_stopped", "ask", body_hash), "reserved"
        )
        key = receiver._delivery_key(event_id, body_hash)
        conn = sqlite3.connect(self.event_db)
        try:
            conn.execute(
                "UPDATE webhook_deliveries SET received_at = ? WHERE delivery_key = ?",
                (int(time.time()) - receiver.WEBHOOK_EVENT_LEASE_SECONDS - 1, key),
            )
            conn.commit()
        finally:
            conn.close()
        self.assertEqual(
            receiver._reserve_event(event_id, "task-a", "task_stopped", "ask", body_hash), "reserved"
        )
        receiver._complete_event(key, "stale_recovered")
        self.assertEqual(
            receiver._reserve_event(event_id, "task-a", "task_stopped", "ask", body_hash), "duplicate"
        )

    def test_routine_orphan_turo_completion_remains_suppressed_without_queue_or_dashboard(self) -> None:
        forward = Mock(side_effect=AssertionError("routine orphan Turo must not queue"))
        dashboard = Mock(side_effect=AssertionError("routine orphan Turo must not refresh dashboard"))
        with (
            patch.object(receiver, "is_coordinator", return_value=False),
            patch.object(receiver, "_fetch_credits_balance", return_value=None),
            patch.object(receiver, "_fetch_task_membership", return_value={"task_type": "standalone", "status": "stopped"}),
            patch.object(receiver, "_forward_to_steward", forward),
            patch.object(receiver, "_spawn_full_dashboard_update", dashboard),
        ):
            result, _ = self.invoke(
                stopped_event(
                    "turo-routine",
                    "orphan-turo-task",
                    title="Verify Turo host/listing and business contact",
                    stop_reason="finish",
                    message="Routine verification completed.",
                )
            )
        self.assertEqual(result["action"], "orphan_turo_dashboard_refresh_suppressed")
        forward.assert_not_called()
        dashboard.assert_not_called()

    def test_project_completion_is_queued_with_stable_delivery_notice_id(self) -> None:
        enqueued: list[tuple[str, str, str | None, str]] = []

        def record_enqueue(notice_id: str, content: str, source_task_id: str | None = None, kind: str = "event") -> bool:
            enqueued.append((notice_id, content, source_task_id, kind))
            return True

        payload = stopped_event(
            "project-completion",
            "project-task",
            title="KeZ implementation completed",
            stop_reason="finish",
            message="Implementation artifact is ready.",
        )
        with (
            patch.object(receiver, "is_coordinator", return_value=False),
            patch.object(receiver, "enqueue_notice", side_effect=record_enqueue),
            patch.object(receiver, "_fetch_credits_balance", return_value={"credits_label": "Credits remaining: 3 / 10"}),
            patch.object(receiver, "_fetch_task_membership", return_value={"task_type": "project", "status": "stopped"}),
            patch.object(
                receiver,
                "_spawn_full_dashboard_update",
                return_value={
                    "status": "dashboard_refresh_cooldown_suppressed",
                    "prior_dispatch_timestamp": 1000.0,
                    "remaining_cooldown_seconds": 1,
                    "cooldown_seconds": 600,
                },
            ) as dashboard,
        ):
            result, body = self.invoke(payload)
        key = receiver._delivery_key(payload["event_id"], hashlib.sha256(body).hexdigest())
        self.assertEqual(result["action"], "dashboard_refresh_cooldown_suppressed")
        self.assertEqual(result["dashboard_refresh"]["remaining_cooldown_seconds"], 1)
        self.assertEqual(len(enqueued), 1)
        notice_id, content, source_task_id, kind = enqueued[0]
        self.assertEqual(notice_id, f"manus-webhook:{key}")
        self.assertEqual(source_task_id, "project-task")
        self.assertEqual(kind, "task_stopped")
        self.assertIn("**Title:** KeZ implementation completed", content)
        self.assertIn("**Summary:**\nImplementation artifact is ready.", content)
        dashboard.assert_called_once()

    def test_missing_coordinator_config_fails_closed_before_downstream_work(self) -> None:
        blocked = Mock(side_effect=AssertionError("downstream work must not run"))
        payload = stopped_event("missing-config", "ordinary-task", stop_reason="finish")
        with (
            patch.object(receiver, "is_coordinator", side_effect=RuntimeError("registry unavailable")),
            patch.object(receiver, "_fetch_credits_balance", blocked),
            patch.object(receiver, "_fetch_task_membership", blocked),
            patch.object(receiver, "_spawn_full_dashboard_update", blocked),
            patch.object(receiver, "_forward_to_steward", blocked),
        ):
            result, body = self.invoke(payload)
        self.assertEqual(result.status_code, 503)
        blocked.assert_not_called()
        # The failed configuration check releases only this exact delivery, so
        # the signed provider retry can reserve it after configuration recovery.
        self.assertEqual(
            receiver._reserve_event(
                payload["event_id"],
                payload["task_detail"]["task_id"],
                payload["event_type"],
                payload["task_detail"]["stop_reason"],
                hashlib.sha256(body).hexdigest(),
            ),
            "reserved",
        )

    def test_owned_runtime_sources_have_no_direct_coordinator_send(self) -> None:
        root = Path(__file__).resolve().parents[1]
        forbidden = "task." + "sendMessage"
        for relative in (
            Path("main.py"),
            Path("kez/kez_lane_monitor.py"),
            Path("kez/kez_lane_monitor_failure.py"),
        ):
            text = (root / relative).read_text(encoding="utf-8")
            self.assertNotIn(forbidden, text, relative)
        legacy_destination = "PfG6WuV" + "PozpFfyNhBDXuGh"
        self.assertNotIn(legacy_destination, (root / "main.py").read_text(encoding="utf-8"))


if __name__ == "__main__":
    unittest.main()
