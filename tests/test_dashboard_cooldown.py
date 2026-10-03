"""Focused offline tests for durable dashboard-refresh cooldown behavior."""
from __future__ import annotations

import concurrent.futures
import json
import os
import subprocess
import sys
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from test_receiver_archer import receiver


class DashboardCooldownTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tempdir.cleanup)
        self.root = Path(self.tempdir.name)
        self.event_db = self.root / "events.sqlite3"
        self.db_patcher = patch.object(receiver, "WEBHOOK_EVENT_DB", str(self.event_db))
        self.db_patcher.start()
        self.addCleanup(self.db_patcher.stop)
        # Initialize the schema before concurrent attempts race for the cooldown row.
        receiver.dashboard_refresh_cooldown_status(now=0.0)

    @staticmethod
    def successful_response(task_id: str) -> Mock:
        response = Mock()
        response.raise_for_status.return_value = None
        response.json.return_value = {"ok": True, "data": {"task_id": task_id}}
        return response

    def spawn_at(self, timestamp: float, task_id: str = "refresh-task") -> tuple[dict, Mock]:
        post = Mock(return_value=self.successful_response(task_id))
        with patch.object(receiver.time, "time", return_value=timestamp), patch.object(
            receiver.requests, "post", post
        ):
            result = receiver._spawn_full_dashboard_update("Eligible project completion")
        return result, post

    def test_first_eligible_completion_dispatches_normally(self) -> None:
        result, post = self.spawn_at(1000.0, "refresh-first")
        self.assertEqual(result["status"], "dashboard_refresh_triggered")
        self.assertEqual(result["dispatch_timestamp"], 1000.0)
        self.assertEqual(result["dashboard_task_id"], "refresh-first")
        self.assertEqual(post.call_count, 1)

    def test_599_second_completion_is_suppressed_without_task_create(self) -> None:
        self.spawn_at(1000.0, "refresh-first")
        post = Mock(side_effect=AssertionError("suppressed callback must not create a task"))
        with patch.object(receiver.time, "time", return_value=1599.0), patch.object(
            receiver.requests, "post", post
        ):
            result = receiver._spawn_full_dashboard_update("Second eligible completion")
        self.assertEqual(result["status"], "dashboard_refresh_cooldown_suppressed")
        self.assertEqual(result["prior_dispatch_timestamp"], 1000.0)
        self.assertEqual(result["remaining_cooldown_seconds"], 1)
        self.assertEqual(result["cooldown_seconds"], 600)
        post.assert_not_called()

    def test_600_second_completion_is_eligible(self) -> None:
        self.spawn_at(1000.0, "refresh-first")
        result, post = self.spawn_at(1600.0, "refresh-second")
        self.assertEqual(result["status"], "dashboard_refresh_triggered")
        self.assertEqual(result["dispatch_timestamp"], 1600.0)
        self.assertEqual(result["dashboard_task_id"], "refresh-second")
        self.assertEqual(post.call_count, 1)

    def test_concurrent_attempts_create_exactly_one_dashboard_task(self) -> None:
        attempts = 8
        barrier = threading.Barrier(attempts)
        post = Mock(return_value=self.successful_response("refresh-concurrent"))

        def attempt(index: int) -> dict:
            barrier.wait(timeout=5)
            return receiver._spawn_full_dashboard_update(f"Concurrent completion {index}")

        with patch.object(receiver.time, "time", return_value=1000.0), patch.object(
            receiver.requests, "post", post
        ), concurrent.futures.ThreadPoolExecutor(max_workers=attempts) as executor:
            results = list(executor.map(attempt, range(attempts)))

        self.assertEqual(
            sum(result["status"] == "dashboard_refresh_triggered" for result in results), 1
        )
        suppressed = [
            result for result in results
            if result["status"] == "dashboard_refresh_cooldown_suppressed"
        ]
        self.assertEqual(len(suppressed), attempts - 1)
        self.assertTrue(all(result["prior_dispatch_timestamp"] == 1000.0 for result in suppressed))
        self.assertTrue(all(result["remaining_cooldown_seconds"] == 600 for result in suppressed))
        self.assertEqual(post.call_count, 1)

    def test_cooldown_persists_across_fresh_process_import(self) -> None:
        self.assertEqual(
            receiver._reserve_dashboard_refresh(now=1000.0)["status"],
            "dashboard_refresh_reserved",
        )
        source_root = Path(__file__).resolve().parents[1]
        environment = os.environ.copy()
        environment["MANUS_WEBHOOK_EVENT_DB"] = str(self.event_db)
        environment["PYTHONPATH"] = str(source_root)
        output = subprocess.check_output(
            [
                sys.executable,
                "-c",
                "import json, main; print(json.dumps(main._reserve_dashboard_refresh(now=1001.0), sort_keys=True))",
            ],
            cwd=source_root,
            env=environment,
            text=True,
        )
        result = json.loads(output)
        self.assertEqual(result["status"], "dashboard_refresh_cooldown_suppressed")
        self.assertEqual(result["prior_dispatch_timestamp"], 1000.0)
        self.assertEqual(result["remaining_cooldown_seconds"], 599)

    def test_suppressed_callbacks_do_not_form_a_delayed_backlog(self) -> None:
        post = Mock(return_value=self.successful_response("refresh-first"))
        with patch.object(receiver.time, "time", return_value=1000.0), patch.object(
            receiver.requests, "post", post
        ):
            self.assertEqual(
                receiver._spawn_full_dashboard_update("First eligible completion")["status"],
                "dashboard_refresh_triggered",
            )
        with patch.object(receiver.time, "time", return_value=1001.0), patch.object(
            receiver.requests, "post", post
        ):
            self.assertEqual(
                receiver._spawn_full_dashboard_update("Suppressed completion")["status"],
                "dashboard_refresh_cooldown_suppressed",
            )
        with patch.object(receiver.time, "time", return_value=1600.0):
            status = receiver.dashboard_refresh_cooldown_status()
        self.assertEqual(status["status"], "dashboard_refresh_cooldown_ready")
        self.assertEqual(post.call_count, 1)


if __name__ == "__main__":
    unittest.main()
