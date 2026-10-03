#!/usr/bin/env python3
"""Send a concise, non-secret coordinator notice when the KeZ monitor unit fails."""

from __future__ import annotations

import hashlib
import sys
from datetime import UTC, datetime

from archer_control import enqueue_notice


def main() -> int:
    unit = sys.argv[1] if len(sys.argv) > 1 else "kez-lane-monitor.service"
    detected_at = datetime.now(UTC)
    # One ID per unit/hour avoids duplicate systemd failure notices without
    # embedding credentials or any lane content in the durable queue key.
    notice_id = "kez-monitor-failure:" + hashlib.sha256(
        f"{unit}\0{detected_at.strftime('%Y%m%d%H')}".encode("utf-8")
    ).hexdigest()
    content = (
        "⚠️ **KeZ lane monitor service failure**\n\n"
        f"- Unit: `{unit}`\n"
        f"- Detected: {detected_at.strftime('%Y-%m-%d %H:%M:%S UTC')}\n"
        "- Effect: the most recent ten-minute observation did not complete. Existing lane tasks were not changed.\n"
        "- Coordinator action: inspect the protected Hetzner host unit/journal; the next timer interval will retry."
    )
    try:
        if not enqueue_notice(notice_id, content, kind="kez_lane_monitor_failure"):
            raise RuntimeError("coordinator queue declined notice")
    except Exception as exc:
        print(f"KeZ monitor failure notification was not queued: {type(exc).__name__}", file=sys.stderr)
        return 1
    print("KeZ monitor failure notification queued (not delivered)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
