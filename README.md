# Manus callbacks → replaceable Archer coordinator

The existing FastAPI receiver stays at `https://sms.reademail.ai/webhook/manus` on Hetzner. It verifies signed task callbacks, retains Turo/orphan suppression and dashboard cooldown behavior, and queues project-task observations for Archer. It no longer embeds a coordinator task ID.

## Identity and memory

| Concern | Location |
|---|---|
| Live pointer | `/etc/archer/coordinator.json` |
| Durable feedback | `/var/lib/archer/outbox.sqlite3` |
| Management memory | Private `daviderichammer/the-project-project`, `archer/` |
| Shared routing module | `/opt/archer-control/archer_control.py` |
| Operator CLI | `/usr/local/bin/archerctl` |

The callback receiver and both KeZ producers import the same module through a systemd `PYTHONPATH` drop-in. Active and retired coordinators are excluded from callback forwarding and dashboard work. A new lane requires a pointer change, not source edits.

```sh
sudo archerctl status
sudo archerctl pause
sudo archerctl switch NEW_TASK_ID --expect CURRENT_TASK_ID --reason 'Replace unhealthy lane'
sudo archerctl resume
```

Switching validates the task through the API, compares the expected current pointer, records retirement/history and writes atomically. It preserves paused state and uses a lock to serialize against an in-flight send. It does not stop the old task or already-dispatched workers. See `archer/REPLACEMENT.md` in the knowledge repo.

## Delivery

`archer-delivery.timer` checks once per minute. Empty queue means no Manus API request and no AI wakeup. Pending notices are batched only into a lane whose latest state is `stopped`. Running, waiting, error and unknown states retain the queue. The dispatcher never creates a lane, answers a question, confirms an action, or manufactures keepalive work.

API acceptance is recorded with the recipient, but is not proof of processing. Transport ambiguity can cause retries: delivery is at-least-once, and notice IDs must be deduplicated by Archer's durable action ledger. Undelivered notices follow a replacement lane; previously accepted ones need reconciliation, not blind replay.

## Event evidence and Turo preservation

The legacy `webhook_events` table remains as historical evidence. The incorrect task/type/reason unique index is removed. New `webhook_deliveries` uses the provider event ID plus payload hash, admitting new turns while rejecting exact retries and recognizing old exact retries. Processing leases can be reclaimed after five minutes on retry.

The deployed orphan-Turo suppression patch, signature verification, task-membership lookup, dashboard filters and ten-minute cooldown are preserved. The Turo worker and replenisher are not part of this deployment. KeZ is observational and remains paused during Archer activation.

## Deployment and rollback

Use the existing receiver virtual environment. `deployment/` contains the queue service, timer and CLI wrapper; the wrapper and queue service reference the existing protected `/etc/manus-webhook.env`, never embedded credentials. Create `/etc/archer` and `/var/lib/archer` with mode 0700, and registry/database files with mode 0600.

Stage and test separately first. Backup live source, unit overrides and the SQLite ledger consistently before restarting **only** `manus-webhook.service`. Add `Environment=PYTHONPATH=/opt/archer-control` to receiver/KeZ units; allow only `/var/lib/archer` as an additional writable path in KeZ's existing `ProtectSystem=strict` sandbox. Do not invoke the old generic `deploy.sh` blindly or overwrite a live protection patch.

For routing trouble, `archerctl pause` preserves intake and queued observations without touching Turo. For code rollback, preserve both current databases, restore the backed-up three Python source files and prior unit overrides, reload systemd and restart only the receiver. Restore historical routing only deliberately; it points to the old Telegram task. A same-host backup protects rollback, not total-host loss.

## Tests

```sh
python -m pytest -q
```

The offline suite covers pointer replacement and stale-operator rejection, queue survival/dedup, active/retired self-callback exclusion, new turns vs retries, legacy event evidence, Turo suppression, dashboard non-recursion, signature rejection, paused/busy/waiting/error states, API `ok:false`, and retry retention. It sends no live API request.
