import asyncio,json
from unittest.mock import patch,Mock
from test_receiver_archer import receiver,FakeRequest,stopped_event

def test_missing_signature_rejected_before_registry_or_database():
    request=FakeRequest(b'{}');request.headers={}
    blocked=Mock(side_effect=AssertionError('Downstream must not run'))
    with patch.object(receiver,'_reserve_event',blocked),patch.object(receiver,'is_coordinator',blocked):
        assert asyncio.run(receiver.manus_webhook(request)).status_code==401
    blocked.assert_not_called()

def test_invalid_signature_rejected_before_registry_or_database():
    blocked=Mock(side_effect=AssertionError('Downstream must not run'))
    with patch.object(receiver,'_verify_signature',return_value=False),patch.object(receiver,'_reserve_event',blocked),patch.object(receiver,'is_coordinator',blocked):
        assert asyncio.run(receiver.manus_webhook(FakeRequest(b'{}'))).status_code==401
    blocked.assert_not_called()

def test_dashboard_refresh_does_not_spawn_more_dashboard_work(tmp_path):
    payload=stopped_event('refresh-complete','dashboard-worker',title='Dashboard Full Refresh (manual)',stop_reason='finish')
    blocked=Mock(side_effect=AssertionError('Recursive dashboard spawn forbidden'))
    with patch.object(receiver,'WEBHOOK_EVENT_DB',str(tmp_path/'events.sqlite3')),patch.object(receiver,'_verify_signature',return_value=True),patch.object(receiver,'is_coordinator',return_value=False),patch.object(receiver,'_fetch_credits_balance',return_value=None),patch.object(receiver,'_fetch_task_membership',return_value={'task_type':'project','status':'stopped'}),patch.object(receiver,'_dashboard_refresh_trigger_membership',return_value=None),patch.object(receiver,'_forward_to_steward',return_value=True),patch.object(receiver,'_spawn_full_dashboard_update',blocked):
        result=asyncio.run(receiver.manus_webhook(FakeRequest(json.dumps(payload).encode())))
        assert result['action']=='coordinator_queued'
    blocked.assert_not_called()
