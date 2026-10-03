import json,pathlib,sys
from unittest.mock import Mock
import pytest
sys.path.insert(0,str(pathlib.Path(__file__).resolve().parents[1]))
import archer_control as ac

OLD='OLDCOORDINATOR1234567890'
NEW='NEWCOORDINATOR1234567890'
LEGACY='LEGACYCOORDINATOR123456'

@pytest.fixture
def registry(tmp_path,monkeypatch):
    monkeypatch.setattr(ac,'CONFIG',tmp_path/'etc'/'coordinator.json')
    monkeypatch.setattr(ac,'STATE',tmp_path/'state')
    ac._save_config({'schema_version':1,'active_task_id':OLD,'retired_task_ids':[LEGACY],
                    'delivery_paused':False,'project_id':'project-1','memory_repo':'private-memory','history':[]})
    return tmp_path

def test_hot_pointer_and_retired_suppression(registry,monkeypatch):
    monkeypatch.setattr(ac,'_task',lambda _: {'task_type':'project','project_id':'project-1'})
    ac.switch_task(NEW,OLD,'replacement test')
    assert ac.load_config()['active_task_id']==NEW
    assert all(ac.is_coordinator(t) for t in [OLD,NEW,LEGACY])
    assert not ac.is_coordinator('worker-task')

def test_compare_and_swap_rejects_stale_operator(registry,monkeypatch):
    monkeypatch.setattr(ac,'_task',lambda _: pytest.fail('Should not call API'))
    with pytest.raises(ac.ControlError,match='Registry changed'):ac.switch_task(NEW,'wrong','test')

def test_bad_task_not_switched(registry,monkeypatch):
    monkeypatch.setattr(ac,'_task',lambda _: {'task_type':'standard'})
    with pytest.raises(ac.ControlError):ac.switch_task(NEW,OLD,'test')
    assert ac.load_config()['active_task_id']==OLD

def test_wrong_project_not_switched(registry,monkeypatch):
    monkeypatch.setattr(ac,'_task',lambda _: {'task_type':'project','project_id':'other'})
    with pytest.raises(ac.ControlError):ac.switch_task(NEW,OLD,'test')

def test_queue_is_idempotent_and_survives_swap(registry,monkeypatch):
    ac.enqueue_notice('notice1','Observation','worker-task')
    ac.enqueue_notice('notice1','Observation','worker-task')
    assert ac.status()['pending_notices']==1
    monkeypatch.setattr(ac,'_task',lambda _: {'task_type':'project'})
    ac.switch_task(NEW,OLD,'test')
    monkeypatch.setattr(ac,'_idle_state',lambda _: 'stopped')
    sent=[]
    monkeypatch.setattr(ac,'_request',lambda *args,**kwargs:sent.append(kwargs['payload']) or {'ok':True})
    assert ac.dispatch_once()['status']=='api_accepted'
    assert sent[0]['task_id']==NEW
    assert 'notice1' in sent[0]['message']['content']
    assert ac.status()['pending_notices']==0
    assert ac.status()['last_api_acceptance']['delivered_to']==NEW

def test_backlog_is_cleared_without_api_call(registry,monkeypatch):
    ac.enqueue_notice('first','First observation')
    ac.enqueue_notice('second','Second observation')
    c=ac._db()
    with c:
        c.execute('UPDATE notices SET next_attempt_at=?',(9999999999,))
    c.close()
    monkeypatch.setattr(ac,'_request',lambda *a,**k:pytest.fail('No API while clearing backlog'))
    assert ac.dispatch_once()=={'status':'backlog_cleared','cleared_notices':2}
    assert ac.status()['pending_notices']==0

def test_empty_queue_never_polls_or_prompts(registry,monkeypatch):
    monkeypatch.setattr(ac,'_request',lambda *a,**k:pytest.fail('No API on empty queue'))
    assert ac.dispatch_once()['status']=='empty'

@pytest.mark.parametrize('state',['running','waiting','error','unknown'])
def test_non_idle_does_not_send_or_approve(registry,monkeypatch,state):
    ac.enqueue_notice('n','observation')
    monkeypatch.setattr(ac,'_idle_state',lambda _:state)
    monkeypatch.setattr(ac,'_request',lambda *a,**k:pytest.fail('No send while not idle'))
    assert ac.dispatch_once()['status']=='lane_'+state
    assert ac.status()['pending_notices']==1

def test_pause_keeps_pending(registry,monkeypatch):
    ac.enqueue_notice('n','observation');ac.pause_delivery(True)
    monkeypatch.setattr(ac,'_request',lambda *a,**k:pytest.fail('No API while paused'))
    assert ac.dispatch_once()['status']=='delivery_paused'
    assert ac.status()['pending_notices']==1

def test_paused_backlog_is_cleared_without_api_call(registry,monkeypatch):
    ac.enqueue_notice('first','First observation')
    ac.enqueue_notice('second','Second observation')
    ac.pause_delivery(True)
    monkeypatch.setattr(ac,'_request',lambda *a,**k:pytest.fail('No API while paused'))
    assert ac.dispatch_once()=={'status':'backlog_cleared','cleared_notices':2}
    assert ac.status()['pending_notices']==0

def test_operator_can_clear_pending_notices(registry,monkeypatch):
    ac.enqueue_notice('first','First observation')
    monkeypatch.setattr(ac,'_request',lambda *a,**k:pytest.fail('Operator clear must not call API'))
    assert ac.clear_pending()=={'status':'pending_cleared','cleared_notices':1}
    assert ac.status()['pending_notices']==0

def test_api_failure_keeps_queue_with_backoff(registry,monkeypatch):
    ac.enqueue_notice('n','observation')
    monkeypatch.setattr(ac,'_idle_state',lambda _:'stopped')
    def fail(*a,**k):raise ac.ControlError('Manus API HTTP 503')
    monkeypatch.setattr(ac,'_request',fail)
    assert ac.dispatch_once()['status']=='retry_pending'
    c=ac._db();r=c.execute('SELECT * FROM notices').fetchone();c.close()
    assert r['delivered_at'] is None and r['attempts']==1 and r['next_attempt_at']>0

def test_ok_false_is_not_acceptance(monkeypatch):
    monkeypatch.setenv('MANUS_API_KEY','fake-test-credential')
    monkeypatch.setattr(ac.requests,'request',lambda *a,**k:Mock(ok=True,json=lambda:{'ok':False}))
    with pytest.raises(ac.ControlError,match='acknowledge'):ac._request('POST','task.sendMessage')

def test_no_config_fails_closed(registry):
    ac.CONFIG.unlink()
    with pytest.raises(ac.ControlError):ac.is_coordinator(OLD)

def test_self_notice_is_not_enqueued(registry):
    assert ac.enqueue_notice('self','observation',OLD)
    assert ac.status()['pending_notices']==0

def test_swap_preserves_paused_state(registry,monkeypatch):
    ac.pause_delivery(True)
    monkeypatch.setattr(ac,'_task',lambda _: {'task_type':'project'})
    ac.switch_task(NEW,OLD,'test')
    assert ac.load_config()['delivery_paused']

def test_nested_message_envelope(monkeypatch):
    monkeypatch.setattr(ac,'_request',lambda *a,**k:{'ok':True,'data':{'messages':[{'type':'status_update','status_update':{'agent_status':'stopped'}}]}})
    assert ac._idle_state(OLD)=='stopped'
