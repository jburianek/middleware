from time import sleep

import pytest
from pytest_dependency import depends

from config import CLUSTER_INFO, CLUSTER_IPS, TIMEOUTS
from utils import make_request, make_ws_request, wait_on_job
from exceptions import JobTimeOut

IGNORE = ['ctdb_shared_vol']  # TODO: dont hardcode this


@pytest.mark.dependency(name='STOP_GVOLS')
def test_stop_all_gvols():
    return
    ans = make_request('get', '/gluster/volume')
    assert ans.status_code == 200, ans.text

    # we will try to stop each gluster volume `retries` times waiting at least
    # 1 second between each attempt
    retries = TIMEOUTS['FUSE_OP_TIMEOUT']
    sleeptime = 1
    for i in filter(lambda x: x['status'] == 'Started' and x['name'] not in IGNORE, ans.json()):
        gvol = i['name']
        for retry in range(retries):
            stop = make_request('post', '/gluster/volume/stop', data={'name': gvol, 'force': True})
            if stop.status_code == 422:
                if 'Another transaction is in progress' in stop.text:
                    # another command is running in the cluster so this isn't fatal but expected
                    # so we'll sleep for `sleeptime` seconds to backoff and let cluster settle
                    sleep(sleeptime)
                else:
                    assert False, f'Failed to stop {gvol!r}: {stop.text}'
            elif stop.status_code == 200:
                break
            elif retry == retries:
                assert False, f'Retried {retries} times to stop {gvol!r} but failed.'


@pytest.mark.parametrize('ip', CLUSTER_IPS)
@pytest.mark.dependency(name='VERIFY_FUSE_UMOUNTED')
def test_verify_all_gvols_are_fuse_umounted(ip, request):
    return
    depends(request, ['STOP_GVOLS'])

    ans = make_request('get', '/gluster/volume/list')
    assert ans.status_code == 200, ans.text

    # we will try to check if each FUSE mountpoint is umounted `retries` times waiting at
    # least `sleeptime` second between each attempt
    retries = TIMEOUTS['FUSE_OP_TIMEOUT']
    sleeptime = 1
    for i in filter(lambda x: x not in IGNORE, ans.json()):
        for retry in range(retries):
            # give each node a little time to actually umount the fuse volume before we claim failure
            rv = make_request('post', f'http://{ip}/api/v2.0/gluster/fuse/is_mounted', data={'name': i})
            assert rv.status_code == 200
            if not rv.json():
                break

            sleep(sleeptime)
            assert retry != retries, f'Waited {retries} seconds on FUSE mount for {i!r} to become umounted.'


@pytest.mark.dependency(name='DELETE_GVOLS')
def test_delete_gvols(request):
    return
    depends(request, ['VERIFY_FUSE_UMOUNTED'])

    ans = make_request('get', '/gluster/volume/list')
    assert ans.status_code == 200, ans.text

    for i in filter(lambda x: x not in IGNORE, ans.json()):
        delete = make_request('delete', f'/gluster/volume/id/{i}')
        assert delete.status_code == 200, delete.text
        try:
            status = wait_on_job(delete.json(), CLUSTER_INFO['NODE_A_IP'], 120)
        except JobTimeOut:
            assert False, f'Timed out waiting for {i!r} to be deleted'
        else:
            assert status['state'] == 'SUCCESS', status


@pytest.mark.dependency(name='CTDB_VOL_DELETE')
def test_delete_ctdb_shared_vol(request):
    return
    depends(request, ['DELETE_GVOLS'])

    payload = {'msg': 'method', 'method': 'ctdb.shared.volume.delete'}
    ip = CLUSTER_INFO['NODE_A_IP']
    ans = make_ws_request(ip, payload)
    assert ans.get('error') is None, ans
    assert isinstance(ans['result'], int), ans
    try:
        status = wait_on_job(ans['result'], ip, 120)
    except JobTimeOut:
        assert False, f'Timed out waiting for ctdb shared volume to be deleted on {ip!r}'
    else:
        assert status['state'] == 'SUCCESS', status


@pytest.mark.parametrize('ip', CLUSTER_IPS)
@pytest.mark.dependency(name='VERIFY_CTDB_UNMOUNTED')
def test_verify_ctdb_shared_vol_is_umounted(ip, request):
    return
    depends(request, ['CTDB_VOL_DELETE'])
    # we will try to check if each FUSE mountpoint is umounted `retries` times waiting at
    # least `sleeptime` second between each attempt
    retries = TIMEOUTS['FUSE_OP_TIMEOUT']
    sleeptime = 1
    ip = CLUSTER_INFO['NODE_A_IP']
    ctdb = IGNORE[0]
    for retry in range(retries):
        # give each node a little time to actually umount the fuse volume before we claim failure
        rv = make_request('post', f'http://{ip}/api/v2.0/gluster/fuse/is_mounted', data={'name': ctdb})
        assert rv.status_code == 200
        if not rv.json():
            break

        sleep(sleeptime)
        assert retry != retries, f'Waited {retries} seconds for /cluster/{ctdb} to become umounted.'


@pytest.mark.parametrize('ip', CLUSTER_IPS)
@pytest.mark.dependency(name='CTDB_TEARDOWN')
def test_ctdb_shared_vol_teardown(ip, request):
    return
    depends(request, ['VERIFY_CTDB_UNMOUNTED'])

    payload = {'msg': 'method', 'method': 'ctdb.shared.volume.teardown'}
    ans = make_ws_request(ip, payload)
    assert ans.get('error') is None, ans
    assert isinstance(ans['result'], int), ans
    try:
        status = wait_on_job(ans['result'], ip, 120)
    except JobTimeOut:
        assert False, f'Timed out waiting for ctdb shared volume to be torn down on {ip!r}'
    else:
        assert status['state'] == 'SUCCESS', status


@pytest.mark.parametrize('ip', CLUSTER_IPS)
def test_verify_ctdb_teardown(ip, request):
    return
    depends(request, ['CTDB_TEARDOWN'])

    payload = {'msg': 'method', 'method': 'cluster.utils.state_to_be_removed'}
    ans = make_ws_request(ip, payload)
    assert ans.get('error') is None, ans
    assert isinstance(ans['result'], list), ans

    files, dirs = ans['result']
    payload = {'msg': 'method', 'method': 'filesystem.stat'}
    for _file in files:
        payload.update({'params': [_file]})
        ans = make_ws_request(ip, payload)
        assert ans.get('error'), ans
        assert '[ENOENT]' in ans['error']['reason']

    payload = {'msg': 'method', 'method': 'filesystem.listdir'}
    for _dir in dirs:
        payload.update({'params': [_dir]})
        ans = make_ws_request(ip, payload)
        assert ans.get('error') is None, ans
        assert len(ans['result']) == 0, ans['result']
