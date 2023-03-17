#!/usr/bin/env python3

import urllib.parse
import contextlib
import pytest
import sys
import os
apifolder = os.getcwd()
sys.path.append(apifolder)
from functions import POST, GET, DELETE, SSH_TEST, wait_on_job
from auto_config import (
    ip,
    dev_test,
    pool_name,
    password,
    user,
)
from protocols import SMB
from pytest_dependency import depends
from utils import create_dataset

reason = 'Skipping for test development testing'
# comment pytestmark for development testing with --dev-test
pytestmark = pytest.mark.skipif(dev_test, reason=reason)

SMB_USER = "smbrpcuser"
SMB_PWD = "smb1234"

@contextlib.contextmanager
def smb_share(path, options=None):
    results = POST("/sharing/smb/", {
        "path": path,
        **(options or {}),
    })
    assert results.status_code == 200, results.text
    id = results.json()["id"]

    try:
        yield id
    finally:
        result = DELETE(f"/sharing/smb/id/{id}/")
        assert result.status_code == 200, result.text

    assert results.status_code == 200, results.text
    global next_uid
    next_uid = results.json()


@pytest.fixture(scope="module")
def start_smb(request):
    results = POST("/service/start/", {"service": "cifs"})
    assert results.status_code == 200, results.text

    results = GET("/service?service=cifs")
    assert results.json()[0]["state"] == "RUNNING", results.text

    try:
        yield request

    finally:
        results = POST("/service/stop/", {"service": "cifs"})
        assert results.status_code == 200, results.text


@pytest.mark.dependency(name="SMB_USER_CREATED")
def test_002_creating_shareuser_to_test_rpc(start_smb):
    results = GET('/user/get_next_uid/')
    assert results.status_code == 200, results.text
    global new_uid
    new_id = results.json()

    global new_smbuser_id
    payload = {
        "username": SMB_USER,
        "full_name": "SMB User",
        "group_create": True,
        "password": SMB_PWD,
        "uid": new_id,
    }
    results = POST("/user/", payload)
    assert results.status_code == 200, results.text
    new_smbuser_id = results.json()


def test_003_test_share_enum(request):
    depends(request, ["SMB_USER_CREATED", "pool_04"], scope="session")

    ds = 'rpc_test'
    path = f'/mnt/{pool_name}/{ds}'
    with create_dataset(f'{pool_name}/{ds}', {'share_type': 'SMB'}):
        with smb_share(path, {"name": "RPC_TEST"}):
            with MS_RPC(username=SMB_USER, password=SMB_PWD, host=ip) as hdl:
                shares = hdl.shares()
                # IPC$ share should always be present
                assert len(shares) == 2, str(shares)


def test_099_delete_smb_user(request):
    depends(request, ["SMB_USER_CREATED"])
    results = DELETE(f"/user/id/{smbuser_id}/", {"delete_group": True})
    assert results.status_code == 200, results.text
