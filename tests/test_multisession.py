"""Multi-session behavior: shared profiles file (stdio mode) and the
HTTP bridge (one daemon, many clients)."""

import asyncio
import json
import os
import socket
import subprocess
import sys
import time

import pytest

from conftest import DSN
from database_mcp.profiles import ProfileStore
from database_mcp.server import Limits, Manager


def test_profile_store_reload_across_instances(tmp_path):
    path = tmp_path / "profiles.json"
    a = ProfileStore(path)
    b = ProfileStore(path)
    time.sleep(0.02)  # ensure a distinguishable mtime
    a.upsert("shared", "postgresql://x@h/db", description="from session A")
    changed = b.maybe_reload()
    assert changed == ["shared"]
    assert "shared" in b.profiles
    assert b.maybe_reload() == []  # no change -> no reload


def test_manager_sees_profiles_from_other_session(seed, tmp_path):
    path = tmp_path / "profiles.json"
    store_a = ProfileStore(path)
    store_a.upsert("test", DSN, allow_writes=False, make_default=True)
    mgr = Manager(ProfileStore(path), Limits())
    try:
        # session A adds a profile AFTER manager B started
        store_a.upsert("late", DSN, description="added by other session")
        time.sleep(0.02)
        r = mgr.dispatch("query", {"sql": "select 1", "profile": "late"})
        assert r["rows"] == [[1]]
    finally:
        mgr.shutdown()


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.mark.anyio
async def test_http_bridge_shared_state(tmp_path):
    from mcp import ClientSession
    from mcp.client.streamable_http import streamable_http_client

    port = _free_port()
    proc = subprocess.Popen(
        [
            sys.executable, "-m", "database_mcp.server",
            "--http", "--port", str(port),
            "--profiles", str(tmp_path / "profiles.json"),
            "--log-days", "0",
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        text=True,
        env={**os.environ, "PYTHONPATH": os.path.dirname(os.path.dirname(__file__))},
    )
    try:
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            if proc.poll() is not None:
                pytest.fail(f"daemon died: {proc.stderr.read()[:500]}")
            try:
                with socket.create_connection(("127.0.0.1", port), timeout=0.3):
                    break
            except OSError:
                await asyncio.sleep(0.15)
        url = f"http://127.0.0.1:{port}/mcp"

        # client 1 adds a profile through the bridge
        async with streamable_http_client(url) as streams:
            r, w, *_ = streams
            async with ClientSession(r, w) as s1:
                await s1.initialize()
                res = await s1.call_tool(
                    "profile_add",
                    {"name": "bridged", "dsn": "postgresql://x@nowhere/db", "test": False},
                )
                assert json.loads(res.content[0].text)["saved"] == "bridged"

        # client 2 (a DIFFERENT session) sees it instantly
        async with streamable_http_client(url) as streams:
            r, w, *_ = streams
            async with ClientSession(r, w) as s2:
                await s2.initialize()
                res = await s2.call_tool("profiles", {})
                names = {p["name"] for p in json.loads(res.content[0].text)["profiles"]}
                assert "bridged" in names
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()


@pytest.fixture
def anyio_backend():
    return "asyncio"
