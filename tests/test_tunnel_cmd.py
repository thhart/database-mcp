"""Unit tests for the ssh command line (no ssh connection needed)."""

from database_mcp.tunnel import build_cmd


def test_keepalive_and_lifecycle_options():
    cmd = build_cmd("user@jump", 15432, "127.0.0.1", 5432, keepalive=120.0)
    joined = " ".join(cmd)
    assert "ServerAliveInterval=120" in joined
    assert "ServerAliveCountMax=1" in joined
    assert "ControlMaster=no" in joined  # multiplexing would break lifecycle
    assert "ControlPath=none" in joined
    assert "BatchMode=yes" in joined
    assert "ExitOnForwardFailure=yes" in joined
    assert "-L 127.0.0.1:15432:127.0.0.1:5432" in joined
    assert cmd[-1] == "user@jump"


def test_keepalive_floor():
    cmd = build_cmd("h", 1, "127.0.0.1", 5432, keepalive=0.2)
    assert "ServerAliveInterval=1" in " ".join(cmd)
