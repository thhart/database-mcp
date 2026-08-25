"""SSH tunnel integration tests — need passwordless `ssh localhost`."""

import subprocess

import pytest

from conftest import DSN


def _ssh_localhost_works() -> bool:
    try:
        r = subprocess.run(
            ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=3", "localhost", "true"],
            capture_output=True,
            timeout=8,
        )
        return r.returncode == 0
    except Exception:
        return False


pytestmark = pytest.mark.skipif(
    not _ssh_localhost_works(), reason="no passwordless ssh to localhost"
)


def _add_tunnel_profile(db, name="tunneled", **extra):
    return db.dispatch(
        "profile_add",
        {"name": name, "dsn": DSN, "ssh_host": "localhost", **extra},
    )


def test_profile_add_via_tunnel(make_db):
    db = make_db()
    r = _add_tunnel_profile(db)
    assert "error" not in r, r
    assert "PostgreSQL" in r["server"]
    assert r["tunnel"]["alive"] is True
    assert r["tunnel"]["remote"] == "127.0.0.1:5432"
    # local port of the tunnel differs from the real server port
    assert r["tunnel"]["local_port"] != 5432


def test_query_and_paging_through_tunnel(make_db):
    db = make_db()
    _add_tunnel_profile(db)
    r = db.dispatch(
        "query",
        {"sql": "select g from generate_series(1,50) g", "page_size": 20, "profile": "tunneled"},
    )
    assert r["page"]["returned"] == 20 and r["page"]["has_more"] is True
    r2 = db.dispatch("fetch", {"cursor": r["page"]["cursor"], "page_size": 40})
    assert r2["page"]["returned"] == 30 and r2["page"]["has_more"] is False


def test_tunnel_death_invalidates_cursors_and_recovers(make_db):
    db = make_db()
    _add_tunnel_profile(db)
    r = db.dispatch(
        "query",
        {"sql": "select g from generate_series(1,100) g", "page_size": 10, "profile": "tunneled"},
    )
    cursor = r["page"]["cursor"]

    db.engines["tunneled"].tunnel.proc.kill()
    db.engines["tunneled"].tunnel.proc.wait(timeout=5)

    r = db.dispatch("fetch", {"cursor": cursor})
    assert "error" in r and "tunnel" in r["error"]

    # next query transparently rebuilds engine + tunnel
    r = db.dispatch("query", {"sql": "select 7", "profile": "tunneled"})
    assert r["rows"] == [[7]]
    assert db.engines["tunneled"].tunnel.alive()


def test_bad_ssh_host_rolls_back_profile(make_db):
    db = make_db()
    r = _add_tunnel_profile(db, name="broken", ssh_host="nonexistent-host-zz9")
    assert "error" in r
    assert "broken" not in db.store.profiles


def test_profile_test_through_tunnel(make_db):
    db = make_db()
    _add_tunnel_profile(db)
    r = db.dispatch("profile_test", {"name": "tunneled"})
    assert "PostgreSQL" in r["server"]
    assert r["tunnel"]["alive"] is True


def test_ssh_profile_persisted_with_ssh_block(make_db):
    db = make_db()
    _add_tunnel_profile(db)
    from database_mcp.profiles import ProfileStore

    reloaded = ProfileStore(db.store.path)
    assert reloaded.profiles["tunneled"]["ssh"] == {"host": "localhost"}
