import json
import os
import time

from conftest import DSN
from database_mcp.profiles import ProfileStore
from database_mcp.qlog import QueryLog
from database_mcp.server import Limits, Manager


def make_logged_db(tmp_path, seed_marker, days=14):
    store = ProfileStore(tmp_path / "profiles.json")
    store.upsert("test", DSN, allow_writes=False, make_default=True)
    qlog = QueryLog(str(tmp_path / "log"), days=days)
    return Manager(store, Limits(), query_log=qlog)


def read_log(tmp_path):
    d = tmp_path / "log"
    entries = []
    for name in sorted(os.listdir(d)):
        if name.endswith(".jsonl"):
            with open(d / name) as f:
                entries += [json.loads(line) for line in f]
    return entries


def test_query_logged_with_duration_and_rows(make_db, tmp_path, seed=None):
    db = make_logged_db(tmp_path, make_db)
    try:
        db.dispatch("query", {"sql": "select id from dbmcp_test.t order by id", "page_size": 7})
        db.dispatch("query", {"sql": "select * from does_not_exist"})
        entries = read_log(tmp_path)
        ok, err = entries[0], entries[1]
        assert ok["tool"] == "query" and ok["ok"] is True
        assert ok["profile"] == "test"
        assert ok["ms"] >= 0
        assert ok["rows"] == 7 and ok["has_more"] is True
        assert "select id from dbmcp_test.t" in ok["sql"]
        assert err["ok"] is False and err["sqlstate"] == "42P01"
    finally:
        db.shutdown()


def test_profile_add_never_logs_dsn(make_db, tmp_path):
    db = make_logged_db(tmp_path, make_db)
    try:
        db.dispatch(
            "profile_add",
            {"name": "sec", "dsn": "postgresql://u:topsecret@127.0.0.1:5432/postgres",
             "test": False},
        )
        raw = "\n".join(json.dumps(e) for e in read_log(tmp_path))
        assert "topsecret" not in raw
        assert '"name": "sec"' in raw or '"name":"sec"' in raw
    finally:
        db.shutdown()


def test_logs_tool_filters(make_db, tmp_path):
    db = make_logged_db(tmp_path, make_db)
    try:
        db.dispatch("query", {"sql": "select 1"})
        db.dispatch("tables", {})
        r = db.dispatch("logs", {"tool": "query"})
        assert all(e["tool"] == "query" for e in r["entries"])
        assert r["log"]["enabled"] is True and r["log"]["files"] == 1
    finally:
        db.shutdown()


def test_retention_prunes_old_files(tmp_path):
    d = tmp_path / "log"
    d.mkdir()
    (d / "query-20200101.jsonl").write_text("{}\n")
    (d / f"query-{time.strftime('%Y%m%d')}.jsonl").write_text("{}\n")
    QueryLog(str(d), days=14)
    names = sorted(os.listdir(d))
    assert "query-20200101.jsonl" not in names
    assert len(names) == 1


def test_disabled_log(tmp_path):
    q = QueryLog(str(tmp_path / "log"), days=0)
    assert q.enabled is False
    q.write({"tool": "query"})  # must be a no-op, not an error
    assert not (tmp_path / "log").exists()
