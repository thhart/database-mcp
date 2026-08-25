from conftest import DSN


def test_profile_add_and_query_by_profile(make_db):
    db = make_db()
    r = db.dispatch(
        "profile_add",
        {"name": "second", "dsn": DSN, "description": "same db, second profile"},
    )
    assert r["saved"] == "second"
    assert "PostgreSQL" in r["server"]
    assert r["default"] == "test"  # unchanged without make_default

    r = db.dispatch("query", {"sql": "select 42", "profile": "second"})
    assert r["rows"] == [[42]]

    listing = db.dispatch("profiles", {})
    names = {p["name"] for p in listing["profiles"]}
    assert names == {"test", "second"}
    # profiles are write-enabled by default now
    entry = next(p for p in listing["profiles"] if p["name"] == "second")
    assert entry["read_only"] is False
    r = db.dispatch(
        "query",
        {"sql": "create temp table wtest as select 1 as x", "profile": "second"},
    )
    assert "error" not in r, r


def test_profile_add_persists(make_db):
    db = make_db()
    db.dispatch("profile_add", {"name": "persisted", "dsn": DSN, "test": False})
    from database_mcp.profiles import ProfileStore

    reloaded = ProfileStore(db.store.path)
    assert "persisted" in reloaded.profiles


def test_profile_make_default(make_db):
    db = make_db()
    db.dispatch("profile_add", {"name": "d2", "dsn": DSN, "make_default": True, "test": False})
    assert db.dispatch("profiles", {})["default"] == "d2"
    # unqualified query now runs on d2
    assert db.dispatch("query", {"sql": "select 1"})["rows"] == [[1]]


def test_profile_remove_closes_cursors(make_db):
    db = make_db()
    db.dispatch("profile_add", {"name": "tmp", "dsn": DSN, "test": False})
    r = db.dispatch(
        "query",
        {"sql": "select g from generate_series(1,100) g", "page_size": 10, "profile": "tmp"},
    )
    cursor = r["page"]["cursor"]
    assert db.dispatch("profile_remove", {"name": "tmp"})["removed"] is True
    r = db.dispatch("fetch", {"cursor": cursor})
    assert "error" in r


def test_unknown_profile_error(make_db):
    db = make_db()
    r = db.dispatch("query", {"sql": "select 1", "profile": "nope"})
    assert "unknown profile 'nope'" in r["error"]
    assert "test" in r["error"]  # lists known profiles


def test_profile_add_bad_dsn_fails_and_keeps_store(make_db):
    db = make_db()
    r = db.dispatch(
        "profile_add",
        {"name": "broken", "dsn": "postgresql://nouser@127.0.0.1:1/none"},
    )
    assert "error" in r
    assert "broken" not in db.store.profiles


def test_profile_test(make_db):
    db = make_db()
    r = db.dispatch("profile_test", {})
    assert r["profile"] == "test"
    assert "PostgreSQL" in r["server"]
    assert r["connect_ms"] >= 0


def test_dsn_redaction(make_db):
    db = make_db()
    db.dispatch(
        "profile_add",
        {"name": "secret", "dsn": "postgresql://u:sekret@127.0.0.1:5432/postgres", "test": False},
    )
    listing = db.dispatch("profiles", {})
    entry = next(p for p in listing["profiles"] if p["name"] == "secret")
    assert "sekret" not in entry["dsn"]
    assert "***" in entry["dsn"]
