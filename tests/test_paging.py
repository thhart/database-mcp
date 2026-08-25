import time

SEL = "select id, txt from dbmcp_test.t order by id"


def test_first_page_and_full_pagination(make_db):
    db = make_db()
    r = db.dispatch("query", {"sql": SEL, "page_size": 100})
    assert r["columns"] == ["id", "txt"]
    assert r["page"]["returned"] == 100
    assert r["page"]["has_more"] is True
    assert "cursor" in r["page"]
    assert r["rows"][0] == [1, "row-1"]

    seen = [row[0] for row in r["rows"]]
    cursor = r["page"]["cursor"]
    while True:
        r = db.dispatch("fetch", {"cursor": cursor, "page_size": 100})
        assert "error" not in r, r
        seen += [row[0] for row in r["rows"]]
        if not r["page"]["has_more"]:
            break
    assert seen == list(range(1, 1001))
    assert r["page"]["total_delivered"] == 1000

    # exhausted cursor auto-closes; another fetch must fail cleanly
    r = db.dispatch("fetch", {"cursor": cursor})
    assert "error" in r and cursor in r["error"]


def test_estimated_rows_from_planner(make_db):
    db = make_db()
    r = db.dispatch("query", {"sql": SEL, "page_size": 10})
    assert 500 <= r["page"]["estimated_rows"] <= 2000


def test_single_page_query_returns_no_cursor(make_db):
    db = make_db()
    r = db.dispatch("query", {"sql": SEL + " limit 5", "page_size": 50})
    assert r["page"]["returned"] == 5
    assert r["page"]["has_more"] is False
    assert "cursor" not in r["page"]
    assert db.dispatch("status", {})["open_cursors"] == []


def test_byte_cap_limits_page(make_db):
    db = make_db(max_page_bytes=2000)
    r = db.dispatch("query", {"sql": "select blob from dbmcp_test.big order by id", "page_size": 50})
    assert 1 <= r["page"]["returned"] < 10
    assert r["page"]["has_more"] is True


def test_cell_truncation(make_db):
    db = make_db(max_cell=100)
    r = db.dispatch("query", {"sql": "select blob from dbmcp_test.big limit 1"})
    cell = r["rows"][0][0]
    assert len(cell) < 500
    assert "+400 chars" in cell


def test_readonly_rejects_writes(make_db):
    db = make_db()
    r = db.dispatch("query", {"sql": "insert into dbmcp_test.t values (0, 'nope')"})
    assert r.get("sqlstate") == "25006"  # read_only_sql_transaction


def test_writes_when_allowed(make_db):
    db = make_db(allow_writes=True)
    r = db.dispatch("query", {"sql": "insert into dbmcp_test.t values (0, 'yes')"})
    assert r == {"status": "INSERT 0 1", "rowcount": 1}
    r = db.dispatch("query", {"sql": "delete from dbmcp_test.t where id = 0"})
    assert r["rowcount"] == 1


def test_params(make_db):
    db = make_db()
    r = db.dispatch("query", {"sql": "select txt from dbmcp_test.t where id = %s", "params": [42]})
    assert r["rows"] == [["row-42"]]


def test_cursor_ttl_eviction(make_db):
    db = make_db(cursor_ttl=0.4)
    r = db.dispatch("query", {"sql": SEL, "page_size": 10})
    cursor = r["page"]["cursor"]
    time.sleep(0.7)
    r = db.dispatch("fetch", {"cursor": cursor})
    assert "error" in r and "expired" in r["error"]


def test_max_cursors_lru_eviction(make_db):
    db = make_db(max_cursors=2)
    tokens = [
        db.dispatch("query", {"sql": SEL, "page_size": 10})["page"]["cursor"]
        for _ in range(3)
    ]
    assert "error" in db.dispatch("fetch", {"cursor": tokens[0]})
    assert "error" not in db.dispatch("fetch", {"cursor": tokens[1]})
    assert "error" not in db.dispatch("fetch", {"cursor": tokens[2]})


def test_explicit_close(make_db):
    db = make_db()
    r = db.dispatch("query", {"sql": SEL, "page_size": 10})
    assert db.dispatch("close", {"cursor": r["page"]["cursor"]}) == {"closed": 1}
    assert db.dispatch("status", {})["open_cursors"] == []


def test_non_cursorable_statement(make_db):
    db = make_db()
    r = db.dispatch("query", {"sql": "show server_version"})
    assert r["columns"] == ["server_version"]
    assert r["page"]["has_more"] is False


def test_tables_and_describe(make_db):
    db = make_db()
    t = db.dispatch("tables", {"filter": "t"})
    names = [row[1] for row in t["rows"]]
    assert "t" in names
    d = db.dispatch("describe", {"table": "dbmcp_test.t"})
    cols = [c[0] for c in d["columns"]]
    assert cols == ["id", "txt"]
    assert any("PRIMARY KEY" in c for c in d["constraints"])


def test_status(make_db):
    db = make_db()
    db.dispatch("query", {"sql": "select 1"})  # connect the default profile
    s = db.dispatch("status", {})
    assert s["default"] == "test"
    prof = s["profiles"][0]
    assert prof["read_only"] is True and prof["connected"] is True
    assert s["limits"]["max_cursors"] == 4


def test_error_is_clean_dict(make_db):
    db = make_db()
    r = db.dispatch("query", {"sql": "select * from does_not_exist"})
    assert "error" in r and r["sqlstate"] == "42P01"
    # pool must still be usable afterwards
    assert db.dispatch("query", {"sql": "select 1"})["rows"] == [[1]]
