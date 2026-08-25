import csv
import json

from database_mcp.bulk import split_statements


def test_split_statements_labels_and_quoting():
    sql = """
    -- the trains
    select * from t where txt = 'a;b';
    /* block comment */
    -- dollar quoted
    select $tag$x;y$tag$::text;
    select 1
    """
    stmts = split_statements(sql)
    assert len(stmts) == 3
    assert stmts[0] == ("the trains", "select * from t where txt = 'a;b'")
    assert stmts[1][0] == "dollar quoted"
    assert "$tag$x;y$tag$" in stmts[1][1]
    assert stmts[2] == (None, "select 1")


def test_script_multiple_results(make_db):
    db = make_db()
    r = db.dispatch(
        "script",
        {
            "sql": """
            -- row count
            select count(*) from dbmcp_test.t;
            -- top three
            select id from dbmcp_test.t order by id limit 3;
            """,
        },
    )
    assert r["statements"] == 2
    a, b = r["results"]
    assert a["label"] == "row count" and a["rows"] == [[1000]]
    assert b["label"] == "top three" and b["rows"] == [[1], [2], [3]]


def test_script_row_cap_and_error_abort(make_db):
    db = make_db()
    r = db.dispatch(
        "script",
        {
            "sql": "select id from dbmcp_test.t order by id; select * from nope; select 2;",
            "rows_per_statement": 5,
        },
    )
    res = r["results"]
    assert res[0]["truncated"] is True and len(res[0]["rows"]) == 5
    assert "error" in res[1] and res[1]["sqlstate"] == "42P01"
    assert "skipped" in res[2]["note"]


def test_export_csv(make_db, tmp_path):
    db = make_db()
    out = str(tmp_path / "t.csv")
    r = db.dispatch(
        "export",
        {"sql": "select id, txt from dbmcp_test.t order by id", "path": out},
    )
    assert r["rows"] == 1000 and r["truncated"] is False
    with open(out) as f:
        rows = list(csv.reader(f))
    assert rows[0] == ["id", "txt"]
    assert rows[1] == ["1", "row-1"]
    assert len(rows) == 1001


def test_export_jsonl_and_max_rows(make_db, tmp_path):
    db = make_db()
    out = str(tmp_path / "t.jsonl")
    r = db.dispatch(
        "export",
        {
            "sql": "select id, txt from dbmcp_test.t order by id",
            "path": out,
            "format": "jsonl",
            "max_rows": 10,
        },
    )
    assert r["rows"] == 10 and r["truncated"] is True
    lines = [json.loads(line) for line in open(out)]
    assert lines[0] == {"id": 1, "txt": "row-1"}
    assert len(lines) == 10


def test_query_timeout_override(make_db):
    db = make_db()
    r = db.dispatch("query", {"sql": "select pg_sleep(2)", "timeout_s": 0.3})
    assert r.get("sqlstate") == "57014"  # query_canceled (statement timeout)
    # the failed cursor must not linger in the registry pinning a connection
    assert db.dispatch("status", {})["open_cursors"] == []
    assert db.dispatch("query", {"sql": "select 1"})["rows"] == [[1]]
