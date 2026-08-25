def test_overview(make_db):
    db = make_db()
    r = db.dispatch("overview", {"filter": "t"})
    row = next(x for x in r["rows"] if x[0] == "dbmcp_test.t")
    assert row[1] == "table"
    assert 900 <= row[2] <= 1100
    assert row[3] == "id,txt"


def test_search_objects_by_name_and_comment(make_db):
    db = make_db()
    r = db.dispatch("search_objects", {"term": "txt"})
    kinds = {(row[0], row[1]) for row in r["rows"]}
    assert ("column", "dbmcp_test.t.txt") in kinds

    r = db.dispatch("search_objects", {"term": "human readable"})
    assert any(row[1] == "dbmcp_test.t.txt" for row in r["rows"])

    r = db.dispatch("search_objects", {"term": "primary demo"})
    assert any(row[1] == "dbmcp_test.t" and row[0] == "table" for row in r["rows"])


def test_profile_stats(make_db):
    db = make_db()
    r = db.dispatch("profile", {"table": "dbmcp_test.t"})
    assert r["table"] == "dbmcp_test.t"
    assert 900 <= r["est_rows"] <= 1100
    assert "note" not in r  # analyzed in seed
    by_col = {row[0]: row for row in r["rows"]}
    id_stats = by_col["id"]
    assert id_stats[2] == 0.0  # null_frac
    assert id_stats[3] == -1.0  # n_distinct: unique
    assert id_stats[7] is not None  # histogram bounds present


def test_relations(make_db):
    db = make_db()
    r = db.dispatch("relations", {"table": "dbmcp_test.child"})
    assert any("dbmcp_test.t" in ref[0] for ref in r["references"])
    assert any("dbmcp_test.grand" in ref[0] for ref in r["referenced_by"])


def test_join_path_direct_and_two_hops(make_db):
    db = make_db()
    r = db.dispatch("join_path", {"from_table": "dbmcp_test.child", "to_table": "dbmcp_test.t"})
    assert r["paths"][0]["hops"] == 1
    assert "dbmcp_test.child.t_id = dbmcp_test.t.id" in r["paths"][0]["join"]

    r = db.dispatch("join_path", {"from_table": "dbmcp_test.grand", "to_table": "dbmcp_test.t"})
    assert r["paths"][0]["hops"] == 2
    j = r["paths"][0]["join"]
    assert j.startswith("FROM dbmcp_test.grand")
    assert "JOIN dbmcp_test.child" in j and "JOIN dbmcp_test.t" in j


def test_join_path_no_path(make_db):
    db = make_db()
    r = db.dispatch("join_path", {"from_table": "dbmcp_test.big", "to_table": "dbmcp_test.t"})
    assert "error" in r and "no FK path" in r["error"]


def test_count_estimate_and_exact(make_db):
    db = make_db()
    r = db.dispatch("count", {"table": "dbmcp_test.t"})
    assert 900 <= r["estimated"] <= 1100
    r = db.dispatch("count", {"table": "dbmcp_test.t", "exact": True})
    assert r["exact"] == 1000
    r = db.dispatch("count", {"table": "dbmcp_test.t", "where": "id <= 10", "exact": True})
    assert r["exact"] == 10


def test_sample(make_db):
    db = make_db()
    r = db.dispatch("sample", {"table": "dbmcp_test.t", "n": 7})
    assert r["columns"] == ["id", "txt"]
    assert len(r["rows"]) == 7
    ids = [row[0] for row in r["rows"]]
    assert all(1 <= i <= 1000 for i in ids)
