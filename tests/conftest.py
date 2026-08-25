import os

import psycopg
import pytest

from database_mcp.profiles import ProfileStore
from database_mcp.server import Limits, Manager

DSN = os.getenv("DBMCP_TEST_DSN", "postgresql://th@127.0.0.1:5432/postgres")


@pytest.fixture(scope="session")
def seed():
    try:
        conn = psycopg.connect(DSN, autocommit=True, connect_timeout=3)
    except Exception as e:
        pytest.skip(f"no test PostgreSQL reachable at {DSN}: {e}")
    with conn:
        conn.execute("drop schema if exists dbmcp_test cascade")
        conn.execute("create schema dbmcp_test")
        conn.execute(
            "create table dbmcp_test.t as "
            "select g as id, 'row-' || g as txt from generate_series(1, 1000) g"
        )
        conn.execute("alter table dbmcp_test.t add primary key (id)")
        conn.execute("analyze dbmcp_test.t")
        conn.execute(
            "create table dbmcp_test.big as "
            "select g as id, repeat('x', 500) as blob from generate_series(1, 50) g"
        )
        conn.execute(
            "create table dbmcp_test.child ("
            " id int primary key,"
            " t_id int not null references dbmcp_test.t(id))"
        )
        conn.execute(
            "insert into dbmcp_test.child select g, g from generate_series(1, 100) g"
        )
        conn.execute(
            "create table dbmcp_test.grand ("
            " id int primary key,"
            " child_id int references dbmcp_test.child(id))"
        )
        conn.execute("comment on table dbmcp_test.t is 'primary demo table'")
        conn.execute("comment on column dbmcp_test.t.txt is 'human readable label'")
        conn.execute("analyze dbmcp_test.t, dbmcp_test.child, dbmcp_test.grand")
    yield
    with psycopg.connect(DSN, autocommit=True) as conn:
        conn.execute("drop schema if exists dbmcp_test cascade")


@pytest.fixture
def make_db(seed, tmp_path):
    managers = []

    def _make(allow_writes=False, **limits):
        store = ProfileStore(tmp_path / f"profiles-{len(managers)}.json")
        store.upsert("test", DSN, allow_writes=allow_writes, make_default=True)
        mgr = Manager(store, Limits(**limits))
        managers.append(mgr)
        return mgr

    yield _make
    for mgr in managers:
        mgr.shutdown()
