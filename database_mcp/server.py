"""database-mcp: SQL database MCP server with true server-side result paging.

PostgreSQL reference implementation. A query runs ONCE as a server-side
cursor; pages are fetched from the held cursor without re-execution (see
pager.py). Connections are named profiles the AI manages at runtime via
profile_add / profile_remove — no server restart needed (see profiles.py).
"""

import argparse
import asyncio
import os
import re
import sys
import time
from dataclasses import dataclass

import psycopg
from psycopg.conninfo import make_conninfo
from psycopg_pool import ConnectionPool

from mcp.server import MCPServer

from . import render
from .pager import CursorExpired, CursorRegistry
from .profiles import ProfileStore, redact_dsn

# statements Postgres can serve through DECLARE ... CURSOR
CURSORABLE = re.compile(r"^\s*(select|with|values|table)\b", re.IGNORECASE)


class UserError(Exception):
    """Bad tool input; message goes verbatim to the model."""


@dataclass
class Limits:
    page_size: int = 50
    max_page_size: int = 500
    max_page_bytes: int = 32_000
    max_cell: int = 400
    cursor_ttl: float = 300.0
    max_cursors: int = 4
    statement_timeout: float = 30.0

    def as_dict(self) -> dict:
        return {
            "page_size": self.page_size,
            "max_page_size": self.max_page_size,
            "max_page_bytes": self.max_page_bytes,
            "max_cell_chars": self.max_cell,
            "cursor_ttl_s": self.cursor_ttl,
            "max_cursors": self.max_cursors,
            "statement_timeout_s": self.statement_timeout,
        }


def clamp_page_size(args: dict, limits: Limits) -> int:
    requested = args.get("page_size") or limits.page_size
    return max(1, min(int(requested), limits.max_page_size))


class Engine:
    """One profile's connection pool and query logic."""

    def __init__(self, name: str, dsn: str, allow_writes: bool, limits: Limits, registry: CursorRegistry):
        self.name = name
        self.allow_writes = allow_writes
        self.limits = limits
        self.registry = registry
        options = (
            f"-c statement_timeout={int(limits.statement_timeout * 1000)} "
            f"-c idle_in_transaction_session_timeout={int((limits.cursor_ttl + 60) * 1000)}"
        )
        self.pool = ConnectionPool(
            make_conninfo(dsn, options=options),
            min_size=0,
            max_size=limits.max_cursors + 2,
            open=False,
            configure=self._configure_conn,
        )

    def _configure_conn(self, conn):
        if not self.allow_writes:
            conn.read_only = True

    def close(self):
        self.registry.close_profile(self.name)
        self.pool.close()

    # -- query -------------------------------------------------------------

    def query(self, args: dict) -> dict:
        sql = args.get("sql", "").strip()
        if not sql:
            raise UserError("sql is required")
        self.pool.open()
        params = args.get("params") or None
        page_size = clamp_page_size(args, self.limits)

        if CURSORABLE.match(sql):
            try:
                return self._query_cursor(sql, params, page_size)
            except psycopg.errors.ProgrammingError as e:
                # e.g. data-modifying CTE: not DECLARE-able -> plain execution
                if "cursor" not in str(e).lower():
                    raise
        return self._query_plain(sql, params, page_size)

    def _query_cursor(self, sql: str, params, page_size: int) -> dict:
        estimated = self._estimate_rows(sql, params)
        conn = self.pool.getconn()
        try:
            cur = conn.cursor(name="pg_" + os.urandom(4).hex())
            cur.execute(sql, params)
            pq = self.registry.open(conn, cur, None, self.pool, self.name)
        except Exception:
            try:
                conn.rollback()
            except Exception:
                pass
            self.pool.putconn(conn)
            raise
        rows, has_more = pq.fetch_page(page_size, self.limits.max_page_bytes, self.limits.max_cell)
        # description is reliably populated once the first row was fetched
        columns = [d.name for d in pq.cursor.description] if pq.cursor.description else []
        page = {"returned": len(rows), "has_more": has_more}
        if estimated is not None:
            page["estimated_rows"] = estimated
        if has_more:
            page["cursor"] = pq.token
        else:
            self.registry.close(pq.token)
        return {"columns": columns, "rows": rows, "page": page}

    def _query_plain(self, sql: str, params, page_size: int) -> dict:
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                if cur.description is None:
                    return {"status": cur.statusmessage, "rowcount": cur.rowcount}
                columns = [d.name for d in cur.description]
                raw = cur.fetchmany(page_size + 1)
                truncated = len(raw) > page_size
                rows = [render.convert_row(r, self.limits.max_cell) for r in raw[:page_size]]
                page = {"returned": len(rows), "has_more": truncated}
                if truncated:
                    page["note"] = (
                        "statement is not pageable via cursor; "
                        "add LIMIT/OFFSET to see further rows"
                    )
                return {"columns": columns, "rows": rows, "page": page}

    def _estimate_rows(self, sql: str, params):
        """Planner row estimate via EXPLAIN, best effort, on its own conn."""
        try:
            with self.pool.connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("EXPLAIN (FORMAT JSON) " + sql, params)
                    plan = cur.fetchone()[0]
                    return int(plan[0]["Plan"]["Plan Rows"])
        except Exception:
            return None

    # -- schema ------------------------------------------------------------

    def tables(self, args: dict) -> dict:
        self.pool.open()
        name_filter = args.get("filter")
        sql = """
            select n.nspname, c.relname, c.relkind::text,
                   greatest(c.reltuples, 0)::bigint,
                   pg_size_pretty(pg_total_relation_size(c.oid))
            from pg_class c
            join pg_namespace n on n.oid = c.relnamespace
            where c.relkind in ('r','v','m','p','f')
              and n.nspname not in ('pg_catalog','information_schema')
              and n.nspname !~ '^pg_toast'
        """
        params: list = []
        if name_filter:
            sql += " and c.relname ilike %s"
            params.append(f"%{name_filter}%")
        sql += " order by n.nspname, c.relname"
        kinds = {"r": "table", "v": "view", "m": "matview", "p": "partitioned", "f": "foreign"}
        with self.pool.connection() as conn:
            rows = conn.execute(sql, params).fetchall()
        return {
            "columns": ["schema", "name", "kind", "est_rows", "size"],
            "rows": [[s, t, kinds.get(k, k), n, sz] for s, t, k, n, sz in rows],
        }

    def describe(self, args: dict) -> dict:
        table = args.get("table", "").strip()
        if not table:
            raise UserError("table is required (optionally schema-qualified)")
        self.pool.open()
        with self.pool.connection() as conn:
            cols = conn.execute(
                """
                select a.attname, format_type(a.atttypid, a.atttypmod),
                       a.attnotnull, pg_get_expr(d.adbin, d.adrelid)
                from pg_attribute a
                left join pg_attrdef d
                       on d.adrelid = a.attrelid and d.adnum = a.attnum
                where a.attrelid = %s::regclass
                  and a.attnum > 0 and not a.attisdropped
                order by a.attnum
                """,
                [table],
            ).fetchall()
            cons = conn.execute(
                """
                select conname, pg_get_constraintdef(oid)
                from pg_constraint
                where conrelid = %s::regclass and contype in ('p','u','f','c')
                order by contype, conname
                """,
                [table],
            ).fetchall()
            idx = conn.execute(
                """
                select i.relname, pg_get_indexdef(x.indexrelid)
                from pg_index x
                join pg_class i on i.oid = x.indexrelid
                where x.indrelid = %s::regclass and not x.indisprimary
                order by i.relname
                """,
                [table],
            ).fetchall()
        return {
            "table": table,
            "columns": [
                [name, typ, "NOT NULL" if notnull else "NULL", default]
                for name, typ, notnull, default in cols
            ],
            "constraints": [f"{n}: {d}" for n, d in cons],
            "indexes": [d for _, d in idx],
        }

    def explain(self, args: dict) -> dict:
        sql = args.get("sql", "").strip()
        if not sql:
            raise UserError("sql is required")
        self.pool.open()
        analyze = bool(args.get("analyze"))
        prefix = "EXPLAIN (ANALYZE, FORMAT TEXT) " if analyze else "EXPLAIN (FORMAT TEXT) "
        with self.pool.connection() as conn:
            rows = conn.execute(prefix + sql, args.get("params") or None).fetchall()
        return {"plan": [r[0] for r in rows]}


class Manager:
    """Routes tools to profile engines; owns the global cursor registry."""

    def __init__(self, store: ProfileStore, limits: Limits):
        self.store = store
        self.limits = limits
        self.registry = CursorRegistry(limits.cursor_ttl, limits.max_cursors)
        self.engines: dict[str, Engine] = {}

    # -- dispatch ----------------------------------------------------------

    def dispatch(self, name: str, args: dict) -> dict:
        try:
            handler = {
                "query": lambda a: self.engine(a.get("profile")).query(a),
                "tables": lambda a: self.engine(a.get("profile")).tables(a),
                "describe": lambda a: self.engine(a.get("profile")).describe(a),
                "explain": lambda a: self.engine(a.get("profile")).explain(a),
                "fetch": self.fetch,
                "close": self.close_tool,
                "status": self.status,
                "profiles": self.profiles_list,
                "profile_add": self.profile_add,
                "profile_remove": self.profile_remove,
                "profile_test": self.profile_test,
            }.get(name)
            if handler is None:
                return {"error": f"unknown tool: {name}"}
            return handler(args)
        except (CursorExpired, UserError) as e:
            return {"error": str(e)}
        except psycopg.Error as e:
            msg = str(e).strip()
            out = {"error": msg}
            if e.sqlstate:
                out["sqlstate"] = e.sqlstate
            return out
        except Exception as e:  # never crash the server on a tool call
            return {"error": f"{type(e).__name__}: {e}"}

    def engine(self, profile: str | None) -> Engine:
        name = profile or self.store.default
        if not name:
            raise UserError("no profiles configured — add one with profile_add")
        spec = self.store.profiles.get(name)
        if spec is None:
            known = ", ".join(self.store.profiles) or "none"
            raise UserError(f"unknown profile '{name}' (known: {known})")
        eng = self.engines.get(name)
        if eng is None:
            eng = Engine(name, spec["dsn"], bool(spec.get("allow_writes")), self.limits, self.registry)
            self.engines[name] = eng
        return eng

    # -- cursor tools ------------------------------------------------------

    def fetch(self, args: dict) -> dict:
        token = args.get("cursor", "")
        if not token:
            raise UserError("cursor is required")
        page_size = clamp_page_size(args, self.limits)
        pq = self.registry.get(token)
        rows, has_more = pq.fetch_page(page_size, self.limits.max_page_bytes, self.limits.max_cell)
        columns = [d.name for d in pq.cursor.description] if pq.cursor.description else []
        page = {
            "returned": len(rows),
            "has_more": has_more,
            "total_delivered": pq.rows_delivered,
        }
        if has_more:
            page["cursor"] = token
        else:
            self.registry.close(token)
        return {"columns": columns, "rows": rows, "page": page}

    def close_tool(self, args: dict) -> dict:
        token = args.get("cursor")
        if token:
            return {"closed": 1 if self.registry.close(token) else 0}
        return {"closed": self.registry.close_all()}

    # -- profile tools -----------------------------------------------------

    def profiles_list(self, args: dict) -> dict:
        return {
            "default": self.store.default,
            "profiles": [
                {
                    "name": name,
                    "dsn": redact_dsn(spec["dsn"]),
                    "read_only": not spec.get("allow_writes"),
                    "description": spec.get("description"),
                    "connected": name in self.engines,
                }
                for name, spec in self.store.profiles.items()
            ],
        }

    def profile_add(self, args: dict) -> dict:
        name = (args.get("name") or "").strip()
        dsn = (args.get("dsn") or "").strip()
        if not name or not dsn:
            raise UserError("name and dsn are required")
        result = {"saved": name}
        if args.get("test", True):
            version, ms = self._probe(dsn)
            result["server"] = version
            result["connect_ms"] = ms
        self._drop_engine(name)  # replacing an existing profile: fresh pool
        self.store.upsert(
            name,
            dsn,
            allow_writes=bool(args.get("allow_writes")),
            description=args.get("description"),
            make_default=bool(args.get("make_default")),
        )
        result["default"] = self.store.default
        return result

    def profile_remove(self, args: dict) -> dict:
        name = (args.get("name") or "").strip()
        if not name:
            raise UserError("name is required")
        self._drop_engine(name)
        removed = self.store.remove(name)
        return {"removed": removed, "default": self.store.default}

    def profile_test(self, args: dict) -> dict:
        name = (args.get("name") or "").strip() or self.store.default
        spec = self.store.profiles.get(name or "")
        if spec is None:
            raise UserError(f"unknown profile '{name}'")
        version, ms = self._probe(spec["dsn"])
        return {"profile": name, "server": version, "connect_ms": ms}

    def _probe(self, dsn: str):
        t0 = time.monotonic()
        with psycopg.connect(dsn, connect_timeout=5) as conn:
            version = conn.execute("select version()").fetchone()[0]
        return version.split(" on ")[0], round((time.monotonic() - t0) * 1000, 1)

    def _drop_engine(self, name: str):
        eng = self.engines.pop(name, None)
        if eng is not None:
            eng.close()

    # -- status ------------------------------------------------------------

    def status(self, args: dict) -> dict:
        self.registry.sweep()
        profs = []
        for name, spec in self.store.profiles.items():
            entry = {
                "name": name,
                "read_only": not spec.get("allow_writes"),
                "connected": name in self.engines,
            }
            eng = self.engines.get(name)
            if eng is not None:
                stats = eng.pool.get_stats()
                entry["pool"] = {k: v for k, v in stats.items() if v}
            profs.append(entry)
        return {
            "default": self.store.default,
            "profiles": profs,
            "open_cursors": self.registry.snapshot(),
            "limits": self.limits.as_dict(),
            "profiles_file": str(self.store.path),
        }

    def shutdown(self):
        self.registry.close_all()
        for eng in self.engines.values():
            eng.pool.close()


# -- MCP wiring ------------------------------------------------------------


def build_server(mgr: Manager) -> MCPServer:
    srv = MCPServer(
        name="database-mcp",
        instructions=(
            "SQL access to PostgreSQL with true server-side result paging and "
            "runtime-managed connection profiles. Run SQL with the query tool; "
            "when page.has_more is true, continue with fetch(cursor) — the "
            "query is not re-executed, the page continues from a held "
            "server-side cursor with a stable snapshot. Connections are named "
            "profiles: list with profiles, switch per call via the profile "
            "parameter, add/change on the fly with profile_add."
        ),
    )

    async def _call(tool: str, args: dict) -> str:
        result = await asyncio.to_thread(mgr.dispatch, tool, args)
        return render.dumps(result)

    @srv.tool(
        description=(
            "Execute SQL on a profile (default profile when omitted). Returns "
            'the first page as compact {"columns":[...],"rows":[[...],...],'
            '"page":{"returned","has_more","estimated_rows","cursor"}}. When '
            "has_more is true, pass page.cursor to the fetch tool — the query "
            "is NOT re-executed; a server-side cursor is held open (stable "
            "snapshot). Placeholders: %s positional with the params array."
        )
    )
    async def query(
        sql: str,
        params: list | None = None,
        page_size: int | None = None,
        profile: str | None = None,
    ) -> str:
        return await _call(
            "query", {"sql": sql, "params": params, "page_size": page_size, "profile": profile}
        )

    @srv.tool(
        description=(
            "Fetch the next page from an open cursor returned by query. "
            "No re-execution: rows continue exactly where the last page ended. "
            "The cursor auto-closes when exhausted (has_more=false)."
        )
    )
    async def fetch(cursor: str, page_size: int | None = None) -> str:
        return await _call("fetch", {"cursor": cursor, "page_size": page_size})

    @srv.tool(
        description=(
            "Close an open cursor (or all cursors when no token is given) "
            "to free its connection early."
        )
    )
    async def close(cursor: str | None = None) -> str:
        return await _call("close", {"cursor": cursor})

    @srv.tool(
        description=(
            "List tables/views/matviews with estimated row counts and sizes "
            "(system schemas excluded). Optional name filter."
        )
    )
    async def tables(filter: str | None = None, profile: str | None = None) -> str:
        return await _call("tables", {"filter": filter, "profile": profile})

    @srv.tool(
        description=(
            "Describe one table: columns with types/nullability/defaults, "
            "constraints (PK/FK/unique/check), and indexes."
        )
    )
    async def describe(table: str, profile: str | None = None) -> str:
        return await _call("describe", {"table": table, "profile": profile})

    @srv.tool(
        description=(
            "Show the query plan (EXPLAIN). Set analyze=true to actually run "
            "the statement and get real timings."
        )
    )
    async def explain(
        sql: str,
        params: list | None = None,
        analyze: bool = False,
        profile: str | None = None,
    ) -> str:
        return await _call(
            "explain", {"sql": sql, "params": params, "analyze": analyze, "profile": profile}
        )

    @srv.tool(
        description="List all connection profiles (DSNs password-redacted) and which one is the default."
    )
    async def profiles() -> str:
        return await _call("profiles", {})

    @srv.tool(
        description=(
            "Add or update a named connection profile at runtime and persist it. "
            "Tests the connection first (set test=false to skip). Profiles are "
            "read-only unless allow_writes=true. make_default=true switches the "
            "default profile."
        )
    )
    async def profile_add(
        name: str,
        dsn: str,
        allow_writes: bool = False,
        description: str | None = None,
        make_default: bool = False,
        test: bool = True,
    ) -> str:
        return await _call(
            "profile_add",
            {
                "name": name,
                "dsn": dsn,
                "allow_writes": allow_writes,
                "description": description,
                "make_default": make_default,
                "test": test,
            },
        )

    @srv.tool(description="Remove a connection profile (open cursors on it are closed).")
    async def profile_remove(name: str) -> str:
        return await _call("profile_remove", {"name": name})

    @srv.tool(
        description="Test connectivity of a profile (default profile when omitted): server version and connect latency."
    )
    async def profile_test(name: str | None = None) -> str:
        return await _call("profile_test", {"name": name})

    @srv.tool(
        description=(
            "Server status: profiles with pool statistics, open cursors, "
            "configured limits, profiles file location."
        )
    )
    async def status() -> str:
        return await _call("status", {})

    return srv


def run():
    parser = argparse.ArgumentParser(
        prog="database-mcp",
        description="SQL database MCP server with true server-side result paging (PostgreSQL)",
    )
    parser.add_argument(
        "--profiles",
        default=os.getenv("DATABASE_MCP_PROFILES"),
        help="profiles JSON file (default ~/.config/database-mcp/profiles.json)",
    )
    parser.add_argument(
        "--dsn",
        default=os.getenv("DATABASE_MCP_DSN") or os.getenv("DATABASE_URL"),
        help="register/update the 'default' profile with this DSN at startup "
        "(env: DATABASE_MCP_DSN or DATABASE_URL)",
    )
    parser.add_argument("--allow-writes", action="store_true", help="the --dsn profile may write")
    parser.add_argument("--page-size", type=int, default=50)
    parser.add_argument("--max-page-size", type=int, default=500)
    parser.add_argument("--max-page-bytes", type=int, default=32_000)
    parser.add_argument("--max-cell", type=int, default=400)
    parser.add_argument("--cursor-ttl", type=float, default=300.0)
    parser.add_argument("--max-cursors", type=int, default=4)
    parser.add_argument("--statement-timeout", type=float, default=30.0)
    args = parser.parse_args()

    store = ProfileStore(args.profiles)
    if args.dsn:
        store.upsert("default", args.dsn, allow_writes=args.allow_writes, make_default=True)
    if not store.profiles:
        print(
            "database-mcp: no profiles configured yet — the AI can add one at "
            "runtime via the profile_add tool",
            file=sys.stderr,
        )

    mgr = Manager(
        store,
        Limits(
            page_size=args.page_size,
            max_page_size=args.max_page_size,
            max_page_bytes=args.max_page_bytes,
            max_cell=args.max_cell,
            cursor_ttl=args.cursor_ttl,
            max_cursors=args.max_cursors,
            statement_timeout=args.statement_timeout,
        ),
    )
    try:
        asyncio.run(build_server(mgr).run_stdio_async())
    finally:
        mgr.shutdown()


if __name__ == "__main__":
    run()
