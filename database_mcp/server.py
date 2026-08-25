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
import threading
import time
from dataclasses import dataclass

import psycopg
from psycopg.conninfo import conninfo_to_dict, make_conninfo
from psycopg_pool import ConnectionPool

from mcp.server import MCPServer

from . import bulk, discovery, render
from .pager import CursorExpired, CursorRegistry
from .qlog import QueryLog, build_record
from .profiles import ProfileStore, redact_dsn
from .tunnel import Tunnel, TunnelError

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
    keepalive: float = 120.0  # ssh ServerAliveInterval + TCP keepalives_idle + pool max_idle
    connect_timeout: float = 5.0  # fail fast instead of hanging on dead hosts

    def as_dict(self) -> dict:
        return {
            "page_size": self.page_size,
            "max_page_size": self.max_page_size,
            "max_page_bytes": self.max_page_bytes,
            "max_cell_chars": self.max_cell,
            "cursor_ttl_s": self.cursor_ttl,
            "max_cursors": self.max_cursors,
            "statement_timeout_s": self.statement_timeout,
            "keepalive_s": self.keepalive,
            "connect_timeout_s": self.connect_timeout,
        }


def clamp_page_size(args: dict, limits: Limits) -> int:
    requested = args.get("page_size") or limits.page_size
    return max(1, min(int(requested), limits.max_page_size))


class Engine:
    """One profile's connection pool, optional SSH tunnel, and query logic."""

    def __init__(self, name: str, spec: dict, limits: Limits, registry: CursorRegistry):
        self.name = name
        self.allow_writes = bool(spec.get("allow_writes"))
        self.limits = limits
        self.registry = registry
        self.tunnel: Tunnel | None = None

        dsn = spec["dsn"]
        overrides: dict = {}
        ssh = spec.get("ssh")
        if ssh:
            info = conninfo_to_dict(dsn)
            remote_host = ssh.get("remote_host") or info.get("host") or "127.0.0.1"
            if remote_host == ssh["host"].split("@")[-1]:
                # DSN host == ssh host: from the remote's view that is localhost
                remote_host = "127.0.0.1"
            remote_port = int(ssh.get("remote_port") or info.get("port") or 5432)
            self.tunnel = Tunnel(ssh["host"], remote_host, remote_port, keepalive=limits.keepalive)
            overrides = {"host": "127.0.0.1", "port": self.tunnel.local_port}

        options = (
            f"-c statement_timeout={int(limits.statement_timeout * 1000)} "
            f"-c idle_in_transaction_session_timeout={int((limits.cursor_ttl + 60) * 1000)}"
        )
        conninfo = make_conninfo(
            dsn,
            options=options,
            connect_timeout=max(1, int(limits.connect_timeout)),
            # TCP keepalives: detect dead peers (~30s) even on pinned cursor
            # connections that never touch the pool's checkout check
            keepalives=1,
            keepalives_idle=max(1, int(limits.keepalive)),
            keepalives_interval=10,
            keepalives_count=3,
            **overrides,
        )
        self.pool = ConnectionPool(
            conninfo,
            min_size=0,
            max_size=limits.max_cursors + 2,
            max_idle=limits.keepalive,
            open=False,
            configure=self._configure_conn,
            # validate every checked-out connection with a cheap round-trip;
            # a stale one is discarded and replaced transparently
            check=ConnectionPool.check_connection,
        )
        self.pool.open()  # min_size=0: marks the pool usable, connects nothing

    def _configure_conn(self, conn):
        if not self.allow_writes:
            conn.read_only = True

    def tunnel_ok(self) -> bool:
        return self.tunnel is None or self.tunnel.alive()

    def close(self):
        self.registry.close_profile(self.name)
        self.pool.close()
        if self.tunnel is not None:
            self.tunnel.close()

    # -- query -------------------------------------------------------------

    def query(self, args: dict) -> dict:
        sql = args.get("sql", "").strip()
        if not sql:
            raise UserError("sql is required")
        self.pool.open()
        params = args.get("params") or None
        page_size = clamp_page_size(args, self.limits)
        timeout_s = args.get("timeout_s")

        if CURSORABLE.match(sql):
            try:
                return self._query_cursor(sql, params, page_size, timeout_s)
            except psycopg.errors.ProgrammingError as e:
                # e.g. data-modifying CTE: not DECLARE-able -> plain execution
                if "cursor" not in str(e).lower():
                    raise
        return self._query_plain(sql, params, page_size, timeout_s)

    def _query_cursor(self, sql: str, params, page_size: int, timeout_s=None) -> dict:
        estimated = self._estimate_rows(sql, params)
        conn = self.pool.getconn()
        try:
            bulk.apply_timeout(conn, timeout_s)
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
        try:
            rows, has_more = pq.fetch_page(page_size, self.limits.max_page_bytes, self.limits.max_cell)
        except Exception:
            self.registry.close(pq.token)  # aborted txn must not pin a connection
            raise
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

    def _query_plain(self, sql: str, params, page_size: int, timeout_s=None) -> dict:
        with self.pool.connection() as conn:
            bulk.apply_timeout(conn, timeout_s)
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

    def __init__(self, store: ProfileStore, limits: Limits, query_log: QueryLog | None = None):
        self.store = store
        self.limits = limits
        self.registry = CursorRegistry(limits.cursor_ttl, limits.max_cursors)
        self.engines: dict[str, Engine] = {}
        self.qlog = query_log
        self._lock = threading.Lock()  # engines dict + store mutations (HTTP: concurrent sessions)

    # -- dispatch ----------------------------------------------------------

    def dispatch(self, name: str, args: dict) -> dict:
        t0 = time.monotonic()
        with self._lock:
            # another session/process may have edited the profiles file
            for changed in self.store.maybe_reload():
                self._drop_engine_locked(changed)
        result = self._dispatch_inner(name, args)
        if self.qlog is not None and name != "logs":
            self.qlog.write(
                build_record(name, args, result,
                             (time.monotonic() - t0) * 1000, self.store.default)
            )
        return result

    def _dispatch_inner(self, name: str, args: dict) -> dict:
        try:
            handler = {
                "query": lambda a: self.engine(a.get("profile")).query(a),
                "tables": lambda a: self.engine(a.get("profile")).tables(a),
                "describe": lambda a: self.engine(a.get("profile")).describe(a),
                "explain": lambda a: self.engine(a.get("profile")).explain(a),
                "script": lambda a: bulk.script(self.engine(a.get("profile")), a),
                "export": lambda a: bulk.export(self.engine(a.get("profile")), a),
                "overview": lambda a: discovery.overview(self.engine(a.get("profile")), a),
                "search_objects": lambda a: discovery.search_objects(self.engine(a.get("profile")), a),
                "profile": lambda a: discovery.profile(self.engine(a.get("profile")), a),
                "relations": lambda a: discovery.relations(self.engine(a.get("profile")), a),
                "join_path": lambda a: discovery.join_path(self.engine(a.get("profile")), a),
                "count": lambda a: discovery.count(self.engine(a.get("profile")), a),
                "sample": lambda a: discovery.sample(self.engine(a.get("profile")), a),
                "fetch": self.fetch,
                "close": self.close_tool,
                "logs": self.logs,
                "status": self.status,
                "profiles": self.profiles_list,
                "profile_add": self.profile_add,
                "profile_remove": self.profile_remove,
                "profile_test": self.profile_test,
            }.get(name)
            if handler is None:
                return {"error": f"unknown tool: {name}"}
            return handler(args)
        except (CursorExpired, UserError, TunnelError) as e:
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
        with self._lock:
            eng = self.engines.get(name)
            if eng is not None and not eng.tunnel_ok():
                # tunnel died: cursors on it are gone, rebuild engine lazily
                self._drop_engine_locked(name)
                eng = None
            if eng is None:
                eng = Engine(name, spec, self.limits, self.registry)
                self.engines[name] = eng
            return eng

    # -- cursor tools ------------------------------------------------------

    def fetch(self, args: dict) -> dict:
        token = args.get("cursor", "")
        if not token:
            raise UserError("cursor is required")
        page_size = clamp_page_size(args, self.limits)
        pq = self.registry.get(token)
        eng = self.engines.get(pq.profile)
        if eng is not None and not eng.tunnel_ok():
            self._drop_engine(pq.profile)
            raise TunnelError(
                f"ssh tunnel for profile '{pq.profile}' went down — its cursors "
                "are invalidated; re-run the query (the tunnel restarts automatically)"
            )
        try:
            rows, has_more = pq.fetch_page(page_size, self.limits.max_page_bytes, self.limits.max_cell)
        except Exception:
            self.registry.close(token)  # dead connection/aborted txn: release
            raise
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
                    **({"ssh": spec["ssh"]} if spec.get("ssh") else {}),
                }
                for name, spec in self.store.profiles.items()
            ],
        }

    def profile_add(self, args: dict) -> dict:
        name = (args.get("name") or "").strip()
        dsn = (args.get("dsn") or "").strip()
        if not name or not dsn:
            raise UserError("name and dsn are required")
        ssh = None
        if args.get("ssh_host"):
            ssh = {"host": args["ssh_host"]}
            if args.get("ssh_remote_host"):
                ssh["remote_host"] = args["ssh_remote_host"]
            if args.get("ssh_remote_port"):
                ssh["remote_port"] = int(args["ssh_remote_port"])

        result = {"saved": name}
        test = args.get("test", True)
        if test and ssh is None:
            version, ms = self._probe(dsn)
            result["server"] = version
            result["connect_ms"] = ms

        old_spec = self.store.profiles.get(name)
        old_default = self.store.default
        self._drop_engine(name)  # replacing an existing profile: fresh pool
        self.store.upsert(
            name,
            dsn,
            allow_writes=bool(args.get("allow_writes", True)),
            description=args.get("description"),
            make_default=bool(args.get("make_default")),
            ssh=ssh,
        )
        if test and ssh is not None:
            # ssh profiles can only be probed through their tunnel: build the
            # engine, verify, and roll the store back if that fails
            t0 = time.monotonic()
            try:
                eng = self.engine(name)
                with eng.pool.connection() as conn:
                    version = conn.execute("select version()").fetchone()[0]
            except Exception:
                self._drop_engine(name)
                if old_spec is not None:
                    self.store.profiles[name] = old_spec
                    self.store.default = old_default
                    self.store.save()
                else:
                    self.store.remove(name)
                raise
            result["server"] = version.split(" on ")[0]
            result["connect_ms"] = round((time.monotonic() - t0) * 1000, 1)
            result["tunnel"] = eng.tunnel.describe()
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
        if spec.get("ssh"):
            t0 = time.monotonic()
            eng = self.engine(name)
            with eng.pool.connection() as conn:
                version = conn.execute("select version()").fetchone()[0]
            return {
                "profile": name,
                "server": version.split(" on ")[0],
                "connect_ms": round((time.monotonic() - t0) * 1000, 1),
                "tunnel": eng.tunnel.describe(),
            }
        version, ms = self._probe(spec["dsn"])
        return {"profile": name, "server": version, "connect_ms": ms}

    def _probe(self, dsn: str):
        t0 = time.monotonic()
        with psycopg.connect(dsn, connect_timeout=max(1, int(self.limits.connect_timeout))) as conn:
            version = conn.execute("select version()").fetchone()[0]
        return version.split(" on ")[0], round((time.monotonic() - t0) * 1000, 1)

    def _drop_engine(self, name: str):
        with self._lock:
            self._drop_engine_locked(name)

    def _drop_engine_locked(self, name: str):
        eng = self.engines.pop(name, None)
        if eng is not None:
            eng.close()

    # -- logs / status -----------------------------------------------------

    def logs(self, args: dict) -> dict:
        if self.qlog is None or not self.qlog.enabled:
            return {"error": "query log disabled (--log-days 0 or no log dir)"}
        entries = self.qlog.read(
            limit=min(int(args.get("limit") or 50), 500),
            tool=args.get("tool"),
            profile=args.get("profile"),
            since=args.get("since"),
        )
        return {"entries": entries, "log": self.qlog.status()}

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
                if eng.tunnel is not None:
                    entry["tunnel"] = eng.tunnel.describe()
            profs.append(entry)
        return {
            "default": self.store.default,
            "profiles": profs,
            "open_cursors": self.registry.snapshot(),
            "limits": self.limits.as_dict(),
            "profiles_file": str(self.store.path),
            "query_log": self.qlog.status() if self.qlog else {"enabled": False},
        }

    def shutdown(self):
        self.registry.close_all()
        for eng in self.engines.values():
            eng.close()
        if self.qlog is not None:
            self.qlog.close()


# -- MCP wiring ------------------------------------------------------------


def build_server(mgr: Manager) -> MCPServer:
    from . import __version__

    srv = MCPServer(
        name="database-mcp",
        title="Database MCP",
        version=__version__,
        website_url="https://github.com/thhart/database-mcp",
        description=(
            "PostgreSQL access with true server-side result paging, "
            "runtime-managed connection profiles (incl. SSH tunnels), and "
            "scan-free schema discovery tools."
        ),
        instructions=(
            "SQL access to PostgreSQL with TRUE server-side result paging.\n"
            "\n"
            "PAGING: run SQL with `query`; when page.has_more is true, continue "
            "with `fetch(cursor)` — the query is NOT re-executed, rows continue "
            "from a held server-side cursor with a stable MVCC snapshot. "
            "Results are compact: columns once, rows as arrays. For several "
            "related queries use `script` (one transaction, every result set "
            "back, labeled); for large results use `export` (streams to a "
            "local csv/jsonl file, nothing enters the context). Long analysis "
            "queries: pass timeout_s.\n"
            "\n"
            "UNKNOWN SCHEMA? Do NOT explore with ad-hoc SQL scans:\n"
            "- `overview` — every table + row estimate + column names, one call\n"
            "- `search_objects` — find tables/columns/functions by name OR comment\n"
            "- `profile` — value distributions from pg_stats, zero table access\n"
            "- `join_path` — shortest FK path as a ready JOIN chain (no guessed joins)\n"
            "- `count` — instant planner estimate; exact=true only when needed\n"
            "- `sample` — genuinely random rows (TABLESAMPLE, no LIMIT bias)\n"
            "\n"
            "CONNECTIONS are named profiles, managed at runtime: `profiles` "
            "lists them, every tool takes profile=, `profile_add` creates or "
            "changes one on the fly (ssh_host= opens a self-healing SSH tunnel "
            "for databases only reachable via a jump host). Profiles are "
            "write-enabled by default; create production profiles with "
            "allow_writes=false and treat them read-only."
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
        timeout_s: float | None = None,
    ) -> str:
        return await _call(
            "query",
            {"sql": sql, "params": params, "page_size": page_size,
             "profile": profile, "timeout_s": timeout_s},
        )

    @srv.tool(
        description=(
            "Run a multi-statement SQL script in ONE transaction and get EVERY "
            "result set back, each labeled by the comment line preceding its "
            "statement. Made for analysis sessions: several related queries in "
            "one round-trip instead of one call per query. Rows per statement "
            "are capped (rows_per_statement); use query+fetch for paging "
            "through one big result."
        )
    )
    async def script(
        sql: str,
        rows_per_statement: int | None = None,
        profile: str | None = None,
        timeout_s: float | None = None,
    ) -> str:
        return await _call(
            "script",
            {"sql": sql, "rows_per_statement": rows_per_statement,
             "profile": profile, "timeout_s": timeout_s},
        )

    @srv.tool(
        description=(
            "Stream a FULL result set into a local file (csv or jsonl) via a "
            "server-side cursor — any size, nothing enters the conversation "
            "context. Returns path, columns, row count, bytes. Use for "
            "materializing large analysis results for local tooling "
            "(pandas, duckdb, ...)."
        )
    )
    async def export(
        sql: str,
        format: str = "csv",
        path: str | None = None,
        max_rows: int = 1_000_000,
        params: list | None = None,
        profile: str | None = None,
        timeout_s: float | None = None,
    ) -> str:
        return await _call(
            "export",
            {"sql": sql, "format": format, "path": path, "max_rows": max_rows,
             "params": params, "profile": profile, "timeout_s": timeout_s},
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
        description=(
            "Orientation card for an unknown database: every table with row "
            "estimate and its column names in ONE compact call — use this "
            "before tables/describe round-trips. Optional table-name filter."
        )
    )
    async def overview(filter: str | None = None, profile: str | None = None) -> str:
        return await _call("overview", {"filter": filter, "profile": profile})

    @srv.tool(
        description=(
            "Find tables, columns, and functions by name OR by comment "
            "(pg_description — often the only documentation a schema has). "
            "Answers 'where is the customer email?' in one call."
        )
    )
    async def search_objects(term: str, limit: int = 50, profile: str | None = None) -> str:
        return await _call("search_objects", {"term": term, "limit": limit, "profile": profile})

    @srv.tool(
        description=(
            "Column statistics from pg_stats WITHOUT touching the table: "
            "null fraction, distinct count (negative = fraction of rows, "
            "-1 = unique), most common values with frequencies, histogram "
            "bounds, physical correlation. Replaces exploratory "
            "SELECT DISTINCT / GROUP BY scans."
        )
    )
    async def profile(table: str, profile: str | None = None) -> str:
        return await _call("profile", {"table": table, "profile": profile})

    @srv.tool(description="Foreign keys of one table, both directions: what it references and what references it.")
    async def relations(table: str, profile: str | None = None) -> str:
        return await _call("relations", {"table": table, "profile": profile})

    @srv.tool(
        description=(
            "Shortest foreign-key path between two tables, rendered as a "
            "ready-to-use JOIN chain (up to 3 equally short paths). Use this "
            "instead of guessing joins on unfamiliar schemas."
        )
    )
    async def join_path(
        from_table: str, to_table: str, max_hops: int = 4, profile: str | None = None
    ) -> str:
        return await _call(
            "join_path",
            {"from_table": from_table, "to_table": to_table, "max_hops": max_hops, "profile": profile},
        )

    @srv.tool(
        description=(
            "Row count, estimate-first: instant planner estimate (no scan), "
            "optionally with a WHERE clause; exact=true runs a real count(*) "
            "under the statement timeout. Prefer the estimate."
        )
    )
    async def count(
        table: str, where: str | None = None, exact: bool = False, profile: str | None = None
    ) -> str:
        return await _call(
            "count", {"table": table, "where": where, "exact": exact, "profile": profile}
        )

    @srv.tool(
        description=(
            "A few genuinely random rows from a table (TABLESAMPLE on big "
            "tables — no scan, no physically-adjacent LIMIT bias)."
        )
    )
    async def sample(table: str, n: int = 5, profile: str | None = None) -> str:
        return await _call("sample", {"table": table, "n": n, "profile": profile})

    @srv.tool(
        description="List all connection profiles (DSNs password-redacted) and which one is the default."
    )
    async def profiles() -> str:
        return await _call("profiles", {})

    @srv.tool(
        description=(
            "Add or update a named connection profile at runtime and persist it. "
            "Tests the connection first (set test=false to skip). Profiles are "
            "write-enabled by default; pass allow_writes=false for a read-only "
            "profile (recommended for production databases). make_default=true "
            "switches the default profile. For a database only reachable via "
            "SSH, set ssh_host (an ssh destination or ~/.ssh/config alias; "
            "BatchMode, so keys/agent must work non-interactively) — a tunnel "
            "is opened automatically and kept alive; ssh_remote_host/"
            "ssh_remote_port default to the DSN's host/port as seen FROM the "
            "ssh host (usually 127.0.0.1:5432)."
        )
    )
    async def profile_add(
        name: str,
        dsn: str,
        allow_writes: bool = True,
        description: str | None = None,
        make_default: bool = False,
        test: bool = True,
        ssh_host: str | None = None,
        ssh_remote_host: str | None = None,
        ssh_remote_port: int | None = None,
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
                "ssh_host": ssh_host,
                "ssh_remote_host": ssh_remote_host,
                "ssh_remote_port": ssh_remote_port,
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
            "Recent query-log entries (newest first): tool, profile, duration "
            "ms, row counts, truncated SQL, errors. Filter by tool/profile/"
            "since (ISO timestamp). The log lives as daily JSONL files — "
            "analyse big windows with duckdb/jq on log.dir instead of "
            "paging through this tool."
        )
    )
    async def logs(
        limit: int = 50,
        tool: str | None = None,
        profile: str | None = None,
        since: str | None = None,
    ) -> str:
        return await _call(
            "logs", {"limit": limit, "tool": tool, "profile": profile, "since": since}
        )

    @srv.tool(
        description=(
            "Server status: profiles with pool statistics, open cursors, "
            "configured limits, profiles file location, query-log state."
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
    parser.add_argument(
        "--read-only",
        action="store_true",
        help="register the --dsn profile read-only (writes are the default)",
    )
    parser.add_argument("--page-size", type=int, default=50)
    parser.add_argument("--max-page-size", type=int, default=500)
    parser.add_argument("--max-page-bytes", type=int, default=32_000)
    parser.add_argument("--max-cell", type=int, default=400)
    parser.add_argument("--cursor-ttl", type=float, default=300.0)
    parser.add_argument("--max-cursors", type=int, default=4)
    parser.add_argument("--statement-timeout", type=float, default=30.0)
    parser.add_argument(
        "--keepalive",
        type=float,
        default=120.0,
        help="seconds: ssh ServerAliveInterval, TCP keepalives_idle, pool max_idle",
    )
    parser.add_argument(
        "--connect-timeout",
        type=float,
        default=5.0,
        help="seconds before a connection attempt to a dead host fails",
    )
    parser.add_argument(
        "--log-dir",
        default=os.getenv("DATABASE_MCP_LOG_DIR")
        or os.path.expanduser("~/.local/state/database-mcp/log"),
        help="query log directory (JSONL, one file per day)",
    )
    parser.add_argument(
        "--log-days",
        type=int,
        default=14,
        help="query log retention in days (0 disables logging)",
    )
    parser.add_argument(
        "--http",
        action="store_true",
        help="run as a shared HTTP daemon (bridge): ONE process serves ALL "
        "Claude sessions — shared profiles, pools, tunnels, cursors, log",
    )
    parser.add_argument("--host", default="127.0.0.1", help="HTTP bind address")
    parser.add_argument("--port", type=int, default=4270, help="HTTP port")
    args = parser.parse_args()

    store = ProfileStore(args.profiles)
    if args.dsn:
        store.upsert("default", args.dsn, allow_writes=not args.read_only, make_default=True)
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
            keepalive=args.keepalive,
            connect_timeout=args.connect_timeout,
        ),
        query_log=QueryLog(args.log_dir, args.log_days),
    )
    try:
        srv = build_server(mgr)
        if args.http:
            asyncio.run(
                srv.run_streamable_http_async(
                    host=args.host, port=args.port, stateless_http=True
                )
            )
        else:
            asyncio.run(srv.run_stdio_async())
    finally:
        mgr.shutdown()


if __name__ == "__main__":
    run()
