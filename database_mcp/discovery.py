"""Discovery & speed-up tools for heterogeneous schemas.

These answer the questions an AI consumer otherwise burns full scans and
failed joins on: what is here (overview, search_objects), what does the
data look like (profile via pg_stats — zero table access, sample via
TABLESAMPLE), how do tables relate (relations, join_path over the FK
graph), and how many rows match (count, estimate-first).

All identifiers pass through %s::regclass server-side resolution; embedded
names come back server-quoted from oid::regclass::text.
"""

from collections import deque

from .render import convert_row

SYSTEM_SCHEMAS = "('pg_catalog','information_schema')"
RELKINDS = "('r','v','m','p','f')"

FK_EDGES_SQL = """
    select c.conrelid, c.confrelid, c.conname,
           (select array_agg(a.attname order by u.ord)
              from unnest(c.conkey) with ordinality u(attnum, ord)
              join pg_attribute a on a.attrelid = c.conrelid and a.attnum = u.attnum),
           (select array_agg(a.attname order by u.ord)
              from unnest(c.confkey) with ordinality u(attnum, ord)
              join pg_attribute a on a.attrelid = c.confrelid and a.attnum = u.attnum)
    from pg_constraint c
    where c.contype = 'f'
"""


def _resolve(conn, table: str) -> int:
    return conn.execute("select %s::regclass::oid", [table]).fetchone()[0]


def _names(conn, oids: list[int]) -> dict[int, str]:
    rows = conn.execute(
        "select oid, oid::regclass::text from pg_class where oid = any(%s)", [oids]
    ).fetchall()
    return dict(rows)


def overview(engine, args: dict) -> dict:
    """One compact orientation card: every table with row estimate + columns."""
    name_filter = args.get("filter")
    sql = f"""
        select n.nspname || '.' || c.relname, c.relkind::text,
               greatest(c.reltuples, 0)::bigint,
               (select string_agg(a.attname, ',' order by a.attnum)
                  from pg_attribute a
                 where a.attrelid = c.oid and a.attnum > 0 and not a.attisdropped)
        from pg_class c
        join pg_namespace n on n.oid = c.relnamespace
        where c.relkind in {RELKINDS}
          and n.nspname not in {SYSTEM_SCHEMAS}
          and n.nspname !~ '^pg_toast'
    """
    params: list = []
    if name_filter:
        sql += " and c.relname ilike %s"
        params.append(f"%{name_filter}%")
    sql += " order by 1"
    kinds = {"r": "table", "v": "view", "m": "matview", "p": "partitioned", "f": "foreign"}
    with engine.pool.connection() as conn:
        raw = conn.execute(sql, params).fetchall()
    rows = []
    for name, kind, est, cols in raw:
        col_list = (cols or "").split(",")
        if len(col_list) > 40:
            cols = ",".join(col_list[:40]) + f",…+{len(col_list) - 40}"
        rows.append([name, kinds.get(kind, kind), est, cols])
    return {"columns": ["table", "kind", "est_rows", "columns"], "rows": rows}


def search_objects(engine, args: dict) -> dict:
    """Find tables/columns/functions by name OR comment — comments are often
    the only documentation a schema has."""
    term = (args.get("term") or "").strip()
    if not term:
        return {"error": "term is required"}
    limit = min(int(args.get("limit") or 50), 200)
    pat = f"%{term}%"
    sql = f"""
        (select 'table' as kind, n.nspname || '.' || c.relname as name,
                null as datatype, obj_description(c.oid, 'pg_class') as comment
           from pg_class c join pg_namespace n on n.oid = c.relnamespace
          where c.relkind in {RELKINDS} and n.nspname not in {SYSTEM_SCHEMAS}
            and n.nspname !~ '^pg_toast'
            and (c.relname ilike %s or obj_description(c.oid, 'pg_class') ilike %s))
        union all
        (select 'column', n.nspname || '.' || c.relname || '.' || a.attname,
                format_type(a.atttypid, a.atttypmod),
                col_description(c.oid, a.attnum)
           from pg_attribute a
           join pg_class c on c.oid = a.attrelid
           join pg_namespace n on n.oid = c.relnamespace
          where c.relkind in {RELKINDS} and n.nspname not in {SYSTEM_SCHEMAS}
            and n.nspname !~ '^pg_toast'
            and a.attnum > 0 and not a.attisdropped
            and (a.attname ilike %s or col_description(c.oid, a.attnum) ilike %s))
        union all
        (select 'function', n.nspname || '.' || p.proname,
                pg_get_function_result(p.oid),
                obj_description(p.oid, 'pg_proc')
           from pg_proc p join pg_namespace n on n.oid = p.pronamespace
          where n.nspname not in {SYSTEM_SCHEMAS} and p.proname ilike %s)
        limit %s
    """
    with engine.pool.connection() as conn:
        rows = conn.execute(sql, [pat, pat, pat, pat, pat, limit]).fetchall()
    return {
        "columns": ["kind", "name", "type", "comment"],
        "rows": [convert_row(r, engine.limits.max_cell) for r in rows],
    }


def profile(engine, args: dict) -> dict:
    """Column statistics straight from pg_stats: value distributions without
    touching the table. n_distinct < 0 means a fraction of the row count
    (-1 = all distinct)."""
    table = (args.get("table") or "").strip()
    if not table:
        return {"error": "table is required"}
    with engine.pool.connection() as conn:
        oid = _resolve(conn, table)
        schema, relname, est = conn.execute(
            """
            select n.nspname, c.relname, greatest(c.reltuples, 0)::bigint
            from pg_class c join pg_namespace n on n.oid = c.relnamespace
            where c.oid = %s
            """,
            [oid],
        ).fetchone()
        activity = conn.execute(
            """
            select greatest(last_analyze, last_autoanalyze)::text,
                   n_live_tup, n_dead_tup
            from pg_stat_user_tables where relid = %s
            """,
            [oid],
        ).fetchone()
        stats = conn.execute(
            """
            select a.attname, format_type(a.atttypid, a.atttypmod),
                   s.null_frac, s.n_distinct, s.avg_width,
                   s.most_common_vals::text, s.most_common_freqs::text,
                   s.histogram_bounds::text, s.correlation
            from pg_attribute a
            left join pg_stats s
                   on s.schemaname = %s and s.tablename = %s and s.attname = a.attname
            where a.attrelid = %s and a.attnum > 0 and not a.attisdropped
            order by a.attnum
            """,
            [schema, relname, oid],
        ).fetchall()
    result = {
        "table": f"{schema}.{relname}",
        "est_rows": est,
        "columns": ["column", "type", "null_frac", "n_distinct", "avg_width",
                    "common_vals", "common_freqs", "histogram", "correlation"],
        "rows": [convert_row(r, engine.limits.max_cell) for r in stats],
    }
    if activity:
        result["last_analyze"] = activity[0]
        result["live_rows"] = activity[1]
        result["dead_rows"] = activity[2]
    if all(r[2] is None for r in stats):
        result["note"] = "no statistics — run ANALYZE on this table first"
    return result


def relations(engine, args: dict) -> dict:
    """Foreign keys of one table, both directions."""
    table = (args.get("table") or "").strip()
    if not table:
        return {"error": "table is required"}
    with engine.pool.connection() as conn:
        oid = _resolve(conn, table)
        out = conn.execute(
            """
            select confrelid::regclass::text, conname, pg_get_constraintdef(oid)
            from pg_constraint where conrelid = %s and contype = 'f' order by 1
            """,
            [oid],
        ).fetchall()
        inc = conn.execute(
            """
            select conrelid::regclass::text, conname, pg_get_constraintdef(oid)
            from pg_constraint where confrelid = %s and contype = 'f' order by 1
            """,
            [oid],
        ).fetchall()
    return {
        "table": table,
        "references": [[t, f"{n}: {d}"] for t, n, d in out],
        "referenced_by": [[t, f"{n}: {d}"] for t, n, d in inc],
    }


def join_path(engine, args: dict) -> dict:
    """Shortest FK path(s) between two tables, rendered as a ready JOIN chain.

    Traverses the FK graph in both directions (a child can be joined to its
    parent and vice versa). Returns up to 3 equally short paths.
    """
    a = (args.get("from_table") or "").strip()
    b = (args.get("to_table") or "").strip()
    if not a or not b:
        return {"error": "from_table and to_table are required"}
    max_hops = min(int(args.get("max_hops") or 4), 6)
    with engine.pool.connection() as conn:
        start, goal = _resolve(conn, a), _resolve(conn, b)
        edges = conn.execute(FK_EDGES_SQL).fetchall()
        if start == goal:
            return {"error": "from_table and to_table are the same relation"}
        # adjacency over the undirected FK graph
        adj: dict[int, list[tuple]] = {}
        for rel, frel, conname, cols, fcols in edges:
            adj.setdefault(rel, []).append((frel, cols, fcols))
            adj.setdefault(frel, []).append((rel, fcols, cols))
        # BFS collecting up to 3 shortest paths
        paths: list[list[tuple]] = []
        queue: deque = deque([(start, [])])
        best: dict[int, int] = {start: 0}
        found_len = None
        while queue:
            node, trail = queue.popleft()
            if found_len is not None and len(trail) >= found_len:
                continue
            for nxt, cols, fcols in adj.get(node, []):
                hop = (node, nxt, cols, fcols)
                if nxt == goal:
                    paths.append(trail + [hop])
                    found_len = len(trail) + 1
                    if len(paths) >= 3:
                        queue.clear()
                    continue
                depth = len(trail) + 1
                if depth >= max_hops:
                    continue
                if best.get(nxt, 10**9) > depth:
                    best[nxt] = depth
                    queue.append((nxt, trail + [hop]))
        if not paths:
            return {
                "from": a, "to": b,
                "error": f"no FK path within {max_hops} hops — join manually "
                         "or raise max_hops",
            }
        oids = {start, goal}
        for p in paths:
            for n1, n2, _, _ in p:
                oids.update((n1, n2))
        names = _names(conn, list(oids))
    rendered = []
    for p in paths:
        clause = f"FROM {names[start]}"
        for n1, n2, cols, fcols in p:
            on = " and ".join(
                f"{names[n1]}.{c} = {names[n2]}.{fc}" for c, fc in zip(cols, fcols)
            )
            clause += f" JOIN {names[n2]} ON {on}"
        rendered.append({"hops": len(p), "join": clause})
    return {"from": names[start], "to": names[goal], "paths": rendered}


def count(engine, args: dict) -> dict:
    """Row count, estimate-first: the planner answers instantly; exact=true
    runs a real count(*) under the statement timeout."""
    table = (args.get("table") or "").strip()
    if not table:
        return {"error": "table is required"}
    where = (args.get("where") or "").strip()
    with engine.pool.connection() as conn:
        oid = _resolve(conn, table)
        name = conn.execute("select %s::regclass::text", [oid]).fetchone()[0]
        sql = f"select * from {name}" + (f" where {where}" if where else "")
        if args.get("exact"):
            exact = conn.execute(
                f"select count(*) from {name}" + (f" where {where}" if where else "")
            ).fetchone()[0]
            return {"table": name, "where": where or None, "exact": exact}
        plan = conn.execute("EXPLAIN (FORMAT JSON) " + sql).fetchone()[0]
        est = int(plan[0]["Plan"]["Plan Rows"])
    return {
        "table": name,
        "where": where or None,
        "estimated": est,
        "note": "planner estimate; pass exact=true for a real count(*)",
    }


def sample(engine, args: dict) -> dict:
    """A few genuinely random rows. Big tables use TABLESAMPLE (no scan, no
    physically-adjacent LIMIT bias); small ones ORDER BY random()."""
    table = (args.get("table") or "").strip()
    if not table:
        return {"error": "table is required"}
    n = min(int(args.get("n") or 5), 50)
    with engine.pool.connection() as conn:
        oid = _resolve(conn, table)
        name, est = conn.execute(
            "select c.oid::regclass::text, greatest(c.reltuples, 0)::bigint "
            "from pg_class c where c.oid = %s",
            [oid],
        ).fetchone()
        rows: list = []
        if est > 10_000:
            pct = min(100.0, max(0.01, n * 500.0 / est))
            cur = conn.execute(
                f"select * from {name} tablesample system ({pct:.4f}) limit {n}"
            )
            rows = cur.fetchall()
            columns = [d.name for d in cur.description]
        if len(rows) < n:  # small table, or unlucky block sampling
            cur = conn.execute(f"select * from {name} order by random() limit {n}")
            rows = cur.fetchall()
            columns = [d.name for d in cur.description]
    return {
        "table": name,
        "est_rows": est,
        "columns": columns,
        "rows": [convert_row(r, engine.limits.max_cell) for r in rows],
    }
