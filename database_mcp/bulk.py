"""Analyst workflows: multi-statement scripts and full-result exports.

Born from watching a real analysis session: analysts bundle several labeled
queries into one script, and materialize big intermediate results. `script`
runs a whole SQL script in one transaction and returns EVERY result set
(labeled by the comment preceding each statement). `export` streams a full
result set through a server-side cursor into a local CSV/JSONL file —
arbitrarily large results without touching the model's context.
"""

import csv
import json
import os
import re
import tempfile
import time

from .render import convert_row

_DOLLAR = re.compile(r"\$[A-Za-z_0-9]*\$")


def split_statements(sql: str) -> list[tuple[str | None, str]]:
    """Split a script into (label, statement) pairs.

    Quote-aware (single/double/dollar quoting, line and block comments).
    A line comment directly before a statement becomes its label.
    """
    stmts: list[tuple[str | None, str]] = []
    cur: list[str] = []
    pending_label: str | None = None
    i, n = 0, len(sql)
    state: str | None = None
    dollar_tag = ""

    def flush():
        nonlocal cur, pending_label
        s = "".join(cur).strip()
        if s:
            stmts.append((pending_label, s))
            pending_label = None
        cur = []

    while i < n:
        ch = sql[i]
        nxt = sql[i + 1] if i + 1 < n else ""
        if state is None:
            if ch == "'":
                state = "sq"
                cur.append(ch)
            elif ch == '"':
                state = "dq"
                cur.append(ch)
            elif ch == "-" and nxt == "-":
                j = sql.find("\n", i)
                j = n if j < 0 else j
                comment = sql[i + 2 : j].strip()
                if not "".join(cur).strip() and comment:
                    pending_label = comment
                i = j
                continue
            elif ch == "/" and nxt == "*":
                j = sql.find("*/", i + 2)
                i = n if j < 0 else j + 2
                continue
            elif ch == "$":
                m = _DOLLAR.match(sql, i)
                if m:
                    dollar_tag = m.group(0)
                    state = "dollar"
                    cur.append(dollar_tag)
                    i += len(dollar_tag)
                    continue
                cur.append(ch)
            elif ch == ";":
                flush()
            else:
                cur.append(ch)
        elif state == "sq":
            cur.append(ch)
            if ch == "'":
                if nxt == "'":
                    cur.append(nxt)
                    i += 1
                else:
                    state = None
        elif state == "dq":
            cur.append(ch)
            if ch == '"':
                state = None
        elif state == "dollar":
            if sql.startswith(dollar_tag, i):
                cur.append(dollar_tag)
                i += len(dollar_tag)
                state = None
                continue
            cur.append(ch)
        if state != "dollar" or not sql.startswith(dollar_tag, i):
            i += 1
    flush()
    return stmts


def apply_timeout(conn, timeout_s):
    """SET LOCAL statement_timeout — reverts automatically at txn end."""
    if timeout_s:
        ms = int(min(float(timeout_s), 600.0) * 1000)
        conn.execute("select set_config('statement_timeout', %s, true)", [str(ms)])


def script(engine, args: dict) -> dict:
    """Run a multi-statement SQL script in ONE transaction; return every
    result set, labeled by the comment preceding each statement."""
    sql = (args.get("sql") or "").strip()
    if not sql:
        return {"error": "sql is required"}
    statements = split_statements(sql)
    if not statements:
        return {"error": "no statements found"}
    per_stmt = max(1, min(int(args.get("rows_per_statement") or engine.limits.page_size),
                          engine.limits.max_page_size))
    budget = engine.limits.max_page_bytes * 4
    used = 0
    results = []
    with engine.pool.connection() as conn:
        apply_timeout(conn, args.get("timeout_s"))
        for idx, (label, stmt) in enumerate(statements):
            entry: dict = {"label": label or f"statement {idx + 1}"}
            try:
                cur = conn.execute(stmt)
            except Exception as e:
                entry["error"] = str(e).strip()
                sqlstate = getattr(e, "sqlstate", None)
                if sqlstate:
                    entry["sqlstate"] = sqlstate
                results.append(entry)
                results.append({"note": f"transaction aborted — "
                                        f"{len(statements) - idx - 1} later statements skipped"})
                break
            if cur.description is None:
                entry["status"] = cur.statusmessage
                entry["rowcount"] = cur.rowcount
            else:
                raw = cur.fetchmany(per_stmt + 1)
                truncated = len(raw) > per_stmt
                rows = [convert_row(r, engine.limits.max_cell) for r in raw[:per_stmt]]
                entry["columns"] = [d.name for d in cur.description]
                entry["rows"] = rows
                if truncated:
                    entry["truncated"] = True
                used += sum(len(str(r)) for r in rows)
            results.append(entry)
            if used > budget and idx + 1 < len(statements):
                results.append({"note": f"byte budget exhausted — "
                                        f"{len(statements) - idx - 1} later statements skipped"})
                break
    return {"statements": len(statements), "results": results}


def export(engine, args: dict) -> dict:
    """Stream a full result set into a local file via a server-side cursor —
    any size, zero context pollution."""
    sql = (args.get("sql") or "").strip()
    if not sql:
        return {"error": "sql is required"}
    fmt = (args.get("format") or "csv").lower()
    if fmt not in ("csv", "jsonl"):
        return {"error": "format must be csv or jsonl"}
    max_rows = int(args.get("max_rows") or 1_000_000)
    path = args.get("path")
    if not path:
        out_dir = os.path.join(tempfile.gettempdir(), "database-mcp")
        os.makedirs(out_dir, exist_ok=True)
        path = os.path.join(out_dir, f"export-{time.strftime('%Y%m%d-%H%M%S')}.{fmt}")
    path = os.path.abspath(path)

    rows_written = 0
    truncated = False
    with engine.pool.connection() as conn:
        apply_timeout(conn, args.get("timeout_s"))
        cur = conn.cursor(name="exp_" + os.urandom(4).hex())
        cur.itersize = 5000
        cur.execute(sql, args.get("params") or None)
        first = cur.fetchmany(1)
        if cur.description is None:
            return {"error": "statement returns no rows — export needs a result set"}
        columns = [d.name for d in cur.description]
        with open(path, "w", newline="", encoding="utf-8") as f:
            if fmt == "csv":
                w = csv.writer(f)
                w.writerow(columns)
                writerow = lambda r: w.writerow(["" if v is None else v for v in r])
            else:
                huge = 10**9  # no cell truncation in exports
                writerow = lambda r: f.write(
                    json.dumps(dict(zip(columns, convert_row(r, huge))),
                               ensure_ascii=False, default=str) + "\n"
                )
            batch = first
            while batch:
                for row in batch:
                    if rows_written >= max_rows:
                        truncated = True
                        break
                    writerow(row)
                    rows_written += 1
                if truncated:
                    break
                batch = cur.fetchmany(5000)
        cur.close()
    return {
        "path": path,
        "format": fmt,
        "columns": columns,
        "rows": rows_written,
        "bytes": os.path.getsize(path),
        "truncated": truncated,
    }
