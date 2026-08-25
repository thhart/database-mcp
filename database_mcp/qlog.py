"""Structured query logging with bounded retention.

Every tool call is appended as one JSON line to a daily file
(<dir>/query-YYYYMMDD.jsonl): timestamp, tool, profile, duration_ms,
row counts, truncated SQL, error/sqlstate. Built for analysis (duckdb,
jq, pandas — or the `logs` tool right in the server).

Retention is bounded by design: daily rotation, files older than
`days` are deleted at startup and on every rollover. SQL is truncated
(2000 chars) and profile DSNs are never logged.
"""

import json
import os
import re
import threading
import time
from datetime import datetime, timedelta

SQL_CAP = 2000
FILE_RE = re.compile(r"^query-(\d{8})\.jsonl$")


class QueryLog:
    def __init__(self, log_dir: str, days: int = 14):
        self.dir = log_dir
        self.days = days
        self._lock = threading.Lock()
        self._current_day = ""
        self._fh = None
        if self.enabled:
            os.makedirs(self.dir, exist_ok=True)
            self.prune()

    @property
    def enabled(self) -> bool:
        return bool(self.dir) and self.days > 0

    def write(self, record: dict):
        if not self.enabled:
            return
        try:
            line = json.dumps(
                {k: v for k, v in record.items() if v is not None},
                ensure_ascii=False,
                default=str,
            )
            day = time.strftime("%Y%m%d")
            with self._lock:
                if day != self._current_day:
                    if self._fh:
                        self._fh.close()
                    self._current_day = day
                    self._fh = open(
                        os.path.join(self.dir, f"query-{day}.jsonl"), "a", encoding="utf-8"
                    )
                    self.prune()
                self._fh.write(line + "\n")
                self._fh.flush()
        except Exception:
            pass  # logging must never break a tool call

    def prune(self):
        cutoff = (datetime.now() - timedelta(days=self.days)).strftime("%Y%m%d")
        try:
            for name in os.listdir(self.dir):
                m = FILE_RE.match(name)
                if m and m.group(1) < cutoff:
                    os.unlink(os.path.join(self.dir, name))
        except Exception:
            pass

    def read(self, limit: int = 50, tool: str | None = None,
             profile: str | None = None, since: str | None = None) -> list[dict]:
        """Most recent entries first, newest files first."""
        if not self.enabled:
            return []
        out: list[dict] = []
        try:
            files = sorted(
                (n for n in os.listdir(self.dir) if FILE_RE.match(n)), reverse=True
            )
            for name in files:
                with open(os.path.join(self.dir, name), encoding="utf-8") as f:
                    lines = f.readlines()
                for line in reversed(lines):
                    try:
                        e = json.loads(line)
                    except Exception:
                        continue
                    if tool and e.get("tool") != tool:
                        continue
                    if profile and e.get("profile") != profile:
                        continue
                    if since and e.get("ts", "") < since:
                        continue
                    out.append(e)
                    if len(out) >= limit:
                        return out
                if since and lines:
                    first = json.loads(lines[0]) if lines else {}
                    if first.get("ts", "") < since:
                        break  # older files cannot match
        except Exception:
            pass
        return out

    def status(self) -> dict:
        if not self.enabled:
            return {"enabled": False}
        total = 0
        files = 0
        try:
            for name in os.listdir(self.dir):
                if FILE_RE.match(name):
                    files += 1
                    total += os.path.getsize(os.path.join(self.dir, name))
        except Exception:
            pass
        return {"enabled": True, "dir": self.dir, "retention_days": self.days,
                "files": files, "total_bytes": total}

    def close(self):
        with self._lock:
            if self._fh:
                self._fh.close()
                self._fh = None


def build_record(tool: str, args: dict, result: dict, ms: float, default_profile) -> dict:
    """Compact, analysis-friendly record; never leaks DSNs or full payloads."""
    rec: dict = {
        "ts": datetime.now().isoformat(timespec="milliseconds"),
        "tool": tool,
        "profile": args.get("profile") or default_profile,
        "ms": round(ms, 1),
    }
    sql = args.get("sql")
    if isinstance(sql, str):
        rec["sql"] = " ".join(sql.split())[:SQL_CAP]
    for key in ("cursor", "table", "term", "name"):
        if args.get(key) and tool != "profile_add":
            rec[key] = args[key]
    if tool == "profile_add":
        rec["name"] = args.get("name")  # never the dsn
    if isinstance(result, dict):
        if "error" in result:
            rec["ok"] = False
            rec["error"] = str(result["error"])[:300]
            if result.get("sqlstate"):
                rec["sqlstate"] = result["sqlstate"]
        else:
            rec["ok"] = True
            page = result.get("page")
            if isinstance(page, dict):
                rec["rows"] = page.get("returned")
                if page.get("has_more"):
                    rec["has_more"] = True
                if page.get("estimated_rows") is not None:
                    rec["estimated_rows"] = page.get("estimated_rows")
            elif isinstance(result.get("rows"), list):
                rec["rows"] = len(result["rows"])
            if result.get("rowcount") is not None:
                rec["rowcount"] = result["rowcount"]
            if tool == "export":
                rec["rows"] = result.get("rows")
                rec["bytes"] = result.get("bytes")
            if tool == "script":
                rec["statements"] = result.get("statements")
    return rec
