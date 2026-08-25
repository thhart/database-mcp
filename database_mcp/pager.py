"""Server-side cursor paging.

The core differentiator of this MCP server: a query is executed ONCE as a
PostgreSQL server-side cursor (DECLARE ... / FETCH FORWARD) inside a held
transaction on a pinned pool connection. Subsequent pages are fetched from
the open cursor — no re-execution, no OFFSET re-scan, and the MVCC snapshot
keeps pagination stable even under concurrent writes.

Held cursors are bounded: at most `max_cursors` concurrently (LRU-evicted
beyond that), each evicted after `ttl_seconds` idle. The server-side
`idle_in_transaction_session_timeout` acts as a backstop so an abandoned
client can never pin a connection forever.
"""

import secrets
import threading
import time as _time

from .render import convert_row, row_size

# fetchmany chunk while filling a page; small enough to respect byte caps
# without a round-trip per row.
FETCH_CHUNK = 64


class CursorExpired(Exception):
    """The cursor token is unknown — expired, evicted, or never existed."""


class PagedQuery:
    """One held server-side cursor plus its pinned connection."""

    def __init__(self, token, conn, cursor, columns, pool, profile):
        self.token = token
        self.conn = conn
        self.cursor = cursor
        self.columns = columns
        self.pool = pool  # the owning profile's pool, for release
        self.profile = profile
        self.rows_delivered = 0
        self.created = _time.monotonic()
        self.last_access = self.created
        self._lookahead = None  # one prefetched row, basis for has_more

    def fetch_page(self, page_size: int, max_bytes: int, max_cell: int):
        """Fetch up to page_size rows (or max_bytes rendered), plus has_more.

        Returns (rows, has_more). Always returns at least one row if one is
        available, even if it alone exceeds max_bytes.
        """
        self.last_access = _time.monotonic()
        rows = []
        used = 0
        while len(rows) < page_size:
            row = self._next_row()
            if row is None:
                break
            converted = convert_row(row, max_cell)
            size = row_size(converted)
            if rows and used + size > max_bytes:
                self._lookahead = row  # push back for the next page
                break
            rows.append(converted)
            used += size
        if self._lookahead is None:
            # peek one row so has_more is exact, kept for the next page
            self._lookahead = self._raw_next()
        has_more = self._lookahead is not None
        self.rows_delivered += len(rows)
        return rows, has_more

    def _next_row(self):
        if self._lookahead is not None:
            row, self._lookahead = self._lookahead, None
            return row
        return self._raw_next()

    def _raw_next(self):
        batch = self.cursor.fetchmany(1)
        return batch[0] if batch else None


class CursorRegistry:
    """token -> PagedQuery with TTL and LRU bounds, across ALL profiles.

    Thread-safe. Each PagedQuery carries its own pool, so one registry can
    hold cursors from any number of connection profiles.
    """

    def __init__(self, ttl_seconds: float, max_cursors: int):
        self.ttl = ttl_seconds
        self.max_cursors = max_cursors
        self._lock = threading.Lock()
        self._cursors: dict[str, PagedQuery] = {}

    def open(self, conn, cursor, columns, pool, profile) -> PagedQuery:
        token = "c_" + secrets.token_hex(4)
        pq = PagedQuery(token, conn, cursor, columns, pool, profile)
        with self._lock:
            self._evict_expired_locked()
            while len(self._cursors) >= self.max_cursors:
                oldest = min(self._cursors.values(), key=lambda c: c.last_access)
                self._release_locked(oldest)
            self._cursors[token] = pq
        return pq

    def get(self, token: str) -> PagedQuery:
        with self._lock:
            self._evict_expired_locked()
            pq = self._cursors.get(token)
        if pq is None:
            raise CursorExpired(
                f"cursor '{token}' not found — it expired (idle > {self.ttl:.0f}s), "
                "was evicted, or was closed. Re-run the query to start over."
            )
        return pq

    def close(self, token: str) -> bool:
        with self._lock:
            pq = self._cursors.get(token)
            if pq is None:
                return False
            self._release_locked(pq)
            return True

    def close_all(self) -> int:
        with self._lock:
            n = len(self._cursors)
            for pq in list(self._cursors.values()):
                self._release_locked(pq)
            return n

    def close_profile(self, profile: str) -> int:
        """Release every cursor belonging to one profile (dropped/changed)."""
        with self._lock:
            mine = [pq for pq in self._cursors.values() if pq.profile == profile]
            for pq in mine:
                self._release_locked(pq)
            return len(mine)

    def sweep(self) -> None:
        with self._lock:
            self._evict_expired_locked()

    def snapshot(self) -> list[dict]:
        now = _time.monotonic()
        with self._lock:
            return [
                {
                    "cursor": pq.token,
                    "profile": pq.profile,
                    "rows_delivered": pq.rows_delivered,
                    "age_s": round(now - pq.created, 1),
                    "idle_s": round(now - pq.last_access, 1),
                }
                for pq in self._cursors.values()
            ]

    # -- internal ----------------------------------------------------------

    def _evict_expired_locked(self):
        now = _time.monotonic()
        for pq in [c for c in self._cursors.values() if now - c.last_access > self.ttl]:
            self._release_locked(pq)

    def _release_locked(self, pq: PagedQuery):
        self._cursors.pop(pq.token, None)
        try:
            pq.cursor.close()
        except Exception:
            pass
        try:
            pq.conn.rollback()
        except Exception:
            pass
        try:
            pq.pool.putconn(pq.conn)
        except Exception:
            pass
