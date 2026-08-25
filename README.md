# database-mcp

SQL database MCP server with **true server-side result paging** — the feature
no established database MCP server has (DBHub caps rows, Google's MCP Toolbox
returns everything, mcp-alchemy truncates at 4000 chars).

PostgreSQL reference implementation.

## Why

Every existing SQL MCP server either truncates large results or dumps them
whole into the model's context. The MCP spec only paginates *list* operations
(`tools/list`), not tool results. database-mcp closes that gap:

- A query runs **once** as a PostgreSQL server-side cursor
  (`DECLARE`/`FETCH FORWARD`) inside a held transaction.
- Each `fetch(cursor)` continues **exactly** where the last page ended —
  no re-execution, no `OFFSET` re-scan, and the MVCC snapshot keeps the
  result stable even under concurrent writes.
- Pages are bounded by rows (`page_size`) **and** rendered bytes
  (`max_page_bytes`); oversized cells are truncated with an explicit marker.
- Held cursors are bounded: max N concurrent (LRU eviction), TTL idle
  eviction, plus `idle_in_transaction_session_timeout` as a server-side
  backstop. Exhausted cursors auto-close.

## Connection profiles — managed by the AI at runtime

Connections are named profiles, persisted in
`~/.config/database-mcp/profiles.json` (chmod 600). The AI can add, change,
test, and remove them on the fly via tools — no server restart:

- `profile_add(name, dsn, allow_writes=false, description, make_default, test=true)`
- `profile_remove(name)` · `profile_test(name)` · `profiles()`
- every query tool takes an optional `profile` parameter; the default profile
  is used when omitted.

Profiles are **write-enabled by default**. For production databases create
the profile with `allow_writes=false` — that enforces read-only at the
session level (`default_transaction_read_only`), so no statement can write
regardless of what the model sends. The CLI `--dsn` profile follows the same
default; pass `--read-only` to register it read-only.

## SSH bridging

A profile can reach a database that is only accessible via SSH (the classic
"Postgres listens on localhost of a remote host" setup):

```
profile_add(name="prod", dsn="postgresql://app@dbhost:5432/app",
            ssh_host="dbhost")
```

- The tunnel is a system-`ssh` subprocess (`-N -L`, BatchMode, keepalives) —
  your `~/.ssh/config`, keys, and agent apply unchanged. Auth must work
  non-interactively.
- `ssh_remote_host`/`ssh_remote_port` default to the DSN's host/port as seen
  *from* the SSH host; if the DSN host equals the SSH host it defaults to
  `127.0.0.1` (the usual case).
- Tunnels start lazily, are health-checked on every use, and are rebuilt
  automatically. If a tunnel dies mid-pagination, its cursors are invalidated
  with a clear error and the next query reconnects.
- Multiplexing (`ControlMaster`) is explicitly disabled for tunnel
  connections so the tunnel's lifetime is exactly the subprocess's lifetime.

## Tools

| Tool | Purpose |
|---|---|
| `query` | Execute SQL, get first page + `cursor` when more rows exist |
| `fetch` | Next page from a held cursor — no re-execution |
| `close` | Close one/all cursors early |
| `tables` | List tables/views with row estimates and sizes |
| `describe` | Columns, constraints, indexes of one table |
| `explain` | Query plan (optionally `analyze`) |
| `script` | Multi-statement SQL in one transaction — every result set back, labeled by preceding comments |
| `export` | Stream a full result set to a local csv/jsonl file via server-side cursor — any size, zero context cost |
| `overview` | Orientation card: every table + row estimate + column names in one call |
| `search_objects` | Find tables/columns/functions by name **or comment** |
| `profile` | Column statistics from `pg_stats` — distributions without a scan |
| `relations` | Foreign keys of a table, both directions |
| `join_path` | Shortest FK path between two tables as a ready JOIN chain |
| `count` | Instant planner estimate (optional `where`), `exact=true` for real `count(*)` |
| `sample` | Genuinely random rows via `TABLESAMPLE` (no LIMIT bias) |
| `profiles` / `profile_add` / `profile_remove` / `profile_test` | Runtime connection management |
| `status` | Profiles, pools, open cursors, limits |

Results are compact JSON — columns once, rows as arrays — roughly half the
tokens of the row-dict format other servers emit. `query` also returns
`estimated_rows` (planner estimate via `EXPLAIN`) so the model knows what it
is paging into.

## Install & run

```bash
uv pip install -e .
database-mcp --dsn postgresql://user@host:5432/db      # registers profile "default"
database-mcp                                           # start empty, add profiles at runtime
```

Claude Code registration:

```bash
claude mcp add database -- database-mcp --dsn postgresql://user@host:5432/db
```

Options: `--profiles FILE`, `--allow-writes`, `--page-size 50`,
`--max-page-size 500`, `--max-page-bytes 32000`, `--max-cell 400`,
`--cursor-ttl 300`, `--max-cursors 4`, `--statement-timeout 30`,
`--keepalive 120`, `--connect-timeout 5`.
Env: `DATABASE_MCP_DSN` / `DATABASE_URL`, `DATABASE_MCP_PROFILES`.

## Staleness handling

Dead connections are detected fast at every layer instead of hanging:

- **SSH tunnels**: `ServerAliveInterval` = `--keepalive` (default 2 min) with
  `ServerAliveCountMax=1` — one missed probe ends the tunnel process, which
  the engine manager detects on next use and rebuilds lazily.
- **DB connections**: TCP keepalives (`keepalives_idle` = `--keepalive`,
  probes every 10 s, 3 misses) catch dead peers in ~30 s — including pinned
  cursor connections outside the pool.
- **Pool checkout check**: every connection handed out is validated with a
  cheap round-trip; a stale one is discarded and replaced transparently —
  the caller never sees the error. Idle pooled connections are recycled
  after `--keepalive` seconds; connection attempts fail after
  `--connect-timeout` (default 5 s) instead of the ~2 min TCP default.

## Multi-session: the HTTP bridge

By default the server speaks stdio (one process per MCP client). For several
concurrent Claude sessions run it as a **shared bridge daemon** instead:

```bash
database-mcp --http --port 4270          # one daemon serves ALL sessions
claude mcp add --transport http database http://127.0.0.1:4270/mcp -s user
```

One process means genuinely shared state: profiles added in one session are
instantly visible in every other, connection pools and SSH tunnels exist
once instead of per session, held cursors survive a client reconnect (within
TTL), and the query log has a single writer. On macOS a LaunchAgent with
`KeepAlive` makes the bridge permanent.

stdio mode stays multi-session aware on a smaller scale: the profiles file
is watched (mtime) and reloaded when another session changes it — but pools,
tunnels, and cursors remain per-process there.

## Query log

Every tool call is logged as one JSON line to a daily file
(`~/.local/state/database-mcp/log/query-YYYYMMDD.jsonl`): timestamp, tool,
profile, **duration in ms**, row counts, truncated SQL (2000 chars),
error/sqlstate. Profile DSNs are never logged. Inspect recent entries with
the `logs` tool (filter by tool/profile/since); for bigger analyses point
duckdb/jq/pandas at the files:

```sql
-- duckdb: p95 query time per profile, last 14 days
select profile, count(*) n, round(quantile_cont(ms, 0.95)) p95_ms
from read_json_auto('~/.local/state/database-mcp/log/query-*.jsonl')
where tool in ('query','fetch','script') group by 1 order by p95_ms desc;
```

**Retention is bounded by design**: daily rotation, files older than
`--log-days` (default 14) are deleted at startup and on every rollover;
`--log-days 0` disables logging, `--log-dir` moves it.

## Tests

```bash
uv pip install -e '.[dev]'
pytest            # needs a local PostgreSQL (DBMCP_TEST_DSN to override)
```

## License

MIT
