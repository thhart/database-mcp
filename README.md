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

Profiles are **read-only by default** (session-level
`default_transaction_read_only`); writes need an explicit
`allow_writes=true` profile.

## Tools

| Tool | Purpose |
|---|---|
| `query` | Execute SQL, get first page + `cursor` when more rows exist |
| `fetch` | Next page from a held cursor — no re-execution |
| `close` | Close one/all cursors early |
| `tables` | List tables/views with row estimates and sizes |
| `describe` | Columns, constraints, indexes of one table |
| `explain` | Query plan (optionally `analyze`) |
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
`--cursor-ttl 300`, `--max-cursors 4`, `--statement-timeout 30`.
Env: `DATABASE_MCP_DSN` / `DATABASE_URL`, `DATABASE_MCP_PROFILES`.

## Tests

```bash
uv pip install -e '.[dev]'
pytest            # needs a local PostgreSQL (DBMCP_TEST_DSN to override)
```

## License

MIT
