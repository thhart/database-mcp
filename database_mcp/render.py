"""Compact, token-efficient rendering of query results.

Results are rendered as columns-once + rows-as-arrays (not a dict per row),
which roughly halves the token cost compared to the row-dict format most
database MCP servers emit. Oversized cells are truncated with an explicit
marker so the model knows data is missing.
"""

import json
import math
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from uuid import UUID

TRUNCATION_MARKER = "…[+{n} chars]"
BYTES_PREVIEW = 32  # bytes shown (hex) before truncating binary values


def convert_cell(value, max_cell: int):
    """Convert a DB value to a JSON-serializable value, truncating big cells."""
    if value is None or isinstance(value, bool) or isinstance(value, int):
        return value
    if isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            return str(value)
        return value
    if isinstance(value, str):
        return _truncate(value, max_cell)
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, (datetime, date, time)):
        return value.isoformat()
    if isinstance(value, timedelta):
        return str(value)
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, memoryview):
        value = value.tobytes()
    if isinstance(value, (bytes, bytearray)):
        hexed = value[:BYTES_PREVIEW].hex()
        if len(value) > BYTES_PREVIEW:
            return f"\\x{hexed}…[{len(value)} bytes]"
        return f"\\x{hexed}"
    if isinstance(value, dict):
        return {k: convert_cell(v, max_cell) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [convert_cell(v, max_cell) for v in value]
    return _truncate(str(value), max_cell)


def _truncate(text: str, max_cell: int) -> str:
    if len(text) <= max_cell:
        return text
    return text[:max_cell] + TRUNCATION_MARKER.format(n=len(text) - max_cell)


def convert_row(row, max_cell: int) -> list:
    return [convert_cell(v, max_cell) for v in row]


def row_size(row: list) -> int:
    """Approximate rendered byte size of a converted row."""
    return len(json.dumps(row, separators=(",", ":"), ensure_ascii=False, default=str))


def dumps(obj) -> str:
    """Compact JSON without wasted whitespace tokens."""
    return json.dumps(obj, separators=(",", ":"), ensure_ascii=False, default=str)
