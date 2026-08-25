"""Persistent connection profiles, managed at runtime by the AI.

Profiles live in a JSON file (default ~/.config/database-mcp/profiles.json,
chmod 600 since DSNs may contain passwords) and can be added, changed, and
removed on the fly through MCP tools — no server restart needed.
"""

import json
import os
from pathlib import Path

from psycopg.conninfo import conninfo_to_dict

DEFAULT_PATH = Path.home() / ".config" / "database-mcp" / "profiles.json"


class ProfileStore:
    def __init__(self, path: Path | None = None):
        self.path = Path(path) if path else DEFAULT_PATH
        self.default: str | None = None
        self.profiles: dict[str, dict] = {}
        self.load()

    def load(self):
        if self.path.exists():
            data = json.loads(self.path.read_text())
            self.profiles = data.get("profiles", {})
            self.default = data.get("default")
            if self.default not in self.profiles:
                self.default = next(iter(self.profiles), None)

    def save(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.path.with_suffix(".tmp")
        tmp.write_text(
            json.dumps({"default": self.default, "profiles": self.profiles}, indent=1)
        )
        os.chmod(tmp, 0o600)
        tmp.replace(self.path)

    def upsert(
        self,
        name: str,
        dsn: str,
        allow_writes: bool = False,
        description: str | None = None,
        make_default: bool = False,
        ssh: dict | None = None,
    ):
        spec = {
            "dsn": dsn,
            "allow_writes": bool(allow_writes),
            "description": description,
        }
        if ssh:
            spec["ssh"] = ssh
        self.profiles[name] = spec
        if make_default or self.default is None:
            self.default = name
        self.save()

    def remove(self, name: str) -> bool:
        if name not in self.profiles:
            return False
        del self.profiles[name]
        if self.default == name:
            self.default = next(iter(self.profiles), None)
        self.save()
        return True


def redact_dsn(dsn: str) -> str:
    """Human-readable DSN with the password masked."""
    try:
        info = conninfo_to_dict(dsn)
    except Exception:
        return "<unparseable dsn>"
    if "password" in info:
        info["password"] = "***"
    return " ".join(f"{k}={v}" for k, v in info.items())
