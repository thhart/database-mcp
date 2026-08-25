"""SSH tunnels for profiles whose database is only reachable via a jump host.

Uses the system ssh binary (BatchMode) so the user's ~/.ssh/config, keys,
and agent apply unchanged — no paramiko-style reimplementation of auth.
A tunnel is one `ssh -N -L` subprocess with keepalives; liveness is checked
on every use and a dead tunnel is rebuilt lazily by the engine manager.
"""

import socket
import subprocess
import time


class TunnelError(Exception):
    """Tunnel could not be established or died; message is user-facing."""


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def build_cmd(ssh_host: str, local_port: int, remote_host: str, remote_port: int, keepalive: float) -> list[str]:
    return [
        "ssh",
        "-N",
        "-o", "BatchMode=yes",  # never prompt: stdio MCP cannot answer
        # a multiplexed client (ControlMaster in the user's config) hands
        # the forward to the master and exits, breaking process-based
        # lifecycle management — force a dedicated connection
        "-o", "ControlMaster=no",
        "-o", "ControlPath=none",
        "-o", "ExitOnForwardFailure=yes",
        # one missed probe declares the tunnel dead: the process exits and
        # the engine manager rebuilds it lazily on next use
        "-o", f"ServerAliveInterval={max(1, int(keepalive))}",
        "-o", "ServerAliveCountMax=1",
        "-o", "TCPKeepAlive=yes",
        "-o", "ConnectTimeout=8",
        "-L", f"127.0.0.1:{local_port}:{remote_host}:{remote_port}",
        ssh_host,
    ]


class Tunnel:
    def __init__(
        self,
        ssh_host: str,
        remote_host: str,
        remote_port: int,
        connect_timeout: float = 10.0,
        keepalive: float = 120.0,
    ):
        self.ssh_host = ssh_host
        self.remote_host = remote_host
        self.remote_port = remote_port
        self.keepalive = keepalive
        self.local_port = _free_port()
        self.proc: subprocess.Popen | None = None
        self._start(connect_timeout)

    def _start(self, timeout: float):
        cmd = build_cmd(
            self.ssh_host, self.local_port, self.remote_host, self.remote_port, self.keepalive
        )
        self.proc = subprocess.Popen(
            cmd, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True
        )
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if self.proc.poll() is not None:
                err = (self.proc.stderr.read() or "").strip()
                raise TunnelError(
                    f"ssh tunnel to {self.ssh_host} failed: {err or 'exited without error output'}"
                )
            try:
                with socket.create_connection(("127.0.0.1", self.local_port), timeout=0.5):
                    return
            except OSError:
                time.sleep(0.15)
        self.close()
        raise TunnelError(f"ssh tunnel to {self.ssh_host} did not come up within {timeout:.0f}s")

    def alive(self) -> bool:
        return self.proc is not None and self.proc.poll() is None

    def close(self):
        if self.proc is not None and self.proc.poll() is None:
            self.proc.terminate()
            try:
                self.proc.wait(timeout=3)
            except subprocess.TimeoutExpired:
                self.proc.kill()

    def describe(self) -> dict:
        return {
            "ssh_host": self.ssh_host,
            "remote": f"{self.remote_host}:{self.remote_port}",
            "local_port": self.local_port,
            "alive": self.alive(),
        }
