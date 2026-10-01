"""Small subprocess controller used by the producer and pipeline notebooks."""

from __future__ import annotations

import os
import signal
import subprocess
from dataclasses import dataclass
from pathlib import Path


@dataclass
class ManagedProcess:
    process: subprocess.Popen[str] | None = None
    log_path: Path | None = None

    @property
    def running(self) -> bool:
        return self.process is not None and self.process.poll() is None

    def start(
        self,
        command: list[str],
        *,
        log_path: Path,
        env: dict[str, str] | None = None,
    ) -> int:
        if self.running:
            raise RuntimeError("process is already running")
        log_path.parent.mkdir(parents=True, exist_ok=True)
        # Cada arranque empieza con el log vacío: el notebook muestra sólo la corrida actual.
        log_handle = log_path.open("w", buffering=1)
        self.process = subprocess.Popen(
            command,
            stdout=log_handle,
            stderr=subprocess.STDOUT,
            text=True,
            env={**os.environ, **(env or {})},
            start_new_session=True,
        )
        self.log_path = log_path
        return self.process.pid

    def stop(self, *, timeout: float = 10) -> None:
        if not self.running or self.process is None:
            return
        os.killpg(self.process.pid, signal.SIGTERM)
        try:
            self.process.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            os.killpg(self.process.pid, signal.SIGKILL)
            self.process.wait(timeout=5)

    def tail(self, lines: int = 40) -> str:
        if self.log_path is None or not self.log_path.exists():
            return ""
        return "\n".join(self.log_path.read_text(errors="replace").splitlines()[-lines:])
