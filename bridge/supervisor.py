"""Cross-platform detached-spawn supervisor: ``feishu-bridge up | status | stop``.

Replaces the old PTY keeper. Starts the bridge as a detached background process
(``python -m bridge run``) — POSIX ``start_new_session`` / Windows
``CREATE_NEW_PROCESS_GROUP | DETACHED`` — and tracks it via a pidfile. No PTY, no tmux,
identical behavior on Windows / Mac / Linux. Liveness uses ``os.kill(pid,0)`` on POSIX
and ctypes ``OpenProcess`` + ``GetExitCodeProcess == STILL_ACTIVE`` on Windows.
"""
from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

from . import feishu_api

REPO = feishu_api.PROJECT_DIR
RUN_DIR = Path(os.environ.get("FEISHU_RUN_DIR", str(Path.home() / ".chat_bridge")))
PIDFILE = RUN_DIR / "bridge.pid"
LOGFILE = RUN_DIR / "bridge.log"
_STILL_ACTIVE = 259


# ---- liveness ----------------------------------------------------------------

def _alive(pid: int) -> bool:
    if pid <= 0:
        return False
    if os.name == "nt":
        return _alive_windows(pid)
    try:
        os.kill(pid, 0)
        return True
    except (OSError, ProcessLookupError):
        return False


def _alive_windows(pid: int) -> bool:
    import ctypes
    from ctypes import wintypes

    PROCESS_QUERY_LIMITED_INFORMATION = 0x1000
    kernel32 = ctypes.windll.kernel32  # type: ignore[attr-defined]
    h = kernel32.OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, False, pid)
    if not h:
        return False
    try:
        code = wintypes.DWORD()
        if not kernel32.GetExitCodeProcess(h, ctypes.byref(code)):
            return False
        return code.value == _STILL_ACTIVE
    finally:
        kernel32.CloseHandle(h)


def _read_pid() -> int | None:
    if not PIDFILE.is_file():
        return None
    try:
        return int(PIDFILE.read_text().strip())
    except (ValueError, OSError):
        return None


def _running_pid() -> int | None:
    pid = _read_pid()
    if pid and _alive(pid):
        return pid
    return None


# ---- commands ----------------------------------------------------------------

def _spawn() -> int:
    """Start a detached bridge process, point the pidfile at it, return its pid."""
    cmd = [sys.executable, "-m", "bridge", "run"]
    out = open(LOGFILE, "ab", buffering=0)
    popen_kwargs: dict = {
        "cwd": str(REPO),
        "stdin": subprocess.DEVNULL,
        "stdout": out,
        "stderr": out,
        "close_fds": True,
    }
    if os.name == "nt":
        # CREATE_NO_WINDOW (not DETACHED_PROCESS): the child gets a *hidden*
        # console its own grandchildren inherit. DETACHED leaves the daemon
        # console-less, so any console-subsystem grandchild it spawns makes
        # Windows allocate a brand-new VISIBLE console — an empty cmd window
        # pops up on the user's desktop.
        popen_kwargs["creationflags"] = 0x00000200 | 0x08000000  # NEW_PROCESS_GROUP | NO_WINDOW
    else:
        popen_kwargs["start_new_session"] = True

    proc = subprocess.Popen(cmd, **popen_kwargs)
    PIDFILE.write_text(str(proc.pid))
    return proc.pid


def respawn() -> int:
    """Spawn a replacement bridge process (for in-place self-restart from a running
    bridge). The caller then exits; the replacement takes over the pidfile."""
    RUN_DIR.mkdir(parents=True, exist_ok=True)
    return _spawn()


def up() -> int:
    RUN_DIR.mkdir(parents=True, exist_ok=True)
    pid = _running_pid()
    if pid:
        print(f"feishu-bridge already running (pid {pid})")
        return 0

    pid = _spawn()
    print(f"feishu-bridge started (pid {pid}); logs: {LOGFILE}")
    return 0


def status() -> int:
    pid = _running_pid()
    if pid:
        print(f"feishu-bridge running (pid {pid})")
        return 0
    print("feishu-bridge not running")
    return 1


def stop() -> int:
    pid = _running_pid()
    if not pid:
        # Clean a stale pidfile if present.
        try:
            PIDFILE.unlink()
        except OSError:
            pass
        print("feishu-bridge not running")
        return 0
    _kill_tree(pid)
    try:
        PIDFILE.unlink()
    except OSError:
        pass
    print(f"feishu-bridge stopped (pid {pid})")
    return 0


def _kill_tree(pid: int) -> None:
    if os.name == "nt":
        subprocess.run(["taskkill", "/PID", str(pid), "/T", "/F"], capture_output=True, timeout=15)
    else:
        import signal

        try:
            os.killpg(os.getpgid(pid), signal.SIGTERM)
        except (ProcessLookupError, PermissionError):
            try:
                os.kill(pid, signal.SIGTERM)
            except ProcessLookupError:
                pass


def handle(cmd: str) -> int:
    if cmd == "up":
        return up()
    if cmd == "status":
        return status()
    if cmd == "stop":
        return stop()
    print(f"unknown supervisor command: {cmd}", file=sys.stderr)
    return 2
