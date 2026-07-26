#!/usr/bin/env python3
"""
Isolated Process Sandboxes for External Unverified Data Adapters

Executes untrusted third-party adapter scripts inside restricted subprocess
containers so that a malicious or buggy endpoint cannot compromise core
connection keys or corrupt shared memory.

Security model
--------------
* Each adapter runs in its own *child process* via :class:`subprocess.Popen`.
  The parent never ``exec``s untrusted code directly.
* The child is launched with a stripped environment (``env`` parameter) - no
  ``DATABASE_URL``, no ``AWS_SECRET_ACCESS_KEY``, no inherited key material.
* **File descriptor isolation.** The child inherits *zero* extraneous open
  file descriptors from the parent:
  * ``close_fds=True`` is passed explicitly to :class:`subprocess.Popen`, so
    every descriptor above stdin/stdout/stderr is closed at ``exec`` time.
  * ``FD_CLOEXEC`` is enforced defensively on every descriptor the sandbox
    itself opens or duplicates, via :func:`set_cloexec` and
    :func:`open_cloexec`. This covers descriptors created by ``os.dup``,
    ``os.pipe``, ``socket.fromfd`` and ``socket.accept`` paths, none of which
    are guaranteed to be close-on-exec on every platform.
  * A pre-exec hook sweeps the child's descriptor table and closes anything
    outside the explicit allow-list, as belt-and-braces for platforms where
    ``close_fds`` is unreliable.
  * Descriptors that genuinely must reach the child are declared through
    ``pass_fds`` rather than by clearing ``FD_CLOEXEC``.
* ``popen`` is wrapped so the parent can apply OS-level hard limits:
  * ``resource.setrlimit`` to cap CPU time (``RLIMIT_CPU``) and address space
    (``RLIMIT_AS``) where the platform permits it.
  * A wall-clock timeout kills the child if it overruns.
* Streams are captured through ``communicate()`` with a size cap to prevent
  log / memory exhaustion attacks.

Usage::

    from src.utils.process_sandbox import SandboxRunner, SandboxConfig

    cfg = SandboxConfig(
        max_cpu_seconds=5,
        max_memory_mb=128,
        wall_timeout_seconds=10,
        blocked_env_vars={"DATABASE_URL", "API_KEY"},
    )

    runner = SandboxRunner(cfg)
    result = runner.run(["python3", "adapter.py", "--pair", "XLM/USDC"])
    print(result.returncode, result.stdout, result.stderr)
"""

from __future__ import annotations

import os
import platform
import resource
import socket
import subprocess
from dataclasses import dataclass, field
from typing import IO, Iterable, Optional, Sequence, Set, Tuple

try:  # POSIX only; absent on Windows.
    import fcntl
except ImportError:  # pragma: no cover - Windows fallback
    fcntl = None  # type: ignore[assignment]


_IS_WINDOWS = platform.system() == "Windows"

# Descriptors the child is always allowed to keep: stdin, stdout, stderr.
_STD_FDS = (0, 1, 2)


# ---------------------------------------------------------------------------
# File descriptor hardening helpers
# ---------------------------------------------------------------------------

def set_cloexec(fd: int, enable: bool = True) -> None:
    """Set (or clear) ``FD_CLOEXEC`` on *fd*.

    A descriptor marked close-on-exec is closed automatically by the kernel
    when the child calls ``exec``, so it can never leak into an untrusted
    adapter process. This is a no-op on platforms without :mod:`fcntl`.

    Parameters
    ----------
    fd:
        An open file descriptor.
    enable:
        ``True`` to set the flag, ``False`` to clear it. Clearing should only
        ever be done for a descriptor deliberately handed to the child, and
        even then ``pass_fds`` is the preferred mechanism.
    """
    if fcntl is None or fd < 0:
        return
    try:
        flags = fcntl.fcntl(fd, fcntl.F_GETFD)
    except OSError:
        # Descriptor already closed or not valid; nothing to harden.
        return
    new_flags = (flags | fcntl.FD_CLOEXEC) if enable else (flags & ~fcntl.FD_CLOEXEC)
    if new_flags != flags:
        try:
            fcntl.fcntl(fd, fcntl.F_SETFD, new_flags)
        except OSError:
            pass


def is_cloexec(fd: int) -> bool:
    """Return ``True`` if *fd* is marked close-on-exec."""
    if fcntl is None or fd < 0:
        return True
    try:
        return bool(fcntl.fcntl(fd, fcntl.F_GETFD) & fcntl.FD_CLOEXEC)
    except OSError:
        return True


def open_cloexec(path: str, flags: int, mode: int = 0o600) -> int:
    """``os.open`` wrapper that guarantees ``FD_CLOEXEC``.

    ``os.open`` sets the flag by default on Python 3.4+, but the guarantee is
    restated here explicitly so that no descriptor opened by this module can
    regress if the default ever changes or the caller passes custom flags.
    """
    o_cloexec = getattr(os, "O_CLOEXEC", 0)
    fd = os.open(path, flags | o_cloexec, mode)
    set_cloexec(fd)
    return fd


def open_file_cloexec(path: str, mode: str = "rb", **kwargs) -> IO:
    """``open()`` wrapper that guarantees ``FD_CLOEXEC`` on the result."""
    handle = open(path, mode, **kwargs)
    try:
        set_cloexec(handle.fileno())
    except (OSError, ValueError):
        handle.close()
        raise
    return handle


def pipe_cloexec() -> Tuple[int, int]:
    """Create a pipe whose *both* ends are close-on-exec.

    Uses ``os.pipe2(O_CLOEXEC)`` atomically where available, which avoids the
    race window between ``os.pipe()`` and a follow-up ``fcntl`` call in a
    threaded parent.
    """
    if hasattr(os, "pipe2") and hasattr(os, "O_CLOEXEC"):
        read_fd, write_fd = os.pipe2(os.O_CLOEXEC)
    else:  # pragma: no cover - legacy / non-Linux fallback
        read_fd, write_fd = os.pipe()
    set_cloexec(read_fd)
    set_cloexec(write_fd)
    return read_fd, write_fd


def dup_cloexec(fd: int) -> int:
    """Duplicate *fd* without leaking the copy into child processes.

    ``os.dup`` clears ``FD_CLOEXEC`` on the new descriptor by definition, so
    the flag must always be reapplied afterwards.
    """
    new_fd = os.dup(fd)
    set_cloexec(new_fd)
    return new_fd


def socket_cloexec(*args, **kwargs) -> socket.socket:
    """Create a socket and enforce ``FD_CLOEXEC`` on its descriptor."""
    sock = socket.socket(*args, **kwargs)
    set_cloexec(sock.fileno())
    return sock


def accept_cloexec(listener: socket.socket) -> Tuple[socket.socket, object]:
    """``accept()`` wrapper that hardens the returned connection socket.

    Accepted sockets do not inherit ``FD_CLOEXEC`` from the listening socket
    on all platforms, which is a classic descriptor-leak vector.
    """
    conn, addr = listener.accept()
    set_cloexec(conn.fileno())
    return conn, addr


def socket_from_fd_cloexec(fd: int, *args, **kwargs) -> socket.socket:
    """``socket.fromfd`` wrapper that hardens the duplicated descriptor."""
    sock = socket.fromfd(fd, *args, **kwargs)
    set_cloexec(sock.fileno())
    return sock


def harden_open_fds(exclude: Iterable[int] = ()) -> int:
    """Mark every currently open descriptor close-on-exec.

    Walks the process descriptor table and sets ``FD_CLOEXEC`` on everything
    except stdin/stdout/stderr and anything in *exclude*. Returns the number
    of descriptors that were modified.

    This is the sweep used to catch descriptors opened by third-party
    libraries, which the sandbox does not control.
    """
    if fcntl is None:
        return 0
    keep = set(_STD_FDS) | set(exclude)
    hardened = 0
    for fd in _list_open_fds():
        if fd in keep:
            continue
        if not is_cloexec(fd):
            set_cloexec(fd)
            hardened += 1
    return hardened


def _list_open_fds() -> Sequence[int]:
    """Best-effort enumeration of open descriptors in this process."""
    for proc_path in ("/proc/self/fd", "/dev/fd"):
        try:
            return sorted(int(name) for name in os.listdir(proc_path) if name.isdigit())
        except (FileNotFoundError, NotADirectoryError, PermissionError, ValueError):
            continue
    # Fallback: probe up to the soft descriptor limit.
    try:
        soft, _hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    except (ValueError, OSError):  # pragma: no cover
        soft = 1024
    if soft in (resource.RLIM_INFINITY, -1) or soft > 4096:
        soft = 4096
    found = []
    for fd in range(int(soft)):
        try:
            os.fstat(fd)
        except OSError:
            continue
        found.append(fd)
    return found


def _max_fd() -> int:
    try:
        soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    except (ValueError, OSError):  # pragma: no cover
        return 4096
    limit = hard if hard not in (resource.RLIM_INFINITY, -1) else soft
    if limit in (resource.RLIM_INFINITY, -1) or limit > 65536:
        limit = 65536
    return int(limit)


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class SandboxConfig:
    """Tunable security knobs for :class:`SandboxRunner`."""

    max_cpu_seconds: int = 10
    max_memory_mb: int = 256
    wall_timeout_seconds: Optional[int] = 30
    blocked_env_vars: Set[str] = field(
        default_factory=lambda: {
            "DATABASE_URL",
            "POSTGRES_PASSWORD",
            "API_KEY",
            "AWS_SECRET_ACCESS_KEY",
            "AWS_ACCESS_KEY_ID",
            "PRIVATE_KEY",
            "SECRET_KEY",
        }
    )
    allowed_env_vars: Set[str] = field(default_factory=lambda: {"PATH", "HOME", "LANG"})
    # Descriptor isolation
    close_child_fds: bool = True
    sweep_child_fd_table: bool = True
    max_open_files: Optional[int] = 64


# ---------------------------------------------------------------------------
# Result container
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class SandboxResult:
    returncode: int
    stdout: str
    stderr: str
    timed_out: bool = False


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------

class SandboxRunner:
    """Runs an external adapter script in a hardened subprocess sandbox.

    Parameters
    ----------
    config:
        Security budget for child processes. See :class:`SandboxConfig`.
    """

    def __init__(self, config: Optional[SandboxConfig] = None) -> None:
        self._cfg = config or SandboxConfig()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def run(
        self,
        args: Sequence[str],
        *,
        cwd: Optional[str] = None,
        env: Optional[dict] = None,
        max_output_bytes: int = 1_048_576,  # 1 MiB safety cap
        pass_fds: Sequence[int] = (),
    ) -> SandboxResult:
        """Execute *args* inside the sandbox and return captured output.

        Parameters
        ----------
        pass_fds:
            Descriptors that must deliberately reach the child. Everything
            not listed here is closed at ``exec`` time. Never clear
            ``FD_CLOEXEC`` manually to achieve this; use this parameter.

        Raises
        ------
        ValueError
            If *args* is empty.
        FileNotFoundError
            If the executable cannot be located.
        """
        if not args:
            raise ValueError("args must not be empty")

        safe_env = self._build_safe_env(env)
        max_mem_bytes = self._cfg.max_memory_mb * 1_048_576
        pass_fds = tuple(pass_fds)

        # Harden the parent's own descriptor table before forking so that any
        # descriptor opened by a third-party library cannot survive the exec.
        if self._cfg.close_child_fds:
            harden_open_fds(exclude=pass_fds)

        # On POSIX we can apply RLIMITs and sweep the fd table before exec.
        # On Windows the resource module is mostly a no-op.
        preexec_fn = self._build_preexec_fn(max_mem_bytes, pass_fds)

        popen_kwargs = {
            "stdout": subprocess.PIPE,
            "stderr": subprocess.PIPE,
            "stdin": subprocess.DEVNULL,
            "cwd": cwd,
            "env": safe_env,
            # Explicit rather than relying on the 3.2+ default: every
            # descriptor above stdio is closed in the child at exec time.
            "close_fds": self._cfg.close_child_fds,
        }
        if pass_fds:
            popen_kwargs["pass_fds"] = pass_fds
        if preexec_fn is not None:
            popen_kwargs["preexec_fn"] = preexec_fn

        proc = subprocess.Popen(list(args), **popen_kwargs)

        # The parent's ends of the stdio pipes must not leak into any *other*
        # child spawned concurrently by this process.
        self._harden_proc_pipes(proc)

        try:
            stdout_bytes, stderr_bytes = proc.communicate(
                timeout=self._cfg.wall_timeout_seconds
            )
            return SandboxResult(
                returncode=proc.returncode,
                stdout=self._truncate(stdout_bytes, max_output_bytes),
                stderr=self._truncate(stderr_bytes, max_output_bytes),
                timed_out=False,
            )
        except subprocess.TimeoutExpired:
            proc.kill()
            # Try one last drain so we don't lose diagnostic output.
            try:
                out, err = proc.communicate(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait()
                out, err = b"", b""
            return SandboxResult(
                returncode=-9,
                stdout=self._truncate(out, max_output_bytes),
                stderr=self._truncate(err, max_output_bytes),
                timed_out=True,
            )
        finally:
            self._close_proc_pipes(proc)

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _build_safe_env(self, override: Optional[dict]) -> dict:
        """Start from the current process env, strip secrets, apply override."""
        base = dict(os.environ)
        for var in self._cfg.blocked_env_vars:
            base.pop(var, None)
        for var in list(base.keys()):
            if var not in self._cfg.allowed_env_vars:
                base.pop(var, None)
        if override:
            base.update(override)
        return base

    def _build_preexec_fn(self, max_mem_bytes: int, pass_fds: Sequence[int]):
        """Return a pre-exec callback that hard-locks the child process."""
        if _IS_WINDOWS:
            return None  # resource.setrlimit not available

        cfg = self._cfg
        keep_fds = set(_STD_FDS) | set(pass_fds)
        sweep = cfg.sweep_child_fd_table
        max_files = cfg.max_open_files
        fd_ceiling = _max_fd()

        def _preexec() -> None:
            try:
                # Cap CPU time (seconds).
                resource.setrlimit(
                    resource.RLIMIT_CPU,
                    (cfg.max_cpu_seconds, cfg.max_cpu_seconds),
                )
                # Cap address space.
                resource.setrlimit(
                    resource.RLIMIT_AS,
                    (max_mem_bytes, max_mem_bytes),
                )
                # Cap how many descriptors the child may open for itself.
                if max_files is not None:
                    resource.setrlimit(
                        resource.RLIMIT_NOFILE, (max_files, max_files)
                    )
            except (ValueError, OSError):
                # Silently ignore if the platform refuses (e.g. already in a
                # container with lower limits).
                pass

            if sweep:
                # Final sweep: close anything still open besides stdio and
                # explicitly passed descriptors. This runs after the fork and
                # before the exec, so it cannot disturb the parent.
                try:
                    os.closerange(3, fd_ceiling) if not keep_fds - set(
                        _STD_FDS
                    ) else _close_all_except(keep_fds, fd_ceiling)
                except OSError:
                    pass

        return _preexec

    @staticmethod
    def _harden_proc_pipes(proc: subprocess.Popen) -> None:
        """Mark the parent-side stdio pipe ends close-on-exec."""
        for stream in (proc.stdin, proc.stdout, proc.stderr):
            if stream is None:
                continue
            try:
                set_cloexec(stream.fileno())
            except (OSError, ValueError):
                continue

    @staticmethod
    def _close_proc_pipes(proc: subprocess.Popen) -> None:
        """Close any parent-side pipe ends still open after the run."""
        for stream in (proc.stdin, proc.stdout, proc.stderr):
            if stream is None or stream.closed:
                continue
            try:
                stream.close()
            except (OSError, ValueError):
                continue

    @staticmethod
    def _truncate(data: bytes, limit: int) -> str:
        if not data:
            return ""
        if len(data) > limit:
            return data[:limit].decode("utf-8", errors="replace") + "\n...[truncated]"
        return data.decode("utf-8", errors="replace")


def _close_all_except(keep: Set[int], ceiling: int) -> None:
    """Close every descriptor from 3 upward except those in *keep*.

    Called only inside the forked child, immediately before ``exec``.
    """
    start = 3
    for fd in sorted(f for f in keep if f >= 3):
        if fd > start:
            os.closerange(start, fd)
        start = fd + 1
    os.closerange(start, ceiling)


__all__ = [
    "SandboxConfig",
    "SandboxResult",
    "SandboxRunner",
    "set_cloexec",
    "is_cloexec",
    "open_cloexec",
    "open_file_cloexec",
    "pipe_cloexec",
    "dup_cloexec",
    "socket_cloexec",
    "accept_cloexec",
    "socket_from_fd_cloexec",
    "harden_open_fds",
]