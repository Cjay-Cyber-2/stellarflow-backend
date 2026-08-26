"""FD isolation tests for src/utils/process_sandbox.py."""
from __future__ import annotations

import os
import sys
import tempfile

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from utils.process_sandbox import (  # noqa: E402
    SandboxConfig,
    SandboxRunner,
    is_cloexec,
    pipe_cloexec,
    dup_cloexec,
    set_cloexec,
)

_CHILD_FD_PROBE = (
    "import os;"
    "d=os.open('/proc/self/fd',os.O_RDONLY|os.O_DIRECTORY);"
    "names=os.listdir(d);"
    "os.close(d);"
    "fds=[int(n) for n in names if n.isdigit()];"
    "live=[f for f in sorted(fds) if f>2 and f!=d and os.path.exists('/proc/self/fd/'+str(f))];"
    "print(','.join(str(f) for f in live))"
)


def _run_probe(runner, **kwargs):
    return runner.run([sys.executable, "-c", _CHILD_FD_PROBE], **kwargs)


def test_fd_isolation_child_inherits_no_extra_fds():
    """A child must see no descriptors beyond stdio."""
    with tempfile.NamedTemporaryFile() as leaked:
        set_cloexec(leaked.fileno(), enable=False)
        runner = SandboxRunner(SandboxConfig(wall_timeout_seconds=15))
        result = _run_probe(runner)
        assert result.returncode == 0, result.stderr
        assert result.stdout.strip() == "", f"leaked fds: {result.stdout}"


def test_fd_isolation_pipe_ends_are_cloexec():
    read_fd, write_fd = pipe_cloexec()
    try:
        assert is_cloexec(read_fd)
        assert is_cloexec(write_fd)
    finally:
        os.close(read_fd)
        os.close(write_fd)


def test_fd_isolation_dup_reapplies_cloexec():
    read_fd, write_fd = pipe_cloexec()
    dup_fd = dup_cloexec(read_fd)
    try:
        assert is_cloexec(dup_fd)
    finally:
        for fd in (read_fd, write_fd, dup_fd):
            os.close(fd)


def test_fd_isolation_pass_fds_reaches_child():
    """Explicitly passed descriptors must survive; nothing else should."""
    read_fd, write_fd = pipe_cloexec()
    try:
        runner = SandboxRunner(SandboxConfig(wall_timeout_seconds=15))
        result = runner.run(
            [sys.executable, "-c", f"import os;os.fstat({write_fd});print('ok')"],
            pass_fds=(write_fd,),
        )
        assert result.returncode == 0, result.stderr
        assert "ok" in result.stdout
    finally:
        os.close(read_fd)
        os.close(write_fd)
