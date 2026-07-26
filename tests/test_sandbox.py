"""
tests/test_sandbox.py
~~~~~~~~~~~~~~~~~~~~~
Comprehensive test suite for src/utils/sandbox.py.

Coverage targets
----------------
* BPF builder instruction construction and compilation
* Pre-built INGESTION_POLICY bytecode generation
* SandboxPolicy creation, apply, context manager, decorator
* Platform detection (Linux / non-Linux)
* seccomp_available / current_seccomp_mode utilities
* Idempotent apply (double-apply safety)
* Non-Linux graceful degradation
"""  # noqa: D205, D400
from __future__ import annotations

import os
import sys
import struct
from unittest.mock import mock_open, patch

import pytest

# ---------------------------------------------------------------------------
# Path bootstrap
# ---------------------------------------------------------------------------
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from utils.sandbox import (  # noqa: E402
    BPFBuilder,
    INGESTION_POLICY,
    MemoryLimit,
    SandboxPolicy,
    Syscall,
    _AUDIT_ARCH_X86_64,
    _DEFAULT_MEMORY_LIMIT_MB,
    _SECCOMP_RET_ALLOW,
    _SECCOMP_RET_KILL_PROCESS,
    _BPF_LD_W_ABS,
    _BPF_JMP_JEQ_K,
    _BPF_RET_K,
    current_seccomp_mode,
    sandbox_policy,
    seccomp_available,
    seccomp_guard,
)


# ===========================================================================
# BPFBuilder
# ===========================================================================


class TestBPFBuilder:
    """Unit tests for the BPF instruction builder."""

    def test_empty_filter_has_preamble(self) -> None:
        """Verify the preamble: arch check + load-nr instructions."""
        bpf = BPFBuilder(_AUDIT_ARCH_X86_64)
        # Preamble: 3 instructions (ld abs[4], jeq arch, ret kill) + 1 (ld abs[0]) = 4
        assert bpf.filter_count() == 4

    def test_single_allow_syscall(self) -> None:
        """Allow one syscall — should emit jeq + ret allow."""
        bpf = BPFBuilder(_AUDIT_ARCH_X86_64)
        bpf.allow_syscall(Syscall.READ)
        # preamble(4) + jeq(1) + allow(1) = 6
        assert bpf.filter_count() == 6

    def test_multiple_allow_syscalls(self) -> None:
        """Allow several syscalls in sorted order."""
        bpf = BPFBuilder(_AUDIT_ARCH_X86_64)
        bpf.allow_syscalls(frozenset([Syscall.READ, Syscall.WRITE, Syscall.CLOSE]))
        # preamble(4) + 3*(jeq+allow) = 4 + 6 = 10
        assert bpf.filter_count() == 10

    def test_compilation_produces_correct_bytecode_size(self) -> None:
        """Each SockFilter is 8 bytes (code:2, jt:1, jf:1, k:4)."""
        bpf = BPFBuilder(_AUDIT_ARCH_X86_64)
        bpf.allow_syscall(Syscall.READ)
        bpf.finalize()
        bytecode = bpf.compile()
        # preamble(4) + allow(2) + default-kill(1) = 7 instructions * 8 = 56 bytes
        assert len(bytecode) == bpf.filter_count() * 8
        assert len(bytecode) == 7 * 8

    def test_compilation_auto_finalizes(self) -> None:
        """compile() must call finalize() if not already done."""
        bpf = BPFBuilder(_AUDIT_ARCH_X86_64)
        bytecode = bpf.compile()
        assert len(bytecode) > 0

    def test_bytecode_starts_with_arch_check(self) -> None:
        """First four instructions must be: load arch, jeq arch, kill (mismatch), load nr."""
        bpf = BPFBuilder(_AUDIT_ARCH_X86_64)
        bytecode = bpf.compile()
        # Decode first 4 instructions
        for i in range(4):
            ins_data = bytecode[i * 8 : (i + 1) * 8]
            code, jt, jf, k = struct.unpack("<HBBI", ins_data)
            if i == 0:
                assert code == _BPF_LD_W_ABS
                assert k == 4  # offset of seccomp_data.arch
            elif i == 1:
                assert code == _BPF_JMP_JEQ_K
                assert k == _AUDIT_ARCH_X86_64
            elif i == 2:
                assert code == _BPF_RET_K
                assert k == _SECCOMP_RET_KILL_PROCESS
            elif i == 3:
                assert code == _BPF_LD_W_ABS
                assert k == 0  # offset of seccomp_data.nr

    def test_finalize_is_idempotent(self) -> None:
        """Calling finalize() twice must not add duplicate default rules."""
        bpf = BPFBuilder(_AUDIT_ARCH_X86_64)
        bpf.finalize()
        count1 = bpf.filter_count()
        bpf.finalize()
        assert bpf.filter_count() == count1

    def test_block_syscall_with_errno(self) -> None:
        """Block a syscall with a specific errno value."""
        bpf = BPFBuilder(_AUDIT_ARCH_X86_64)
        bpf.block_syscall_with_errno(Syscall.CREAT, errno_val=1)  # EPERM
        # preamble(4) + jeq(1) + ret_errno(1) = 6
        assert bpf.filter_count() == 6

        bytecode = bpf.compile()
        # Find the RET instruction for creat
        # It should be instruction at index 5 (0-indexed)
        ins = bytecode[5 * 8 : 6 * 8]
        code, jt, jf, k = struct.unpack("<HBBI", ins)
        assert code == _BPF_RET_K
        # k should be SECCOMP_RET_ERRNO | errno_val
        assert k == (0x00050000 | 1)

    def test_allow_open_read_only_instruction_count(self) -> None:
        """The allow_open_read_only helper produces a reasonable number of instructions."""
        bpf = BPFBuilder(_AUDIT_ARCH_X86_64)
        bpf.allow_open_read_only()
        # preamble(4) + (jeq + jump placeholder + ld + and + jeq + kill + allow) = 4 + 7 = 11
        assert 10 <= bpf.filter_count() <= 15

    def test_allow_openat_read_only_instruction_count(self) -> None:
        """The allow_openat_read_only helper produces a reasonable number of instructions."""
        bpf = BPFBuilder(_AUDIT_ARCH_X86_64)
        bpf.allow_openat_read_only()
        assert 10 <= bpf.filter_count() <= 15

    def test_chaining_multiple_operations(self) -> None:
        """Multiple builder calls must stack correctly."""
        bpf = BPFBuilder(_AUDIT_ARCH_X86_64)
        bpf.allow_syscalls(frozenset([Syscall.READ, Syscall.WRITE]))
        bpf.allow_open_read_only()
        bpf.allow_openat_read_only()
        bpf.block_syscall_with_errno(Syscall.CREAT)
        bytecode = bpf.compile()
        assert len(bytecode) > 0
        assert bpf.filter_count() > 10


# ===========================================================================
# Pre-built Policies
# ===========================================================================


class TestPrebuiltPolicies:
    """Tests for the INGESTION_POLICY and other pre-compiled filters."""

    def test_ingestion_policy_is_bytes(self) -> None:
        """INGESTION_POLICY must be pre-compiled BPF bytecode."""
        assert isinstance(INGESTION_POLICY, bytes)
        assert len(INGESTION_POLICY) > 0

    def test_ingestion_policy_bytecode_is_multiple_of_eight(self) -> None:
        """Each BPF instruction is exactly 8 bytes."""
        assert len(INGESTION_POLICY) % 8 == 0

    def test_ingestion_policy_contains_arch_check(self) -> None:
        """The preamble architecture check must be present."""
        ins_count = len(INGESTION_POLICY) // 8
        # First instruction: ld arch
        ins0 = struct.unpack("<HBBI", INGESTION_POLICY[0:8])
        assert ins0[0] == _BPF_LD_W_ABS
        assert ins0[3] == 4  # arch offset

        # Third instruction: ret kill for mismatch
        ins2 = struct.unpack("<HBBI", INGESTION_POLICY[16:24])
        assert ins2[0] == _BPF_RET_K

    def test_ingestion_policy_has_default_kill(self) -> None:
        """The last instruction must be RET_KILL (default-deny)."""
        last = struct.unpack(
            "<HBBI", INGESTION_POLICY[-8:]
        )
        assert last[0] == _BPF_RET_K
        assert last[3] == _SECCOMP_RET_KILL_PROCESS

    def test_ingestion_policy_has_reasonable_size(self) -> None:
        """Policy should be between 50 and 500 instructions (reasonable BPF program size)."""
        num_insns = len(INGESTION_POLICY) // 8
        assert 50 <= num_insns <= 500, (
            f"Expected 50-500 instructions, got {num_insns}"
        )


# ===========================================================================
# SandboxPolicy  (non-Linux platform path)
# ===========================================================================


class TestSandboxPolicyNonLinux:
    """Behaviour on non-Linux platforms (graceful degradation)."""

    def setup_method(self) -> None:
        self.policy = SandboxPolicy(INGESTION_POLICY, name="test-policy")

    def test_name_property(self) -> None:
        assert self.policy.name == "test-policy"

    def test_initial_state(self) -> None:
        assert not self.policy.applied

    def test_apply_returns_false_on_non_linux(self) -> None:
        """On non-Linux, apply() must return False without raising."""
        with patch("utils.sandbox._is_linux", False):
            result = self.policy.apply()
            assert result is False
            assert not self.policy.applied

    def test_context_manager_noop_on_non_linux(self) -> None:
        """Enter/exit the context manager without seccomp."""
        with patch("utils.sandbox._is_linux", False):
            with SandboxPolicy(INGESTION_POLICY, name="ctx-test") as p:
                assert p.name == "ctx-test"
                assert not p.applied

    def test_repr(self) -> None:
        r = repr(self.policy)
        assert "test-policy" in r
        assert "pending" in r


# ===========================================================================
# SandboxPolicy  (Linux path — mocks prctl)
# ===========================================================================


class TestSandboxPolicyLinux:
    """Tests that exercise the prctl path using mocks."""

    def test_apply_calls_prctl_on_linux(self) -> None:
        """On Linux, apply() must invoke prctl via libc."""
        with patch("utils.sandbox._is_linux", True), \
             patch("utils.sandbox._raw_prctl") as mock_prctl:
            policy = SandboxPolicy(INGESTION_POLICY, name="linux-test")
            result = policy.apply()
            assert result is True
            mock_prctl.assert_called_once()
            # Verify the first arg is PR_SET_SECCOMP (22)
            args, _ = mock_prctl.call_args
            assert args[0] == 22  # PR_SET_SECCOMP

    def test_apply_is_idempotent(self) -> None:
        """Second call to apply() must be a no-op (True, no prctl)."""
        with patch("utils.sandbox._is_linux", True), \
             patch("utils.sandbox._raw_prctl") as mock_prctl:
            policy = SandboxPolicy(INGESTION_POLICY, name="idem-test")
            assert policy.apply() is True
            assert policy.applied
            # Second call
            assert policy.apply() is True
            # prctl must have been called exactly once
            assert mock_prctl.call_count == 1

    def test_apply_handles_prctl_failure(self) -> None:
        """If prctl raises OSError, apply() returns False."""
        with patch("utils.sandbox._is_linux", True), \
             patch("utils.sandbox._raw_prctl", side_effect=OSError(1, "EPERM")):
            policy = SandboxPolicy(INGESTION_POLICY, name="fail-test")
            result = policy.apply()
            assert result is False
            assert not policy.applied

    def test_context_manager_applies_on_enter(self) -> None:
        """Using the policy as context manager on Linux must call apply()."""
        with patch("utils.sandbox._is_linux", True), \
             patch("utils.sandbox._raw_prctl") as mock_prctl:
            with SandboxPolicy(INGESTION_POLICY, name="ctx-linux") as p:
                assert p.applied
            mock_prctl.assert_called_once()

    def test_context_manager_does_not_raise_on_prctl_failure(self) -> None:
        """Context manager must not raise even when prctl fails."""
        with patch("utils.sandbox._is_linux", True), \
             patch("utils.sandbox._raw_prctl", side_effect=OSError(1, "EPERM")):
            # Must not raise
            with SandboxPolicy(INGESTION_POLICY, name="ctx-softfail"):
                pass

    def test_decorator_applies_policy(self) -> None:
        """The decorator protocol must invoke apply()."""
        with patch("utils.sandbox._is_linux", True), \
             patch("utils.sandbox._raw_prctl") as mock_prctl:
            policy = SandboxPolicy(INGESTION_POLICY, name="deco-test")

            called = False

            @policy
            def my_func() -> int:
                nonlocal called
                called = True
                return 42

            result = my_func()
            assert called
            assert result == 42
            assert mock_prctl.call_count == 1


# ===========================================================================
# seccomp_guard decorator
# ===========================================================================


class TestSeccompGuard:
    """Tests for the seccomp_guard convenience decorator."""

    def test_guard_decorates_function(self) -> None:
        """seccomp_guard must produce a callable wrapper."""
        policy = SandboxPolicy(INGESTION_POLICY, name="guard-test")
        decorated = seccomp_guard(policy)(lambda: 7)
        assert callable(decorated)

    def test_guard_calls_policy_apply(self) -> None:
        """The wrapped function must trigger policy.apply()."""
        with patch("utils.sandbox._is_linux", False):
            policy = SandboxPolicy(INGESTION_POLICY, name="guard-test")
            with patch.object(policy, "apply") as mock_apply:
                decorated = seccomp_guard(policy)(lambda: 42)
                assert decorated() == 42
                mock_apply.assert_called_once()

    def test_guard_preserves_function_metadata(self) -> None:
        """The wrapper must preserve __name__ and __doc__ of the original."""
        policy = SandboxPolicy(INGESTION_POLICY, name="meta-test")

        def original_func(a: int, b: int) -> int:
            """Add two numbers."""
            return a + b

        wrapped = seccomp_guard(policy)(original_func)
        assert wrapped.__name__ == "original_func"
        assert wrapped.__doc__ == "Add two numbers."
        assert wrapped(3, 4) == 7


# ===========================================================================
# sandbox_policy factory
# ===========================================================================


class TestSandboxPolicyFactory:
    """Tests for the sandbox_policy convenience function."""

    def test_creates_default_policy_without_args(self) -> None:
        """sandbox_policy() without arguments returns INGESTION_POLICY."""
        policy = sandbox_policy()
        assert isinstance(policy, SandboxPolicy)
        assert policy.name == "ingestion-default"

    def test_creates_custom_policy_with_bpf_bytes(self) -> None:
        """sandbox_policy(bytes, name=...) uses the provided bytes."""
        bpf = BPFBuilder().allow_syscall(Syscall.READ).compile()
        policy = sandbox_policy(bpf, name="custom")
        assert isinstance(policy, SandboxPolicy)
        assert policy.name == "custom"

    def test_default_policy_is_singleton(self) -> None:
        """Multiple calls without args return the same object."""
        p1 = sandbox_policy()
        p2 = sandbox_policy()
        assert p1 is p2

    def test_custom_policy_is_not_singleton(self) -> None:
        """Custom policies (with bpf_bytes) are new instances each time."""
        bpf = BPFBuilder().allow_syscall(Syscall.READ).compile()
        p1 = sandbox_policy(bpf, name="a")
        p2 = sandbox_policy(bpf, name="b")
        assert p1 is not p2


# ===========================================================================
# Utility Functions
# ===========================================================================


class TestSeccompAvailable:
    """Tests for seccomp_available()."""

    def test_returns_false_on_non_linux(self) -> None:
        with patch("utils.sandbox._is_linux", False):
            assert seccomp_available() is False

    def test_returns_true_when_seccomp_present(self) -> None:
        """If /proc/self/status contains Seccomp: 2, return True."""
        fake_status = "Name:   python\nSeccomp:        2\n"
        with patch("utils.sandbox._is_linux", True), \
             patch("builtins.open", mock_open(read_data=fake_status)):
            result = seccomp_available()
            assert result is True

    def test_returns_false_when_file_not_found(self) -> None:
        with patch("utils.sandbox._is_linux", True), \
             patch("builtins.open", side_effect=OSError):
            assert seccomp_available() is False


class TestCurrentSeccompMode:
    """Tests for current_seccomp_mode()."""

    def test_returns_negative_on_non_linux(self) -> None:
        with patch("utils.sandbox._is_linux", False):
            assert current_seccomp_mode() == -1

    def test_returns_negative_when_libc_none(self) -> None:
        with patch("utils.sandbox._is_linux", True), \
             patch("utils.sandbox._LIBC", None):
            assert current_seccomp_mode() == -1

    def test_returns_prctl_result_on_linux(self) -> None:
        with patch("utils.sandbox._is_linux", True), \
             patch("utils.sandbox._raw_prctl", return_value=0):
            assert current_seccomp_mode() == 0


# ===========================================================================
# Syscall Enumeration
# ===========================================================================


class TestSyscallEnum:
    """Tests for the Syscall namespace."""

    def test_known_syscalls_have_correct_values(self) -> None:
        """Spot-check canonical syscall numbers."""
        assert Syscall.READ == 0
        assert Syscall.WRITE == 1
        assert Syscall.OPEN == 2
        assert Syscall.CLOSE == 3
        assert Syscall.EXIT == 60
        assert Syscall.EXIT_GROUP == 231
        assert Syscall.SOCKET == 41
        assert Syscall.CONNECT == 42

    def test_open_creat_blocked_syscalls_present(self) -> None:
        """Sanity: dangerous filesystem syscalls are enumerated."""
        assert Syscall.CREAT == 85
        assert Syscall.OPENAT == 257
        assert Syscall.MKDIR == 83
        assert Syscall.RENAME == 82
        assert Syscall.UNLINK == 87

    def test_no_duplicate_syscall_values(self) -> None:
        """All syscall names in the class must map to unique numbers."""
        values = [
            v
            for k, v in vars(Syscall).items()
            if isinstance(v, int) and not k.startswith("_")
        ]
        assert len(values) == len(set(values)), (
            "Syscall enum contains duplicate values"
        )


# ===========================================================================
# Edge Cases & Robustness
# ===========================================================================


class TestEdgeCases:
    """Edge cases and robustness assertions."""

    def test_empty_policy_is_valid(self) -> None:
        """A policy with zero allowed syscalls is still valid (just kills everything)."""
        bpf = BPFBuilder(_AUDIT_ARCH_X86_64)
        bytecode = bpf.compile()
        assert len(bytecode) > 0
        # Should just have preamble + default kill
        assert len(bytecode) // 8 >= 5

    def test_very_large_policy_compiles(self) -> None:
        """Allow a large number of syscalls without error."""
        bpf = BPFBuilder(_AUDIT_ARCH_X86_64)
        many_syscalls = frozenset(range(0, 400))
        bpf.allow_syscalls(many_syscalls)
        bytecode = bpf.compile()
        # Each allow = 2 insns * 8 bytes = 16 bytes per syscall
        # 400 syscalls * 16 + preamble(32) + kill(8) ≈ 6440 bytes
        assert len(bytecode) > 1000
        assert len(bytecode) % 8 == 0

    def test_policy_with_no_allowed_and_no_blocked_is_still_valid(self) -> None:
        """A minimal policy block some syscalls but allow none explicitly."""
        bpf = BPFBuilder(_AUDIT_ARCH_X86_64)
        bpf.block_syscall_with_errno(Syscall.CREAT)
        bytecode = bpf.compile()
        assert len(bytecode) > 0

    def test_sandbox_policy_apply_with_empty_bytes(self) -> None:
        """A policy with empty bytecode should handle gracefully."""
        # Create a minimal valid BPF program: arch check + default kill
        bpf = BPFBuilder(_AUDIT_ARCH_X86_64)
        min_bytes = bpf.compile()
        policy = SandboxPolicy(min_bytes, name="empty")
        assert policy.name == "empty"
        assert not policy.applied


# ===========================================================================
# Memory Limits
# ===========================================================================


class TestMemoryLimit:
    """Tests for the MemoryLimit context manager and decorator."""

    def test_memory_limit_creation(self) -> None:
        """MemoryLimit should be created with default and custom values."""
        ml = MemoryLimit()
        assert ml.limit_mb == _DEFAULT_MEMORY_LIMIT_MB
        assert ml.name == "worker"
        assert not ml.applied

        ml_custom = MemoryLimit(limit_mb=128, name="parser")
        assert ml_custom.limit_mb == 128
        assert ml_custom.name == "parser"

    def test_memory_limit_repr(self) -> None:
        """MemoryLimit should have a readable repr."""
        ml = MemoryLimit(limit_mb=64, name="test")
        r = repr(ml)
        assert "test" in r
        assert "64" in r
        assert "pending" in r

    def test_memory_limit_skip_on_non_linux(self) -> None:
        """MemoryLimit should return False on non-Linux platforms."""
        with patch("utils.sandbox._is_linux", False):
            ml = MemoryLimit(limit_mb=64, name="nonlinux")
            result = ml.apply()
            assert result is False
            assert not ml.applied

    def test_memory_limit_context_manager(self) -> None:
        """MemoryLimit should work as a context manager."""
        with patch("utils.sandbox._is_linux", True), \
             patch("utils.sandbox.resource") as mock_resource:
            mock_resource.RLIMIT_AS = 9
            mock_resource.getrlimit.return_value = (-1, -1)
            ml = MemoryLimit(limit_mb=128, name="ctx-test")
            with ml:
                assert ml.applied
            # After exiting, restore should have been called
            mock_resource.setrlimit.assert_any_call(9, (128 * 1024 * 1024, 128 * 1024 * 1024))
            mock_resource.setrlimit.assert_any_call(9, (-1, -1))

    def test_memory_limit_decorator_raises_memory_error(self) -> None:
        """MemoryLimit as decorator should catch MemoryError and re-raise."""
        with patch("utils.sandbox._is_linux", True), \
             patch("utils.sandbox.resource") as mock_resource:
            mock_resource.RLIMIT_AS = 9
            mock_resource.getrlimit.return_value = (-1, -1)

            ml = MemoryLimit(limit_mb=64, name="deco-test")

            @ml
            def bad_worker():
                raise MemoryError("test OOM")

            with pytest.raises(MemoryError):
                bad_worker()

    def test_memory_limit_is_idempotent(self) -> None:
        """Calling apply() twice should only apply once."""
        with patch("utils.sandbox._is_linux", True), \
             patch("utils.sandbox.resource") as mock_resource:
            mock_resource.RLIMIT_AS = 9
            mock_resource.getrlimit.return_value = (-1, -1)
            ml = MemoryLimit(limit_mb=256, name="idem")
            ml.apply()
            ml.apply()
            # setrlimit should be called only once
            mock_resource.setrlimit.assert_called_once()

    def test_memory_limit_default_value(self) -> None:
        """Default memory limit should be defined."""
        assert _DEFAULT_MEMORY_LIMIT_MB > 0
        assert isinstance(_DEFAULT_MEMORY_LIMIT_MB, int)


# ===========================================================================
# test_memory_limit_containment (verification test)
# ===========================================================================


class TestMemoryLimitContainment:
    """Verification test: workers exceeding memory bounds raise MemoryError."""

    def test_memory_limit_containment(self) -> None:
        """Workers exceeding memory bounds raise controlled MemoryError."""
        with patch("utils.sandbox._is_linux", True), \
             patch("utils.sandbox.resource") as mock_resource:
            mock_resource.RLIMIT_AS = 9
            mock_resource.getrlimit.return_value = (-1, -1)

            ml = MemoryLimit(limit_mb=32, name="containment-test")

            @ml
            def worker_exceeds_memory():
                """Simulate a worker that exceeds memory."""
                raise MemoryError("Worker exceeded memory limit")

            with pytest.raises(MemoryError, match="Worker exceeded memory limit"):
                worker_exceeds_memory()

            # After context manager exits, limits are restored
            assert not ml.applied

    def test_memory_limit_normal_worker_succeeds(self) -> None:
        """Workers within memory bounds complete normally."""
        with patch("utils.sandbox._is_linux", True), \
             patch("utils.sandbox.resource") as mock_resource:
            mock_resource.RLIMIT_AS = 9
            mock_resource.getrlimit.return_value = (-1, -1)

            ml = MemoryLimit(limit_mb=64, name="normal-worker")

            @ml
            def worker_normal():
                return "success"

            result = worker_normal()
            assert result == "success"

    def test_memory_limit_logs_isolation_event(self) -> None:
        """Memory limit breach should log an isolation event."""
        with patch("utils.sandbox._is_linux", True), \
             patch("utils.sandbox.resource") as mock_resource, \
             patch("utils.sandbox.logger") as mock_logger:
            mock_resource.RLIMIT_AS = 9
            mock_resource.getrlimit.return_value = (-1, -1)

            ml = MemoryLimit(limit_mb=16, name="log-test")

            @ml
            def worker_breach():
                raise MemoryError("OOM detected")

            with pytest.raises(MemoryError):
                worker_breach()

            # Verify error was logged
            mock_logger.error.assert_called()
            call_args = mock_logger.error.call_args
            log_msg = call_args[0][0]
            log_args = call_args[0][1:]
            assert "MemoryError" in log_msg
            assert any("log-test" in str(a) for a in log_args)
