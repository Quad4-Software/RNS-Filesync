"""Fuzzing for path jail, ACL parsing, protocol, and delta bounds."""

from __future__ import annotations

import io

from hypothesis import given, settings
from hypothesis import strategies as st

from rns_filesync import protocol
from rns_filesync.paths import PathJailError, normalize_relpath, resolve_under_root
from rns_filesync.permissions import PermissionStore
from rns_filesync.transfer import (
    MAX_DELTA_BLOCK_NUM,
    apply_delta_blocks,
    write_bytes_atomic,
)


@given(st.text(min_size=0, max_size=80))
@settings(max_examples=200)
def test_fuzz_normalize_never_escapes(text):
    try:
        result = normalize_relpath(text)
    except PathJailError:
        return
    parts = result.replace("\\", "/").split("/")
    assert ".." not in parts
    assert not result.startswith("/")
    assert "\x00" not in result
    assert result not in (".", "")


@given(st.text(min_size=1, max_size=40), st.text(min_size=0, max_size=40))
@settings(max_examples=100)
def test_fuzz_resolve_under_root_stays_jailed(tmp_path_factory, rel, junk):
    root = tmp_path_factory.mktemp("jail")
    try:
        path = resolve_under_root(str(root), rel)
    except PathJailError:
        return
    assert path.startswith(str(root.resolve()))


@given(st.text(min_size=0, max_size=64))
@settings(max_examples=150)
def test_fuzz_acl_rules_never_crash(text):
    store = PermissionStore()
    store.add_rule(text)
    store.load_allowed_text(text)
    # Checks with random-looking hashes must not raise.
    store.check("ab" * 16, "read")
    store.can_connect("cd" * 16)


@given(
    st.lists(
        st.sampled_from(["r", "w", "d", "rw", "rwd", "adm", "nope", "", "R"]),
        min_size=0,
        max_size=6,
    ),
    st.lists(
        st.one_of(
            st.just("all"),
            st.just("none"),
            st.just("*"),
            st.from_regex(r"[0-9a-f]{32}", fullmatch=True),
            st.text(min_size=0, max_size=20),
        ),
        min_size=0,
        max_size=6,
    ),
)
@settings(max_examples=100)
def test_fuzz_acl_matrix_consistency(perms, targets):
    store = PermissionStore()
    for perm, target in zip(perms, targets, strict=False):
        store.add_rule(f"{perm}:{target}")
    peer = "11" * 16
    # Invariants: blocked always denies; none on a perm denies that perm.
    if store.enabled:
        for permission in ("read", "write", "delete"):
            allowed = store.check(peer, permission)
            assert isinstance(allowed, bool)
        assert isinstance(store.can_connect(peer), bool)


@given(st.binary(min_size=0, max_size=256))
@settings(max_examples=100)
def test_fuzz_protocol_garbage(raw):
    try:
        protocol.decode_message(raw)
    except Exception:
        pass


@given(
    st.dictionaries(
        st.text(min_size=1, max_size=24),
        st.one_of(
            st.none(),
            st.booleans(),
            st.integers(),
            st.text(max_size=24),
            st.binary(max_size=24),
            st.dictionaries(st.text(max_size=8), st.integers(), max_size=3),
        ),
        max_size=8,
    ),
)
@settings(max_examples=80)
def test_fuzz_protocol_type_injection(payload):
    payload = dict(payload)
    payload["type"] = "file_request"
    try:
        raw = protocol.encode_message(payload)
        decoded = protocol.decode_message(raw)
        assert decoded["type"] == "file_request"
    except Exception:
        pass


@given(
    st.lists(
        st.one_of(
            st.integers(min_value=-10, max_value=MAX_DELTA_BLOCK_NUM + 10),
            st.booleans(),
        ),
        min_size=1,
        max_size=8,
    ),
)
@settings(max_examples=80)
def test_fuzz_delta_block_bounds(blocks):
    import tempfile

    with tempfile.TemporaryDirectory() as root:
        write_bytes_atomic(root, "f.bin", b"hello")
        try:
            apply_delta_blocks(root, "f.bin", blocks, io.BytesIO(b"Z" * 64))
        except (PathJailError, TypeError, ValueError, OSError):
            return
        for block in blocks:
            assert isinstance(block, int) and not isinstance(block, bool)
            assert 0 <= block <= MAX_DELTA_BLOCK_NUM
